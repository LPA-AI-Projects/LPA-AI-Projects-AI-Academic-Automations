import type { AppState, PriceRow, SlidesProfile, Step } from "./model";

export interface Components {
  uncached: number;
  write: number;
  read: number;
  output: number;
  search: number;
}
export const zero = (): Components => ({ uncached: 0, write: 0, read: 0, output: 0, search: 0 });
export const total = (c: Components) => c.uncached + c.write + c.read + c.output + c.search;
export const tokenCost = (c: Components) => c.uncached + c.write + c.read + c.output;
export function addTo(a: Components, b: Components, k = 1) {
  a.uncached += b.uncached * k;
  a.write += b.write * k;
  a.read += b.read * k;
  a.output += b.output * k;
  a.search += b.search * k;
  return a;
}
export const scale = (b: Components, k: number) => addTo(zero(), b, k);

export const isPriced = (p?: PriceRow | null): p is PriceRow =>
  !!p && p.input != null && p.output != null && Number.isFinite(p.input) && Number.isFinite(p.output);

export interface CalcOpts {
  caching: boolean;
  hitRate: number;
  window: "5m" | "1h";
  outputMult?: number | undefined;
  searchesOverride?: number | null | undefined;
}

export const writeMult = (p: PriceRow, w: "5m" | "1h") => (w === "1h" ? p.writeMult1h : p.writeMult5m);

/** h* = (writeMult - 1) / (writeMult - readMult) */
export function breakEvenHitRate(p: PriceRow, w: "5m" | "1h") {
  const wm = writeMult(p, w);
  const d = wm - p.readMult;
  if (d <= 0) return 0;
  return Math.max(0, Math.min(1, (wm - 1) / d));
}

export function cacheEligible(step: Pick<Step, "system">, p: PriceRow, caching: boolean) {
  return caching && step.system > 0 && step.system >= p.minCache;
}

/** Cost of `step.calls` calls of a step (one attempt). */
export function stepCost(step: Step, p: PriceRow, o: CalcOpts): Components {
  const ip = (p.input ?? 0) / 1e6;
  const op = (p.output ?? 0) / 1e6;
  const C = step.system;
  const U = step.input + step.searchInput;
  const O = step.output * (o.outputMult ?? 1);
  const S = o.searchesOverride != null && step.searches > 0 ? o.searchesOverride : step.searches;
  const c = zero();
  if (cacheEligible(step, p, o.caching)) {
    const h = o.hitRate;
    c.uncached = U * ip;
    c.write = C * ip * (1 - h) * writeMult(p, o.window);
    c.read = C * ip * h * p.readMult;
  } else {
    c.uncached = (C + U) * ip;
  }
  c.output = O * op;
  c.search = (S * (p.searchPer1k ?? 0)) / 1000;
  return scale(c, step.calls);
}

export function retryStats(f: number, maxAttempts: number) {
  const m = Math.max(1, Math.round(maxAttempts));
  let exp = 0;
  for (let k = 0; k < m; k++) exp += f ** k;
  return { expCalls: exp, fallbackProb: f ** m };
}

export function stepMultiplier(step: Step, rs: { expCalls: number; fallbackProb: number }) {
  if (step.kind === "json" || step.kind === "refine") return rs.expCalls;
  if (step.kind === "fallback") return rs.fallbackProb;
  return 1;
}

export interface StepResult {
  step: Step;
  perCall: Components;
  mult: number;
  expected: Components;
  eligible: boolean;
}

export function jobCost(steps: Step[], job: Step["job"], p: PriceRow, o: CalcOpts, rs: ReturnType<typeof retryStats>) {
  const rows: StepResult[] = steps
    .filter((s) => s.job === job && s.include)
    .map((s) => {
      const perCall = stepCost(s, p, o);
      const mult = stepMultiplier(s, rs);
      return { step: s, perCall, mult, expected: scale(perCall, mult), eligible: cacheEligible(s, p, o.caching) };
    });
  const t = zero();
  rows.forEach((r) => addTo(t, r.expected));
  return { rows, total: t };
}

/** Slides get no caching benefit; everything bills as uncached input. */
export function slidesCost(s: SlidesProfile, p: PriceRow, outputMult = 1): Components {
  const c = zero();
  if (!s.include) return c;
  const ip = (p.input ?? 0) / 1e6;
  const op = (p.output ?? 0) / 1e6;
  const inTok = s.plannerInput + s.slideCalls * (s.slideSystem + s.slideInput) + s.validatorInput;
  const outTok = s.plannerOutput + s.slideCalls * s.slideOutput + s.validatorOutput;
  const k = s.modules * s.retryFactor;
  c.uncached = inTok * ip * k;
  c.output = outTok * op * outputMult * k;
  return c;
}

export interface Overrides {
  caching?: boolean;
  outputMult?: number;
  searchesOverride?: number | null;
  steps?: Step[];
  failRate?: number;
}

export function effectiveFailRate(state: AppState) {
  return state.scenario.structured ? state.scenario.structuredFailRate : state.scenario.failRate;
}

export function compute(state: AppState, p: PriceRow, ov: Overrides = {}) {
  const sc = state.scenario;
  const steps = ov.steps ?? state.steps;
  const f = ov.failRate ?? effectiveFailRate(state);
  const rs = retryStats(f, sc.maxAttempts);
  const o: CalcOpts = {
    caching: ov.caching ?? sc.caching,
    hitRate: sc.hitRate,
    window: sc.window,
    outputMult: ov.outputMult,
    searchesOverride: ov.searchesOverride,
  };
  const outline = jobCost(steps, "outline", p, o, rs);
  const refine = jobCost(steps, "refine", p, o, rs);
  const assessment = jobCost(steps, "assessment", p, o, rs);
  const slides = slidesCost(state.slides, p, ov.outputMult ?? 1);

  const perOutline = total(outline.total);
  const perRefine = total(refine.total);
  const perAssessment = total(assessment.total);
  const perSlides = total(slides);

  const monthly = zero();
  addTo(monthly, outline.total, sc.outlines);
  addTo(monthly, refine.total, sc.refines);
  addTo(monthly, assessment.total, sc.assessments);
  addTo(monthly, slides, sc.slidesJobs);
  const monthlyApi = total(monthly);
  const fixedTotal = state.fixed.reduce((a, x) => a + (Number(x.amount) || 0), 0);

  // retry / fallback waste per outline
  const jsonRow = outline.rows.find((r) => r.step.kind === "json");
  const fbRow = outline.rows.find((r) => r.step.kind === "fallback");
  const retryWaste = jsonRow ? total(jsonRow.perCall) * (rs.expCalls - 1) : 0;
  const fallbackWaste = fbRow ? total(fbRow.perCall) * rs.fallbackProb : 0;

  return {
    f,
    rs,
    opts: o,
    outline,
    refine,
    assessment,
    slides,
    perOutline,
    perRefine,
    perAssessment,
    perSlides,
    monthly,
    monthlyApi,
    monthlySearch: monthly.search,
    fixedTotal,
    allIn: monthlyApi + fixedTotal,
    wastePerOutline: retryWaste + fallbackWaste,
    retryWaste,
    fallbackWaste,
  };
}
export type Result = ReturnType<typeof compute>;

export interface LedgerLine {
  group: string;
  name: string;
  detail: string;
  amount: number;
}

export function ledger(state: AppState, r: Result): LedgerLine[] {
  const sc = state.scenario;
  const lines: LedgerLine[] = [];
  const jobs: [typeof r.outline, number, string][] = [
    [r.outline, sc.outlines, "outline"],
    [r.refine, sc.refines, "refine"],
    [r.assessment, sc.assessments, "assessment"],
  ];
  for (const [job, vol, unit] of jobs) {
    for (const row of job.rows) {
      const tok = tokenCost(row.perCall);
      if (row.step.kind === "fallback") {
        lines.push({ group: "Retries / fallback", name: row.step.name, detail: `${vol} ${unit}s × p=${(row.mult * 100).toFixed(3)}%`, amount: tok * row.mult * vol });
      } else {
        lines.push({ group: "Model calls", name: row.step.name, detail: `${vol} ${unit}s × ${row.step.calls} call(s)`, amount: tok * vol });
        if (row.mult > 1)
          lines.push({ group: "Retries / fallback", name: `${row.step.name} — JSON retries`, detail: `${vol} × ${(row.mult - 1).toFixed(3)} extra calls`, amount: tok * (row.mult - 1) * vol });
      }
    }
  }
  lines.push({ group: "Web search", name: "Web search", detail: "all searching steps", amount: r.monthlySearch });
  lines.push({ group: "Slides", name: "Slides jobs", detail: `${sc.slidesJobs} jobs × ${state.slides.modules} modules`, amount: r.perSlides * sc.slidesJobs });
  for (const fx of state.fixed) lines.push({ group: "Fixed", name: fx.name, detail: "monthly", amount: Number(fx.amount) || 0 });
  return lines;
}

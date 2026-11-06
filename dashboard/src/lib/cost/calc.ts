import type { LogRecord } from "./logs";
import type { AppState, PriceRow, Step } from "./model";

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
  return caching && step.system > 0 && step.system * (p.tokenFactor ?? 1) >= p.minCache;
}

/** Cost of `step.calls` calls of a step (one attempt). */
export function stepCost(step: Step, p: PriceRow, o: CalcOpts): Components {
  const ip = (p.input ?? 0) / 1e6;
  const op = (p.output ?? 0) / 1e6;
  const tf = p.tokenFactor ?? 1;
  const C = step.system * tf;
  const U = (step.input + step.searchInput) * tf;
  const O = step.output * (o.outputMult ?? 1) * tf;
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

  const perOutline = total(outline.total);
  const perRefine = total(refine.total);

  const monthly = zero();
  addTo(monthly, outline.total, sc.outlines);
  addTo(monthly, refine.total, sc.refines);
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
    perOutline,
    perRefine,
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
  for (const fx of state.fixed) lines.push({ group: "Fixed", name: fx.name, detail: "monthly", amount: Number(fx.amount) || 0 });
  return lines;
}

// ───────────────────────── Monthly analysis ─────────────────────────

export interface ProjectionRow {
  month: string;
  outlines: number;
  refines: number;
  anthropic: number;
  fixed: number;
  total: number;
  cumulative: number;
}

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/** `months` months starting at `start`, with volumes growing `growthPct`% per month. */
export function projectMonths(
  state: AppState,
  r: Pick<Result, "perOutline" | "perRefine" | "fixedTotal">,
  months = 12,
  start: Date = new Date(),
): ProjectionRow[] {
  const g = 1 + (state.scenario.monthlyGrowth || 0) / 100;
  const rows: ProjectionRow[] = [];
  let cumulative = 0;
  for (let k = 0; k < months; k++) {
    const d = new Date(start.getFullYear(), start.getMonth() + k, 1);
    const outlines = state.scenario.outlines * g ** k;
    const refines = state.scenario.refines * g ** k;
    const anthropic = outlines * r.perOutline + refines * r.perRefine;
    const total = anthropic + r.fixedTotal;
    cumulative += total;
    rows.push({ month: `${MONTHS[d.getMonth()]} ${d.getFullYear()}`, outlines, refines, anthropic, fixed: r.fixedTotal, total, cumulative });
  }
  return rows;
}

/** Cost of one logged call from its real token counts (no token factor: the log already holds real tokens). */
export function recordCost(
  r: Pick<LogRecord, "input" | "output" | "cacheWrite" | "cacheRead" | "searches">,
  p: PriceRow,
  window: "5m" | "1h",
) {
  const ip = (p.input ?? 0) / 1e6;
  const op = (p.output ?? 0) / 1e6;
  const search = (r.searches * (p.searchPer1k ?? 0)) / 1000;
  const actual = r.input * ip + r.cacheWrite * ip * writeMult(p, window) + r.cacheRead * ip * p.readMult + r.output * op + search;
  const noCache = (r.input + r.cacheWrite + r.cacheRead) * ip + r.output * op + search;
  return { actual, noCache };
}

export interface ActualMonth {
  month: string; // YYYY-MM
  outlines: number; // one learning_objectives call per outline job
  refines: number; // refine_outline calls (retries counted)
  calls: number;
  input: number;
  output: number;
  cacheWrite: number;
  cacheRead: number;
  searches: number;
  cost: number;
  noCache: number;
}

/** Real monthly spend from imported logs. Each call is priced with its own model's price row when known. */
export function monthlyActual(
  records: LogRecord[],
  prices: PriceRow[],
  fallback: PriceRow,
  window: "5m" | "1h",
): { rows: ActualMonth[]; undated: number } {
  const map = new Map<string, ActualMonth>();
  let undated = 0;
  for (const r of records) {
    if (!r.date) {
      undated++;
      continue;
    }
    const month = r.date.slice(0, 7);
    const a = map.get(month) ?? { month, outlines: 0, refines: 0, calls: 0, input: 0, output: 0, cacheWrite: 0, cacheRead: 0, searches: 0, cost: 0, noCache: 0 };
    const model = r.model.toLowerCase();
    const p = prices.find((x) => isPriced(x) && model.includes(x.id)) ?? fallback;
    const c = recordCost(r, p, window);
    a.calls++;
    if (r.label === "learning_objectives") a.outlines++;
    if (r.label === "refine_outline") a.refines++;
    a.input += r.input;
    a.output += r.output;
    a.cacheWrite += r.cacheWrite;
    a.cacheRead += r.cacheRead;
    a.searches += r.searches;
    a.cost += c.actual;
    a.noCache += c.noCache;
    map.set(month, a);
  }
  return { rows: [...map.values()].sort((x, y) => x.month.localeCompare(y.month)), undated };
}

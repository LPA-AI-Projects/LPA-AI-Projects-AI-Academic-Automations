import { describe, expect, it } from "vitest";
import { breakEvenHitRate, compute, monthlyActual, projectMonths, recordCost, retryStats, stepCost } from "./calc";
import { PRODUCTION_MODEL_ID, defaultState } from "./model";
import { parseLogs } from "./logs";
import { STORAGE_KEY, loadState, mergePrices, mergeState, parseOutlineLogs } from "./store";
import type { RequestItem, Summary } from "./api";
import { monthlyCosts, priceForModel, requestCost, sumMonths, usageCost } from "./dbcost";

const s = defaultState();
const sonnet = s.prices[0]!;
const json = s.steps.find((x) => x.id === "js")!;

describe("cost rules", () => {
  it("caching off bills (C+U) at input price", () => {
    const c = stepCost(json, sonnet, { caching: false, hitRate: 0.3, window: "5m" });
    expect(c.uncached).toBeCloseTo((4700 + 4300) * 3e-6, 10);
    expect(c.output).toBeCloseTo(6000 * 15e-6, 10);
  });
  it("caching on applies h*read + (1-h)*write to C", () => {
    const c = stepCost(json, sonnet, { caching: true, hitRate: 0.3, window: "5m" });
    expect(c.write + c.read).toBeCloseTo(4700 * 3e-6 * (0.3 * 0.1 + 0.7 * 1.25), 10);
  });
  it("no write fee below min cacheable tokens", () => {
    const haiku = s.prices[1]!; // min 4096
    const lo = s.steps[0]!; // C=1200
    const c = stepCost(lo, haiku, { caching: true, hitRate: 0.3, window: "5m" });
    expect(c.write).toBe(0);
    expect(c.uncached).toBeCloseTo(2500 * 1e-6, 10);
  });
  it("expected calls 1+f+f^2 and fallback f^3", () => {
    const r = retryStats(0.1, 3);
    expect(r.expCalls).toBeCloseTo(1.11, 10);
    expect(r.fallbackProb).toBeCloseTo(0.001, 10);
  });
  it("break-even hit rate is (w-1)/(w-r)", () => {
    expect(breakEvenHitRate(sonnet, "5m")).toBeCloseTo(0.25 / 1.15, 10);
    expect(breakEvenHitRate(sonnet, "1h")).toBeCloseTo(1 / 1.9, 10);
  });
  it("search cost $10 per 1000", () => {
    const r = compute(s, sonnet);
    expect(r.outline.rows.find((x) => x.step.id === "rs")!.expected.search).toBeCloseTo(0.03, 10);
  });
});

describe("verified prices and tokenizer factor", () => {
  const byId = (id: string) => s.prices.find((x) => x.id === id)!;
  it("Sonnet 5.5 uses $2/$10, 0.05x cache reads and 512-token minimum", () => {
    const p = byId("sonnet-5-5");
    expect([p.input, p.output, p.readMult, p.minCache]).toEqual([2, 10, 0.05, 512]);
  });
  it("token factor scales tokens, so the 33% cheaper list price is only ~13% cheaper", () => {
    const a = compute(s, byId("sonnet-4-6")).perOutline;
    const b = compute(s, byId("sonnet-5-5")).perOutline;
    expect(b / a).toBeGreaterThan(0.8);
    expect(b / a).toBeLessThan(0.95);
  });
  it("token factor 1 reproduces the list-price arithmetic", () => {
    const p = { ...byId("sonnet-4-6"), tokenFactor: 1 };
    expect(stepCost(json, p, { caching: false, hitRate: 0.3, window: "5m" }).output).toBeCloseTo(6000 * 15e-6, 10);
  });
});

describe("log parser", () => {
  it("ignores prefix and treats None as 0", () => {
    const { records } = parseLogs(
      "2026-11-01 10:00:00 INFO app: Anthropic usage | label=outline_json model=claude-sonnet-4-6 input=300 output=5200 cache_write=4900 cache_read=None web_searches=0 stop=end_turn structured=False",
    );
    expect(records[0]).toMatchObject({ label: "outline_json", input: 300, output: 5200, cacheWrite: 4900, cacheRead: 0, date: "2026-11-01" });
  });
});

describe("outline + refine only, locked model", () => {
  it("has no assessment, slides or Gamma anywhere in the defaults", () => {
    const d = defaultState();
    expect(d.steps.every((x) => x.job === "outline" || x.job === "refine")).toBe(true);
    expect(JSON.stringify(d)).not.toMatch(/assessment|slidesJobs|gamma/i);
  });
  it("structured output defaults to ON like the backend", () => {
    expect(defaultState().scenario.structured).toBe(true);
  });
  it("saved settings cannot change the production model or bring back removed fields", () => {
    const saved = { ...defaultState(), scenario: { ...defaultState().scenario, currentModelId: "opus-5-5", slidesJobs: 9 }, slides: { include: true } };
    const m = mergeState(saved) as unknown as Record<string, unknown>;
    const sc = m["scenario"] as Record<string, unknown>;
    expect(Object.keys(sc)).not.toContain("currentModelId");
    expect(Object.keys(sc)).not.toContain("slidesJobs");
    expect(Object.keys(m)).not.toContain("slides");
    expect(PRODUCTION_MODEL_ID).toBe("sonnet-4-6");
  });
  it("drops assessment steps from old saved profiles", () => {
    const old = { ...defaultState(), steps: [...defaultState().steps, { ...defaultState().steps[0]!, id: "as", job: "assessment" }] };
    expect(mergeState(old).steps.some((x) => x.id === "as")).toBe(false);
  });
});

describe("monthly analysis", () => {
  const st = defaultState();
  const sonnet = st.prices.find((x) => x.id === "sonnet-4-6")!;
  const r = compute(st, sonnet);

  it("projects 12 months and the total is the sum of the months", () => {
    const rows = projectMonths(st, r, 12, new Date(2026, 10, 15));
    expect(rows).toHaveLength(12);
    expect(rows[0]!.month).toBe("Nov 2026");
    expect(rows[11]!.month).toBe("Oct 2027");
    const sum = rows.reduce((a, x) => a + x.total, 0);
    expect(rows[11]!.cumulative).toBeCloseTo(sum, 8);
    expect(rows[0]!.total).toBeCloseTo(r.allIn, 8);
  });
  it("applies compound monthly growth to volumes", () => {
    const g = { ...st, scenario: { ...st.scenario, monthlyGrowth: 10 } };
    const rows = projectMonths(g, r, 3, new Date(2026, 0, 1));
    expect(rows[1]!.outlines).toBeCloseTo(st.scenario.outlines * 1.1, 8);
    expect(rows[2]!.outlines).toBeCloseTo(st.scenario.outlines * 1.21, 8);
  });
  it("prices a logged call from real tokens, with and without caching", () => {
    const c = recordCost({ input: 300, output: 5200, cacheWrite: 0, cacheRead: 4900, searches: 0 }, sonnet, "5m");
    expect(c.actual).toBeCloseTo(300 * 3e-6 + 4900 * 3e-6 * 0.1 + 5200 * 15e-6, 10);
    expect(c.noCache).toBeCloseTo((300 + 4900) * 3e-6 + 5200 * 15e-6, 10);
    expect(c.noCache).toBeGreaterThan(c.actual);
  });
  it("groups imported logs by month and ignores other features", () => {
    const text = [
      "2026-10-31 10:00:00 Anthropic usage | label=learning_objectives model=claude-sonnet-4-6 input=1000 output=900 cache_write=1200 cache_read=0 web_searches=0 stop=end_turn structured=True",
      "2026-11-02 10:00:00 Anthropic usage | label=learning_objectives model=claude-sonnet-4-6 input=1000 output=900 cache_write=0 cache_read=1200 web_searches=0 stop=end_turn structured=True",
      "2026-11-02 10:05:00 Anthropic usage | label=refine_outline model=claude-sonnet-4-6 input=7000 output=6000 cache_write=0 cache_read=0 web_searches=0 stop=end_turn structured=True",
      "2026-11-02 10:06:00 Anthropic usage | label=completion model=claude-sonnet-4-6 input=900 output=200 cache_write=0 cache_read=0 web_searches=None stop=end_turn structured=False",
      "Anthropic usage | label=research model=claude-sonnet-4-6 input=100 output=100 cache_write=0 cache_read=0 web_searches=1 stop=end_turn structured=False",
    ].join("\n");
    const logs = parseOutlineLogs(text);
    expect(logs.ignored).toBe(1);
    const { rows, undated } = monthlyActual(logs.records, st.prices, sonnet, "5m");
    expect(undated).toBe(1);
    expect(rows.map((x) => [x.month, x.outlines, x.refines, x.calls])).toEqual([
      ["2026-10", 1, 0, 1],
      ["2026-11", 1, 1, 2],
    ]);
  });
});

describe("saved settings survive a reload", () => {
  it("loadState returns what was saved (logs included), not the defaults", () => {
    const store = new Map<string, string>();
    (globalThis as unknown as { localStorage: unknown }).localStorage = {
      getItem: (k: string) => store.get(k) ?? null,
      setItem: (k: string, v: string) => void store.set(k, v),
    };
    const saved = { ...defaultState(), logText: "2026-11-02 Anthropic usage | label=research model=claude-sonnet-4-6 input=1 output=1 cache_write=0 cache_read=0 web_searches=0 stop=end_turn structured=True", scenario: { ...defaultState().scenario, outlines: 321 } };
    store.set(STORAGE_KEY, JSON.stringify(saved));
    const loaded = loadState();
    expect(loaded.logText).toContain("label=research");
    expect(loaded.scenario.outlines).toBe(321);
    delete (globalThis as unknown as { localStorage?: unknown }).localStorage;
    expect(loadState().scenario.outlines).toBe(defaultState().scenario.outlines);
  });
});

describe("database costs", () => {
  const st = defaultState();
  const sonnet = st.prices.find((x) => x.id === "sonnet-4-6")!;
  const est = compute(st, sonnet);
  const counts = (o: number, c: number, f: number, r: number) => ({ outlines: o, completed: c, failed: f, other: o - c - f, refines: r });
  const summary = (usage: Summary["usage"] = []): Summary => ({
    generated_at: "",
    months: ["2026-10", "2026-11"],
    by_month: [
      { month: "2026-10", zoho: counts(10, 8, 2, 3), bitrix: counts(5, 5, 0, 2) },
      { month: "2026-11", zoho: counts(4, 4, 0, 0), bitrix: counts(2, 2, 0, 1) },
    ],
    usage,
    usage_recording: { available: usage.length > 0, first_recorded: null },
  });

  it("with no recorded usage every completed outline and refinement is estimated", () => {
    const rows = monthlyCosts(summary(), st.prices, sonnet, "5m", est.perOutline, est.perRefine);
    const oct = rows[0]!;
    expect(oct.completed).toBe(13);
    expect(oct.failed).toBe(2);
    expect(oct.refines).toBe(5);
    expect(oct.recorded).toBe(0);
    expect(oct.estimated).toBeCloseTo(13 * est.perOutline + 5 * est.perRefine, 10);
    expect(sumMonths(rows).outlines).toBe(21);
  });

  it("recorded usage is priced exactly and removes those requests from the estimate", () => {
    const u = { month: "2026-11", kind: "outline", label: "learning_objectives", model: "claude-sonnet-4-6", calls: 4, input: 1000, output: 900, cache_write: 1200, cache_read: 0, searches: 0, jobs: 4 };
    const r = { ...u, kind: "refine", label: "refine_outline", calls: 1, jobs: 0, input: 7000, output: 6000, cache_write: 0, cache_read: 0 };
    const nov = monthlyCosts(summary([u, r]), st.prices, sonnet, "5m", est.perOutline, est.perRefine)[1]!;
    expect(nov.recorded).toBeCloseTo(usageCost(u, sonnet, "5m") + usageCost(r, sonnet, "5m"), 10);
    expect(nov.recordedOutlines).toBe(4);
    expect(nov.estimated).toBeCloseTo(Math.max(0, 6 - 4) * est.perOutline + Math.max(0, 1 - 1) * est.perRefine, 10);
  });

  it("matches a logged model name to its price row, falling back to production", () => {
    expect(priceForModel("claude-sonnet-5-5", st.prices, sonnet).id).toBe("sonnet-5-5");
    expect(priceForModel("gpt-5.4", st.prices, sonnet).id).toBe("gpt-5-4");
    expect(priceForModel("mystery-model", st.prices, sonnet).id).toBe("sonnet-4-6");
  });

  it("prices a request from each model's recorded tokens", () => {
    const base: RequestItem = { id: "1", created_at: null, source: "zoho", record_id: "r", course_name: "", company_name: "", status: "completed", failed: false, versions: 1, refinements: 0, pdf_url: null, usage: { outline: { calls: 1, input: 1_000_000, output: 0, cache_write: 0, cache_read: 0, searches: 0 }, refine: null, outline_parts: [{ model: "claude-sonnet-4-6", calls: 1, input: 1_000_000, output: 0, cache_write: 0, cache_read: 0, searches: 0 }] } };
    const c = requestCost(base, sonnet, "5m", est.perOutline, est.perRefine, st.prices);
    expect(c.exact).toBe(true);
    expect(c.outline).toBeCloseTo(3, 10);
  });

  it("keeps official prices when a saved sheet has stale rates", () => {
    const merged = mergePrices([{ id: "sonnet-4-6", model: "old", provider: "Anthropic", input: 99, output: 99, writeMult5m: 1, writeMult1h: 1, readMult: 1, minCache: 1, searchPer1k: 0, tokenFactor: 1, notes: "", lastVerified: "", qualityNotes: "" }], st.prices);
    expect(merged.find((p) => p.id === "sonnet-4-6")!.input).toBe(3);
    expect(merged.find((p) => p.id === "sonnet-4-6")!.output).toBe(15);
  });

  it("prices one request from recorded tokens, or estimates it", () => {
    const base: RequestItem = { id: "1", created_at: null, source: "zoho", record_id: "r", course_name: "", company_name: "", status: "completed", failed: false, versions: 3, refinements: 2, pdf_url: null, usage: { outline: null, refine: null } };
    const estimated = requestCost(base, sonnet, "5m", est.perOutline, est.perRefine);
    expect(estimated.exact).toBe(false);
    expect(estimated.total).toBeCloseTo(est.perOutline + 2 * est.perRefine, 10);
    const tokens = { calls: 4, input: 1000, output: 900, cache_write: 0, cache_read: 0, searches: 0 };
    const mixed = requestCost({ ...base, usage: { outline: tokens, refine: null } }, sonnet, "5m", est.perOutline, est.perRefine);
    expect(mixed.outlineRecorded).toBe(true);
    expect(mixed.outline).toBeCloseTo(usageCost(tokens, sonnet, "5m"), 10);
    expect(mixed.exact).toBe(false); // refinements still estimated
    const failed = requestCost({ ...base, status: "failed", refinements: 0 }, sonnet, "5m", est.perOutline, est.perRefine);
    expect(failed.total).toBe(0);
    expect(failed.exact).toBe(true);
  });
});

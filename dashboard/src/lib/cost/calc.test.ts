import { describe, expect, it } from "vitest";
import { breakEvenHitRate, compute, monthlyActual, projectMonths, recordCost, retryStats, stepCost } from "./calc";
import { PRODUCTION_MODEL_ID, defaultState } from "./model";
import { parseLogs } from "./logs";
import { STORAGE_KEY, loadState, mergeState, parseOutlineLogs } from "./store";

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

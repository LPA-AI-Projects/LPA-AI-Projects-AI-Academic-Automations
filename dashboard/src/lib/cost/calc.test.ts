import { describe, expect, it } from "vitest";
import { breakEvenHitRate, compute, retryStats, stepCost } from "./calc";
import { defaultState } from "./model";
import { parseLogs } from "./logs";

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

describe("log parser", () => {
  it("ignores prefix and treats None as 0", () => {
    const { records } = parseLogs(
      "2026-11-01 10:00:00 INFO app: Anthropic usage | label=outline_json model=claude-sonnet-4-6 input=300 output=5200 cache_write=4900 cache_read=None web_searches=0 stop=end_turn structured=False",
    );
    expect(records[0]).toMatchObject({ label: "outline_json", input: 300, output: 5200, cacheWrite: 4900, cacheRead: 0, date: "2026-11-01" });
  });
});

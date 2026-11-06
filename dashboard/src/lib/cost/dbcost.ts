import type { ByMonth, RequestItem, Summary, UsageAgg } from "./api";
import { writeMult } from "./calc";
import type { PriceRow } from "./model";

type Tokens = { input: number; output: number; cache_write: number; cache_read: number; searches: number };

/** Price row for a logged model name such as "claude-sonnet-4-6"; the production row when unknown. */
export function priceForModel(model: string, prices: PriceRow[], fallback: PriceRow): PriceRow {
  const m = model.toLowerCase();
  return prices.find((p) => p.input != null && p.output != null && m.includes(p.id)) ?? fallback;
}

/** Cost of recorded token usage (real tokens, so no token factor). */
export function usageCost(u: Tokens, p: PriceRow, window: "5m" | "1h"): number {
  const ip = (p.input ?? 0) / 1e6;
  const op = (p.output ?? 0) / 1e6;
  return (
    u.input * ip +
    u.cache_write * ip * writeMult(p, window) +
    u.cache_read * ip * p.readMult +
    u.output * op +
    (u.searches * (p.searchPer1k ?? 0)) / 1000
  );
}

export interface MonthCost {
  month: string;
  outlines: number; // all outline requests, any status
  completed: number;
  failed: number;
  other: number;
  refines: number;
  zoho: { outlines: number; refines: number };
  bitrix: { outlines: number; refines: number };
  recordedOutlines: number;
  recordedRefines: number;
  recorded: number; // $ priced from recorded token usage
  estimated: number; // $ for requests without recorded usage, at the estimated unit cost
  total: number;
}

const add = (a: ByMonth, k: "outlines" | "completed" | "failed" | "other" | "refines") => a.zoho[k] + a.bitrix[k];

/** Month rows combining database counts with recorded usage; unrecorded work is estimated. */
export function monthlyCosts(
  summary: Summary,
  prices: PriceRow[],
  fallback: PriceRow,
  window: "5m" | "1h",
  perOutline: number,
  perRefine: number,
): MonthCost[] {
  const usageByMonth = new Map<string, UsageAgg[]>();
  for (const u of summary.usage) usageByMonth.set(u.month, [...(usageByMonth.get(u.month) ?? []), u]);
  return summary.by_month.map((m) => {
    const rows = usageByMonth.get(m.month) ?? [];
    const recorded = rows.reduce((a, u) => a + usageCost(u, priceForModel(u.model, prices, fallback), window), 0);
    const recordedOutlines = rows.filter((u) => u.kind === "outline" && u.label === "learning_objectives").reduce((a, u) => a + u.jobs, 0);
    const recordedRefines = rows.filter((u) => u.kind === "refine" && u.label === "refine_outline").reduce((a, u) => a + u.calls, 0);
    const completed = add(m, "completed");
    const refines = add(m, "refines");
    const estimated = Math.max(0, completed - recordedOutlines) * perOutline + Math.max(0, refines - recordedRefines) * perRefine;
    return {
      month: m.month,
      outlines: add(m, "outlines"),
      completed,
      failed: add(m, "failed"),
      other: add(m, "other"),
      refines,
      zoho: { outlines: m.zoho.outlines, refines: m.zoho.refines },
      bitrix: { outlines: m.bitrix.outlines, refines: m.bitrix.refines },
      recordedOutlines,
      recordedRefines,
      recorded,
      estimated,
      total: recorded + estimated,
    };
  });
}

export function sumMonths(rows: MonthCost[]): MonthCost {
  const z = (): MonthCost => ({
    month: "Total",
    outlines: 0,
    completed: 0,
    failed: 0,
    other: 0,
    refines: 0,
    zoho: { outlines: 0, refines: 0 },
    bitrix: { outlines: 0, refines: 0 },
    recordedOutlines: 0,
    recordedRefines: 0,
    recorded: 0,
    estimated: 0,
    total: 0,
  });
  return rows.reduce((a, r) => {
    a.outlines += r.outlines;
    a.completed += r.completed;
    a.failed += r.failed;
    a.other += r.other;
    a.refines += r.refines;
    a.zoho.outlines += r.zoho.outlines;
    a.zoho.refines += r.zoho.refines;
    a.bitrix.outlines += r.bitrix.outlines;
    a.bitrix.refines += r.bitrix.refines;
    a.recordedOutlines += r.recordedOutlines;
    a.recordedRefines += r.recordedRefines;
    a.recorded += r.recorded;
    a.estimated += r.estimated;
    a.total += r.total;
    return a;
  }, z());
}

export interface RequestCost {
  total: number;
  outline: number;
  refine: number;
  outlineRecorded: boolean;
  refineRecorded: boolean;
  /** true when every counted part comes from recorded usage */
  exact: boolean;
}

/** Cost of one request: recorded tokens when available, otherwise the estimated unit cost. */
export function requestCost(item: RequestItem, price: PriceRow, window: "5m" | "1h", perOutline: number, perRefine: number): RequestCost {
  const o = item.usage.outline;
  const r = item.usage.refine;
  const outlineRecorded = !!o && o.calls > 0;
  const refineRecorded = !!r && r.calls > 0;
  const outline = outlineRecorded ? usageCost(o, price, window) : item.status === "completed" ? perOutline : 0;
  const refine = refineRecorded ? usageCost(r, price, window) : item.refinements * perRefine;
  const outlineExact = outlineRecorded || item.status !== "completed";
  const refineExact = refineRecorded || item.refinements === 0;
  return { total: outline + refine, outline, refine, outlineRecorded, refineRecorded, exact: outlineExact && refineExact };
}

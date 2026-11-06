import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from "react";
import { PRODUCTION_MODEL_ID, defaultState, type AppState, type PriceRow } from "./model";
import { isPriced } from "./calc";
import { OUTLINE_REFINE_LABELS, aggregate, parseLogs, type LogRecord } from "./logs";

export const STORAGE_KEY = "outline-ai-cost-console:v3";
const KEY = STORAGE_KEY; // v3: outline + refine only, locked production model

export interface ParsedLogs {
  records: LogRecord[];
  skipped: number;
  /** Usage lines from other features (slides, assessments) that are not part of this dashboard. */
  ignored: number;
}

interface Ctx {
  state: AppState;
  setState: (fn: (s: AppState) => AppState) => void;
  reset: () => void;
  replace: (s: AppState) => void;
  priced: PriceRow[];
  /** The production model. Fixed: it cannot be changed from the UI or from saved settings. */
  current: PriceRow | undefined;
  logs: ParsedLogs;
  byLabel: ReturnType<typeof aggregate>;
}

const StoreCtx = createContext<Ctx | null>(null);

/** Keep only keys that exist in the defaults, so stale saved fields (old model choice, slides, ...) are dropped. */
function pickKnown<T extends object>(defaults: T, raw: unknown): T {
  if (!raw || typeof raw !== "object") return defaults;
  const out = { ...defaults } as Record<string, unknown>;
  for (const k of Object.keys(defaults)) if (k in (raw as object)) out[k] = (raw as Record<string, unknown>)[k];
  return out as T;
}

export function mergeState(raw: unknown): AppState {
  const d = defaultState();
  if (!raw || typeof raw !== "object") return d;
  const r = raw as Partial<AppState>;
  return {
    ...d,
    version: 1,
    scenario: pickKnown(d.scenario, r.scenario),
    compare: pickKnown(d.compare, r.compare),
    prices: Array.isArray(r.prices) ? r.prices : d.prices,
    steps: Array.isArray(r.steps) ? r.steps.filter((s) => s.job === "outline" || s.job === "refine") : d.steps,
    fixed: Array.isArray(r.fixed) ? r.fixed : d.fixed,
    logText: typeof r.logText === "string" ? r.logText : "",
  };
}

export function parseOutlineLogs(text: string): ParsedLogs {
  const parsed = parseLogs(text);
  const records = parsed.records.filter((r) => OUTLINE_REFINE_LABELS.has(r.label));
  return { records, skipped: parsed.skipped, ignored: parsed.records.length - records.length };
}

/** Saved settings (or defaults). Read once, synchronously, so a reload can never overwrite them with defaults. */
export function loadState(): AppState {
  try {
    const raw = typeof localStorage === "undefined" ? null : localStorage.getItem(KEY);
    if (raw) return mergeState(JSON.parse(raw));
  } catch {
    /* ignore */
  }
  return defaultState();
}

export function StoreProvider({ children }: { children: ReactNode }) {
  const [state, set] = useState<AppState>(loadState);

  useEffect(() => {
    try {
      localStorage.setItem(KEY, JSON.stringify(state));
    } catch {
      /* ignore */
    }
  }, [state]);

  const logs = useMemo(() => parseOutlineLogs(state.logText), [state.logText]);
  const byLabel = useMemo(() => aggregate(logs.records, "label"), [logs]);
  const priced = useMemo(() => state.prices.filter(isPriced), [state.prices]);
  const current = priced.find((p) => p.id === PRODUCTION_MODEL_ID);

  const value: Ctx = {
    state,
    setState: (fn) => set((s) => fn(s)),
    reset: () => set(defaultState()),
    replace: (s) => set(s),
    priced,
    current,
    logs,
    byLabel,
  };
  return <StoreCtx.Provider value={value}>{children}</StoreCtx.Provider>;
}

export function useStore() {
  const c = useContext(StoreCtx);
  if (!c) throw new Error("useStore outside provider");
  return c;
}

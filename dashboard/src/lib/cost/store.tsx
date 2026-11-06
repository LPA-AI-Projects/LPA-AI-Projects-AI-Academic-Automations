import { createContext, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { defaultState, type AppState, type PriceRow } from "./model";
import { isPriced } from "./calc";
import { aggregate, parseLogs } from "./logs";

const KEY = "outline-ai-cost-console:v1";

interface Ctx {
  state: AppState;
  setState: (fn: (s: AppState) => AppState) => void;
  reset: () => void;
  replace: (s: AppState) => void;
  priced: PriceRow[];
  current: PriceRow | undefined;
  logs: ReturnType<typeof parseLogs>;
  byLabel: ReturnType<typeof aggregate>;
}

const StoreCtx = createContext<Ctx | null>(null);

export function mergeState(raw: unknown): AppState {
  const d = defaultState();
  if (!raw || typeof raw !== "object") return d;
  const r = raw as Partial<AppState>;
  return {
    ...d,
    ...r,
    version: 1,
    slides: { ...d.slides, ...(r.slides ?? {}) },
    scenario: { ...d.scenario, ...(r.scenario ?? {}) },
    compare: { ...d.compare, ...(r.compare ?? {}) },
    prices: Array.isArray(r.prices) ? r.prices : d.prices,
    steps: Array.isArray(r.steps) ? r.steps : d.steps,
    fixed: Array.isArray(r.fixed) ? r.fixed : d.fixed,
    logText: typeof r.logText === "string" ? r.logText : "",
  };
}

export function StoreProvider({ children }: { children: ReactNode }) {
  const [state, set] = useState<AppState>(defaultState);
  const loaded = useRef(false);

  useEffect(() => {
    try {
      const raw = localStorage.getItem(KEY);
      if (raw) set(mergeState(JSON.parse(raw)));
    } catch {
      /* ignore */
    }
    loaded.current = true;
  }, []);

  useEffect(() => {
    if (!loaded.current) return;
    try {
      localStorage.setItem(KEY, JSON.stringify(state));
    } catch {
      /* ignore */
    }
  }, [state]);

  const logs = useMemo(() => parseLogs(state.logText), [state.logText]);
  const byLabel = useMemo(() => aggregate(logs.records, "label"), [logs]);
  const priced = useMemo(() => state.prices.filter(isPriced), [state.prices]);
  const current = priced.find((p) => p.id === state.scenario.currentModelId) ?? priced[0];

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

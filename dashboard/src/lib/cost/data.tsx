import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";
import { ApiError, apiGet, clearKey, loadKey, saveKey, type Summary } from "./api";

const MONTH_KEY = "outline-cost-dashboard-month";

interface DataCtx {
  connected: boolean;
  status: "idle" | "loading" | "ready" | "error";
  error: string | null;
  summary: Summary | null;
  apiKey: string;
  /** Months that have requests, newest first. */
  months: string[];
  /** "all" or YYYY-MM. */
  month: string;
  setMonth: (m: string) => void;
  connect: (key: string) => Promise<boolean>;
  disconnect: () => void;
  refresh: () => Promise<void>;
}

const Ctx = createContext<DataCtx | null>(null);

function loadMonth(): string {
  try {
    return localStorage.getItem(MONTH_KEY) ?? "all";
  } catch {
    return "all";
  }
}

export function DataProvider({ children }: { children: ReactNode }) {
  const [apiKey, setApiKey] = useState<string>(loadKey);
  const [summary, setSummary] = useState<Summary | null>(null);
  const [status, setStatus] = useState<DataCtx["status"]>(apiKey ? "loading" : "idle");
  const [error, setError] = useState<string | null>(null);
  const [month, setMonthState] = useState<string>(loadMonth);

  const load = useCallback(async (key: string): Promise<boolean> => {
    setStatus("loading");
    setError(null);
    try {
      setSummary(await apiGet<Summary>("/summary", key));
      setStatus("ready");
      return true;
    } catch (e) {
      setStatus("error");
      setError(e instanceof ApiError || e instanceof Error ? e.message : "Could not load data.");
      if (e instanceof ApiError && e.status === 401) {
        clearKey();
        setApiKey("");
        setSummary(null);
      }
      return false;
    }
  }, []);

  useEffect(() => {
    if (apiKey) void load(apiKey);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const connect = useCallback(
    async (key: string) => {
      const k = key.trim();
      if (!k) return false;
      const ok = await load(k);
      if (ok) {
        saveKey(k);
        setApiKey(k);
      }
      return ok;
    },
    [load],
  );

  const disconnect = useCallback(() => {
    clearKey();
    setApiKey("");
    setSummary(null);
    setStatus("idle");
    setError(null);
  }, []);

  const refresh = useCallback(async () => {
    if (apiKey) await load(apiKey);
  }, [apiKey, load]);

  const months = useMemo(() => [...(summary?.months ?? [])].sort().reverse(), [summary]);
  const effectiveMonth = month === "all" || months.includes(month) || !summary ? month : "all";

  const setMonth = useCallback((m: string) => {
    setMonthState(m);
    try {
      localStorage.setItem(MONTH_KEY, m);
    } catch {
      /* ignore */
    }
  }, []);

  const value: DataCtx = {
    connected: !!apiKey && status !== "idle",
    status,
    error,
    summary,
    apiKey,
    months,
    month: effectiveMonth,
    setMonth,
    connect,
    disconnect,
    refresh,
  };
  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useData() {
  const c = useContext(Ctx);
  if (!c) throw new Error("useData outside provider");
  return c;
}

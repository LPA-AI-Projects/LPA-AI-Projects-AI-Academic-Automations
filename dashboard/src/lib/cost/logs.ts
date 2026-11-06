import type { Step } from "./model";

export interface LogRecord {
  date: string | null;
  label: string;
  model: string;
  input: number;
  output: number;
  cacheWrite: number;
  cacheRead: number;
  searches: number;
  stop: string;
  structured: boolean;
}

const RE = /Anthropic usage \| (.+)$/;

const num = (v?: string) => {
  if (!v || v === "None") return 0;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

export function parseLogs(text: string): { records: LogRecord[]; skipped: number } {
  const records: LogRecord[] = [];
  let skipped = 0;
  for (const raw of text.split(/\r?\n/)) {
    const line = raw.trim();
    if (!line) continue;
    const m = line.match(RE);
    if (!m) {
      skipped++;
      continue;
    }
    const kv: Record<string, string> = {};
    for (const part of (m[1] ?? "").trim().split(/\s+/)) {
      const i = part.indexOf("=");
      if (i > 0) kv[part.slice(0, i)] = part.slice(i + 1);
    }
    const label = kv["label"];
    if (!label) {
      skipped++;
      continue;
    }
    const d = line.slice(0, m.index).match(/(\d{4}-\d{2}-\d{2})/);
    records.push({
      date: d?.[1] ?? null,
      label,
      model: kv["model"] && kv["model"] !== "None" ? kv["model"] : "unknown",
      input: num(kv["input"]),
      output: num(kv["output"]),
      cacheWrite: num(kv["cache_write"]),
      cacheRead: num(kv["cache_read"]),
      searches: num(kv["web_searches"]),
      stop: kv["stop"] ?? "",
      structured: kv["structured"] === "True",
    });
  }
  return { records, skipped };
}

export interface Agg {
  key: string;
  calls: number;
  input: number;
  output: number;
  cacheWrite: number;
  cacheRead: number;
  searches: number;
  maxTokens: number;
}

export function aggregate(records: LogRecord[], by: "label" | "model"): Agg[] {
  const map = new Map<string, Agg>();
  for (const r of records) {
    const key = r[by];
    const a = map.get(key) ?? { key, calls: 0, input: 0, output: 0, cacheWrite: 0, cacheRead: 0, searches: 0, maxTokens: 0 };
    a.calls++;
    a.input += r.input;
    a.output += r.output;
    a.cacheWrite += r.cacheWrite;
    a.cacheRead += r.cacheRead;
    a.searches += r.searches;
    if (r.stop === "max_tokens") a.maxTokens++;
    map.set(key, a);
  }
  return [...map.values()].sort((a, b) => b.calls - a.calls);
}

export const hitRate = (a: { cacheWrite: number; cacheRead: number }) =>
  a.cacheRead + a.cacheWrite > 0 ? a.cacheRead / (a.cacheRead + a.cacheWrite) : null;

export function perDay(records: LogRecord[]) {
  const m = new Map<string, number>();
  for (const r of records) if (r.date) m.set(r.date, (m.get(r.date) ?? 0) + 1);
  return [...m.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([date, calls]) => ({ date, calls }));
}

/** Replace estimate tokens with real per-label averages. Cached tokens = cache_write + cache_read. */
export function applyLogAverages(steps: Step[], byLabel: Agg[]): Step[] {
  return steps.map((s) => {
    const a = byLabel.find((x) => x.key === s.label);
    if (!a || a.calls === 0) return s;
    const n = a.calls;
    return {
      ...s,
      system: Math.round((a.cacheWrite + a.cacheRead) / n),
      input: Math.round(a.input / n),
      searchInput: 0,
      output: Math.round(a.output / n),
      searches: a.searches / n,
    };
  });
}

/** Deterministic sample (~40 lines). */
export function sampleLogs(): string {
  let seed = 7;
  const rnd = () => ((seed = (seed * 16807) % 2147483647) / 2147483647);
  const jitter = (v: number, p = 0.15) => Math.round(v * (1 + (rnd() * 2 - 1) * p));
  const lines: string[] = [];
  const push = (day: number, label: string, input: number, output: number, cw: number, cr: number, ws: number | "None", stop = "end_turn", structured = "False") => {
    const h = String(8 + Math.floor(rnd() * 10)).padStart(2, "0");
    const mi = String(Math.floor(rnd() * 60)).padStart(2, "0");
    lines.push(
      `2026-11-0${day} ${h}:${mi}:12,481 INFO app.llm: Anthropic usage | label=${label} model=claude-sonnet-4-6 input=${input} output=${output} cache_write=${cw} cache_read=${cr} web_searches=${ws} stop=${stop} structured=${structured}`,
    );
  };
  for (let day = 1; day <= 5; day++) {
    const outlines = day === 3 ? 2 : 1;
    for (let k = 0; k < outlines; k++) {
      const warm = rnd() < 0.35;
      push(day, "learning_objectives", jitter(1300), jitter(1000), warm ? 0 : 1200, warm ? 1200 : 0, 0);
      push(day, "context_profile", jitter(1276), jitter(300), 0, 0, "None");
      push(day, "research", jitter(10600), jitter(800), 0, 0, 3);
      const jw = rnd() < 0.3;
      push(day, "outline_json", jitter(4300), jitter(6000), jw ? 0 : 4700, jw ? 4700 : 0, 0, rnd() < 0.12 ? "max_tokens" : "end_turn");
      if (day === 2) push(day, "outline_json", jitter(4300), jitter(6000), 0, 4700, 0);
      if (day === 4 && k === 0) push(day, "normalize_outline", jitter(6500), jitter(5800), 0, 0, 0);
    }
    push(day, "refine_outline", jitter(7400), jitter(6000), 0, 0, 0, day === 5 ? "max_tokens" : "end_turn");
    if (day % 2 === 1) push(day, "completion", jitter(900), jitter(200), 0, 0, "None");
  }
  push(5, "outline_text_fallback", jitter(6500), jitter(6000), 0, 0, 0);
  return lines.join("\n");
}

export function money(v: number | null | undefined, signed = false): string {
  if (v == null || !Number.isFinite(v)) return "—";
  const a = Math.abs(v);
  const d = a === 0 ? 2 : a >= 1 ? 2 : a >= 0.01 ? 3 : 4;
  const s = a.toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
  const sign = v < 0 ? "−" : signed && v > 0 ? "+" : "";
  return `${sign}$${s}`;
}
export const pct = (v: number | null | undefined, d = 1) =>
  v == null || !Number.isFinite(v) ? "—" : `${(v * 100).toFixed(d)}%`;
export const int = (v: number) => Math.round(v).toLocaleString("en-US");

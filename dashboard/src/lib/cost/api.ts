/** Client for the backend's /api/v1/cost-dashboard endpoints (they need the backend API key). */

export interface BucketCounts {
  outlines: number;
  completed: number;
  failed: number;
  other: number;
  refines: number;
}
export interface ByMonth {
  month: string; // YYYY-MM
  zoho: BucketCounts;
  bitrix: BucketCounts;
}
export interface UsageAgg {
  month: string;
  kind: string; // outline | refine
  label: string;
  model: string;
  calls: number;
  input: number;
  output: number;
  cache_write: number;
  cache_read: number;
  searches: number;
  jobs: number;
}
export interface Summary {
  generated_at: string;
  months: string[];
  by_month: ByMonth[];
  usage: UsageAgg[];
  usage_recording: { available: boolean; first_recorded: string | null };
}
export interface UsageTotals {
  calls: number;
  input: number;
  output: number;
  cache_write: number;
  cache_read: number;
  searches: number;
}
/** Same tokens, split by the model that was actually billed. */
export interface UsagePart extends UsageTotals {
  model: string;
}
export type Source = "zoho" | "bitrix";
export interface RequestItem {
  id: string;
  created_at: string | null;
  source: Source;
  record_id: string;
  course_name: string;
  company_name: string;
  status: string;
  failed: boolean;
  versions: number;
  refinements: number;
  pdf_url: string | null;
  usage: {
    outline: UsageTotals | null;
    refine: UsageTotals | null;
    outline_parts?: UsagePart[];
    refine_parts?: UsagePart[];
  };
}
export interface RequestList {
  total: number;
  limit: number;
  offset: number;
  items: RequestItem[];
}
export interface VersionInfo {
  version: number;
  created_at: string | null;
  feedback: string | null;
  pdf_url: string | null;
  is_refinement: boolean;
}
export interface UsageCall {
  kind: string;
  label: string;
  model: string;
  created_at: string | null;
  input: number;
  output: number;
  cache_write: number;
  cache_read: number;
  searches: number;
  stop: string | null;
  structured: boolean;
}
export interface RequestDetail {
  id: string;
  created_at: string | null;
  source: Source;
  record_id: string;
  status: string;
  error: string | null;
  pdf_url: string | null;
  inputs: Record<string, unknown>;
  versions: VersionInfo[];
  refinements: number;
  usage_calls: UsageCall[];
  responsible: { name: string | null; created_by: string | null; from: string } | null;
}

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}

const KEY_STORAGE = "outline-cost-dashboard-key";
const BASE = (import.meta.env["VITE_API_BASE"] as string | undefined) ?? "";

export function loadKey(): string {
  try {
    return sessionStorage.getItem(KEY_STORAGE) ?? "";
  } catch {
    return "";
  }
}
export function saveKey(key: string): void {
  try {
    sessionStorage.setItem(KEY_STORAGE, key);
  } catch {
    /* ignore */
  }
}
export function clearKey(): void {
  try {
    sessionStorage.removeItem(KEY_STORAGE);
  } catch {
    /* ignore */
  }
}

export async function apiGet<T>(path: string, key: string, params: Record<string, string | number | undefined> = {}): Promise<T> {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== "") qs.set(k, String(v));
  const url = `${BASE}/api/v1/cost-dashboard${path}${qs.size ? `?${qs}` : ""}`;
  const res = await fetch(url, { headers: { "X-API-Key": key } });
  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = (await res.json()) as { detail?: unknown };
      if (typeof body.detail === "string") detail = body.detail;
    } catch {
      /* not JSON */
    }
    throw new ApiError(res.status, res.status === 401 ? "That access key was not accepted." : detail || `Request failed (${res.status})`);
  }
  return (await res.json()) as T;
}

/** Naive UTC timestamps from the API -> local display. */
export function fmtDate(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(/[zZ]|[+-]\d\d:?\d\d$/.test(iso) ? iso : `${iso}Z`);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });
}

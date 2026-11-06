export interface PriceRow {
  id: string;
  model: string;
  provider: string;
  input: number | null;
  output: number | null;
  writeMult5m: number;
  writeMult1h: number;
  readMult: number;
  minCache: number;
  searchPer1k: number | null;
  /** Token-count multiplier vs. the older tokenizer (Claude 4.7+ produce ~30% more tokens for the same text). */
  tokenFactor: number;
  notes: string;
  lastVerified: string;
  qualityNotes: string;
}

export type StepKind = "once" | "json" | "fallback" | "refine";
/** Only course-outline generation and outline refinement are tracked. */
export type Job = "outline" | "refine";

/** The production model (ANTHROPIC_MODEL on the backend). The dashboard cannot be switched to another one. */
export const PRODUCTION_MODEL_ID = "sonnet-4-6";

export interface Step {
  id: string;
  job: Job;
  name: string;
  label: string; // log label
  kind: StepKind;
  calls: number;
  system: number; // cacheable system tokens
  input: number; // other input
  searchInput: number; // search-result input (uncached)
  output: number;
  searches: number;
  include: boolean;
}

export interface Scenario {
  outlines: number;
  refines: number;
  /** Month-over-month volume growth in percent, used by the 12-month projection. */
  monthlyGrowth: number;
  failRate: number; // 0..1
  maxAttempts: number;
  structured: boolean;
  structuredFailRate: number;
  caching: boolean;
  hitRate: number;
  window: "5m" | "1h";
  dataSource: "estimates" | "logs";
}

export interface FixedCost {
  id: string;
  name: string;
  amount: number;
}

export interface CompareSettings {
  selected: string[];
  workload: "outline" | "refine" | "monthly";
  caching: boolean;
  outputMult: number;
  searches: number;
  switchCost: number;
}

export interface AppState {
  version: 1;
  prices: PriceRow[];
  steps: Step[];
  scenario: Scenario;
  fixed: FixedCost[];
  compare: CompareSettings;
  logText: string;
}

const price = (
  id: string,
  model: string,
  provider: string,
  input: number | null,
  output: number | null,
  minCache: number,
  notes = "",
  o: Partial<Pick<PriceRow, "writeMult5m" | "writeMult1h" | "readMult" | "tokenFactor">> = {},
): PriceRow => ({
  id,
  model,
  provider,
  input,
  output,
  writeMult5m: o.writeMult5m ?? 1.25,
  writeMult1h: o.writeMult1h ?? 2,
  readMult: o.readMult ?? 0.1,
  minCache,
  searchPer1k: provider === "Anthropic" ? 10 : null,
  tokenFactor: o.tokenFactor ?? 1,
  notes,
  lastVerified: input === null ? "" : VERIFIED,
  qualityNotes: "",
});

const VERIFIED = "2026-11-06";
const NEW_TOKENIZER = "Newer tokenizer: ~30% more tokens for the same text (token factor 1.3).";

export function defaultState(): AppState {
  return {
    version: 1,
    prices: [
      price("sonnet-4-6", "Claude Sonnet 4.6", "Anthropic", 3, 15, 1024, "Production model (locked). Anthropic pricing page."),
      price("haiku-4-5", "Claude Haiku 4.5", "Anthropic", 1, 5, 4096, "Anthropic pricing page."),
      price("opus-4-5", "Claude Opus 4.5", "Anthropic", 5, 25, 4096, "Anthropic pricing page."),
      price("sonnet-5-5", "Claude Sonnet 5.5", "Anthropic", 2, 10, 512, `Anthropic pricing page. Cache reads 0.05x. ${NEW_TOKENIZER}`, { readMult: 0.05, tokenFactor: 1.3 }),
      price("haiku-5-5", "Claude Haiku 5.5", "Anthropic", 0.1, 0.5, 512, `Anthropic pricing page, prompts up to 100k tokens ($0.50 / $2.50 above). ${NEW_TOKENIZER}`, { tokenFactor: 1.3 }),
      price("opus-5-5", "Claude Opus 5.5", "Anthropic", 4, 20, 512, `Anthropic pricing page. Cache reads 0.05x. ${NEW_TOKENIZER}`, { readMult: 0.05, tokenFactor: 1.3 }),
      price("fable-5-1", "Claude Fable 5.1", "Anthropic", 10, 50, 512, `Anthropic pricing page. Cache reads 0.025x. ${NEW_TOKENIZER}`, { readMult: 0.025, tokenFactor: 1.3 }),
      price("gpt-5-4", "OpenAI GPT-5.4", "OpenAI", 2.5, 15, 1024, "From third-party listings (verify on openai.com/api/pricing). Caching is automatic: no write fee, cached input ~0.1x. Different tokenizer.", { writeMult5m: 1, writeMult1h: 1 }),
    ],
    steps: [
      { id: "lo", job: "outline", name: "Learning objectives", label: "learning_objectives", kind: "once", calls: 1, system: 1200, input: 1300, searchInput: 0, output: 1000, searches: 0, include: true },
      { id: "cp", job: "outline", name: "Context profile", label: "context_profile", kind: "once", calls: 1, system: 176, input: 1100, searchInput: 0, output: 300, searches: 0, include: true },
      { id: "rs", job: "outline", name: "Research (web search)", label: "research", kind: "once", calls: 1, system: 152, input: 2500, searchInput: 8000, output: 800, searches: 3, include: true },
      { id: "js", job: "outline", name: "Structured JSON outline", label: "outline_json", kind: "json", calls: 1, system: 4700, input: 4300, searchInput: 0, output: 6000, searches: 0, include: true },
      { id: "fb", job: "outline", name: "Text-outline fallback", label: "outline_text_fallback", kind: "fallback", calls: 1, system: 5000, input: 1500, searchInput: 0, output: 6000, searches: 0, include: true },
      { id: "rf", job: "refine", name: "Refine call", label: "refine_outline", kind: "refine", calls: 1, system: 1400, input: 6000, searchInput: 0, output: 6000, searches: 0, include: true },
    ],
    scenario: {
      outlines: 200,
      refines: 100,
      monthlyGrowth: 0,
      failRate: 0.1,
      maxAttempts: 3,
      structured: true, // backend default: ANTHROPIC_STRUCTURED_OUTPUT=true
      structuredFailRate: 0.01,
      caching: true,
      hitRate: 0.3,
      window: "5m",
      dataSource: "estimates",
    },
    fixed: [
      { id: "railway", name: "Railway hosting", amount: 5 },
      { id: "supabase", name: "Supabase", amount: 0 },
      { id: "openai", name: "OpenAI fallback usage", amount: 0 },
      { id: "other", name: "Other", amount: 0 },
    ],
    compare: {
      selected: [PRODUCTION_MODEL_ID, "sonnet-5-5", "haiku-4-5", "opus-4-5"],
      workload: "outline",
      caching: true,
      outputMult: 1,
      searches: 3,
      switchCost: 0,
    },
    logText: "",
  };
}

export const uid = () =>
  typeof crypto !== "undefined" && "randomUUID" in crypto
    ? crypto.randomUUID().slice(0, 8)
    : String(Date.now());

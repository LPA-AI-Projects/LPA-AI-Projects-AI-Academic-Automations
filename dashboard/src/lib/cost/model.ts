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
  notes: string;
  lastVerified: string;
  qualityNotes: string;
}

export type StepKind = "once" | "json" | "fallback" | "refine";
export type Job = "outline" | "refine" | "assessment";

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

export interface SlidesProfile {
  include: boolean;
  modules: number;
  plannerInput: number;
  plannerOutput: number;
  slideCalls: number;
  slideSystem: number;
  slideInput: number;
  slideOutput: number;
  validatorInput: number;
  validatorOutput: number;
  retryFactor: number;
}

export interface Scenario {
  outlines: number;
  refines: number;
  slidesJobs: number;
  assessments: number;
  failRate: number; // 0..1
  maxAttempts: number;
  structured: boolean;
  structuredFailRate: number;
  caching: boolean;
  hitRate: number;
  window: "5m" | "1h";
  currentModelId: string;
  dataSource: "estimates" | "logs";
}

export interface FixedCost {
  id: string;
  name: string;
  amount: number;
}

export interface CompareSettings {
  selected: string[];
  workload: "outline" | "refine" | "monthly" | "slides";
  caching: boolean;
  outputMult: number;
  searches: number;
  baselineId: string;
  switchCost: number;
}

export interface AppState {
  version: 1;
  prices: PriceRow[];
  steps: Step[];
  slides: SlidesProfile;
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
): PriceRow => ({
  id,
  model,
  provider,
  input,
  output,
  writeMult5m: 1.25,
  writeMult1h: 2,
  readMult: 0.1,
  minCache,
  searchPer1k: provider === "Anthropic" ? 10 : null,
  notes,
  lastVerified: "",
  qualityNotes: "",
});

const NEEDS = "Enter price from pricing page";

export function defaultState(): AppState {
  return {
    version: 1,
    prices: [
      price("sonnet-4-6", "Claude Sonnet 4.6", "Anthropic", 3, 15, 1024),
      price("haiku-4-5", "Claude Haiku 4.5", "Anthropic", 1, 5, 4096),
      price("opus-4-5", "Claude Opus 4.5", "Anthropic", 5, 25, 4096),
      price("sonnet-5-5", "Claude Sonnet 5.5", "Anthropic", null, null, 1024, NEEDS),
      price("haiku-5-5", "Claude Haiku 5.5", "Anthropic", null, null, 4096, NEEDS),
      price("opus-5-5", "Claude Opus 5.5", "Anthropic", null, null, 4096, NEEDS),
      price("fable-5-1", "Claude Fable 5.1", "Anthropic", null, null, 1024, NEEDS),
      price("gpt-5-4", "OpenAI GPT-5.4", "OpenAI", null, null, 1024, NEEDS),
    ],
    steps: [
      { id: "lo", job: "outline", name: "Learning objectives", label: "learning_objectives", kind: "once", calls: 1, system: 1200, input: 1300, searchInput: 0, output: 1000, searches: 0, include: true },
      { id: "cp", job: "outline", name: "Context profile", label: "context_profile", kind: "once", calls: 1, system: 176, input: 1100, searchInput: 0, output: 300, searches: 0, include: true },
      { id: "rs", job: "outline", name: "Research (web search)", label: "research", kind: "once", calls: 1, system: 152, input: 2500, searchInput: 8000, output: 800, searches: 3, include: true },
      { id: "js", job: "outline", name: "Structured JSON outline", label: "outline_json", kind: "json", calls: 1, system: 4700, input: 4300, searchInput: 0, output: 6000, searches: 0, include: true },
      { id: "fb", job: "outline", name: "Text-outline fallback", label: "outline_text_fallback", kind: "fallback", calls: 1, system: 5000, input: 1500, searchInput: 0, output: 6000, searches: 0, include: true },
      { id: "rf", job: "refine", name: "Refine call", label: "refine_outline", kind: "refine", calls: 1, system: 1400, input: 6000, searchInput: 0, output: 6000, searches: 0, include: true },
      { id: "as", job: "assessment", name: "Assessment (pre + post)", label: "assessment", kind: "once", calls: 2, system: 0, input: 6000, searchInput: 0, output: 2500, searches: 0, include: true },
    ],
    slides: {
      include: true,
      modules: 8,
      plannerInput: 15000,
      plannerOutput: 800,
      slideCalls: 15,
      slideSystem: 400,
      slideInput: 20000,
      slideOutput: 600,
      validatorInput: 40000,
      validatorOutput: 1500,
      retryFactor: 1.3,
    },
    scenario: {
      outlines: 200,
      refines: 100,
      slidesJobs: 0,
      assessments: 0,
      failRate: 0.1,
      maxAttempts: 3,
      structured: false,
      structuredFailRate: 0.01,
      caching: true,
      hitRate: 0.3,
      window: "5m",
      currentModelId: "sonnet-4-6",
      dataSource: "estimates",
    },
    fixed: [
      { id: "railway", name: "Railway hosting", amount: 5 },
      { id: "supabase", name: "Supabase", amount: 0 },
      { id: "gamma", name: "Gamma plan", amount: 0 },
      { id: "openai", name: "OpenAI fallback usage", amount: 0 },
      { id: "other", name: "Other", amount: 0 },
    ],
    compare: {
      selected: ["sonnet-4-6", "haiku-4-5", "opus-4-5"],
      workload: "outline",
      caching: true,
      outputMult: 1,
      searches: 3,
      baselineId: "sonnet-4-6",
      switchCost: 0,
    },
    logText: "",
  };
}

export const uid = () =>
  typeof crypto !== "undefined" && "randomUUID" in crypto
    ? crypto.randomUUID().slice(0, 8)
    : String(Date.now());

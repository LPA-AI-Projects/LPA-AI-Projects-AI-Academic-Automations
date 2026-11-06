import { Plus, SlidersHorizontal, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Checkbox } from "@/components/ui/checkbox";
import { Switch } from "@/components/ui/switch";
import { Slider } from "@/components/ui/slider";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle, SheetTrigger } from "@/components/ui/sheet";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { InfoTip, NumInput, Panel } from "./bits";
import { useStore } from "@/lib/cost/store";
import { isPriced } from "@/lib/cost/calc";
import { uid, type PriceRow, type Scenario, type SlidesProfile, type Step } from "@/lib/cost/model";
import { cn } from "@/lib/utils";

const th = "whitespace-nowrap px-2 py-2 text-left text-xs font-medium text-muted-foreground";
const td = "px-2 py-1.5 align-middle";

export function PriceSheetEditor() {
  const { state, setState } = useStore();
  const upd = (id: string, patch: Partial<PriceRow>) =>
    setState((s) => ({ ...s, prices: s.prices.map((p) => (p.id === id ? { ...p, ...patch } : p)) }));
  return (
    <Panel
      title="Price sheet (USD per 1M tokens)"
      tip="Rows with an empty input or output price are excluded from every calculation and shown greyed out."
      actions={
        <Button
          size="sm"
          variant="outline"
          onClick={() =>
            setState((s) => ({
              ...s,
              prices: [
                ...s.prices,
                { id: uid(), model: "New model", provider: "Anthropic", input: null, output: null, writeMult5m: 1.25, writeMult1h: 2, readMult: 0.1, minCache: 1024, searchPer1k: 10, tokenFactor: 1, notes: "Enter price from pricing page", lastVerified: "", qualityNotes: "" },
              ],
            }))
          }
        >
          <Plus className="mr-1 h-4 w-4" /> Add row
        </Button>
      }
      bodyClass="p-0 sm:p-0"
    >
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="border-b bg-muted/40">
            <tr>
              <th className={th}>Current</th>
              <th className={th}>Model</th>
              <th className={th}>Provider</th>
              <th className={th}>Input $</th>
              <th className={th}>Output $</th>
              <th className={th}>Write × (5m)</th>
              <th className={th}>Write × (1h)</th>
              <th className={th}>Read ×</th>
              <th className={th}>Min cacheable</th>
              <th className={th}>Search $/1k</th>
              <th className={th}>Token ×</th>
              <th className={th}>Notes</th>
              <th className={th}>Last verified</th>
              <th className={th} />
            </tr>
          </thead>
          <tbody>
            {state.prices.map((p) => {
              const priced = isPriced(p);
              return (
                <tr key={p.id} className={cn("border-b last:border-0", !priced && "bg-muted/40")}>
                  <td className={td}>
                    <input
                      type="radio"
                      name="current-model"
                      aria-label={`Use ${p.model} as current model`}
                      disabled={!priced}
                      checked={state.scenario.currentModelId === p.id}
                      onChange={() => setState((s) => ({ ...s, scenario: { ...s.scenario, currentModelId: p.id } }))}
                      className="accent-primary"
                    />
                  </td>
                  <td className={td}>
                    <Input value={p.model} onChange={(e) => upd(p.id, { model: e.target.value })} className={cn("h-8 min-w-[150px]", !priced && "text-muted-foreground")} />
                  </td>
                  <td className={td}>
                    <Select value={p.provider} onValueChange={(v) => upd(p.id, { provider: v })}>
                      <SelectTrigger className="h-8 w-[110px]"><SelectValue /></SelectTrigger>
                      <SelectContent>
                        {["Anthropic", "OpenAI", "Other"].map((x) => <SelectItem key={x} value={x}>{x}</SelectItem>)}
                      </SelectContent>
                    </Select>
                  </td>
                  <td className={td}><NumInput nullable value={p.input} placeholder="—" onChange={(v) => upd(p.id, { input: v })} aria-label="Input price" /></td>
                  <td className={td}><NumInput nullable value={p.output} placeholder="—" onChange={(v) => upd(p.id, { output: v })} aria-label="Output price" /></td>
                  <td className={td}><NumInput value={p.writeMult5m} onChange={(v) => upd(p.id, { writeMult5m: v ?? 0 })} /></td>
                  <td className={td}><NumInput value={p.writeMult1h} onChange={(v) => upd(p.id, { writeMult1h: v ?? 0 })} /></td>
                  <td className={td}><NumInput value={p.readMult} onChange={(v) => upd(p.id, { readMult: v ?? 0 })} /></td>
                  <td className={td}><NumInput value={p.minCache} onChange={(v) => upd(p.id, { minCache: v ?? 0 })} /></td>
                  <td className={td}><NumInput nullable value={p.searchPer1k} placeholder="n/a" onChange={(v) => upd(p.id, { searchPer1k: v })} /></td>
                  <td className={td}><NumInput value={p.tokenFactor ?? 1} onChange={(v) => upd(p.id, { tokenFactor: v ?? 1 })} aria-label="Token factor" /></td>
                  <td className={td}>
                    <div className="flex items-center gap-1.5">
                      {!priced && <Badge variant="warning" className="whitespace-nowrap">Enter price from pricing page</Badge>}
                      <Input value={p.notes} onChange={(e) => upd(p.id, { notes: e.target.value })} className="h-8 min-w-[140px]" />
                    </div>
                  </td>
                  <td className={td}><Input type="date" value={p.lastVerified} onChange={(e) => upd(p.id, { lastVerified: e.target.value })} className="h-8 w-[140px]" /></td>
                  <td className={td}>
                    <Button size="icon" variant="ghost" className="h-8 w-8" aria-label={`Delete ${p.model}`} onClick={() => setState((s) => ({ ...s, prices: s.prices.filter((x) => x.id !== p.id) }))}>
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </Panel>
  );
}

const JOB_LABEL: Record<Step["job"], string> = { outline: "One outline", refine: "One refine", assessment: "Assessment job" };
const KIND_LABEL: Record<Step["kind"], string> = { once: "1×", json: "× (1+f+f²)", fallback: "× f³", refine: "× (1+f+f²)" };

export function CallProfileEditor() {
  const { state, setState } = useStore();
  const upd = (id: string, patch: Partial<Step>) =>
    setState((s) => ({ ...s, steps: s.steps.map((x) => (x.id === id ? { ...x, ...patch } : x)) }));
  const sl = state.slides;
  const us = (patch: Partial<SlidesProfile>) => setState((s) => ({ ...s, slides: { ...s.slides, ...patch } }));
  const slideFields: [keyof SlidesProfile, string][] = [
    ["modules", "Modules"],
    ["plannerInput", "Planner input"],
    ["plannerOutput", "Planner output"],
    ["slideCalls", "Slide calls / module"],
    ["slideSystem", "Slide system"],
    ["slideInput", "Slide input"],
    ["slideOutput", "Slide output"],
    ["validatorInput", "Validator input"],
    ["validatorOutput", "Validator output"],
    ["retryFactor", "Retry factor"],
  ];
  return (
    <div className="space-y-4">
      <Panel title="Call profile (tokens per call)" tip="Cacheable = the unchanged system prompt. Search-result input is billed as normal input. Retry multipliers use the JSON failure rate f." bodyClass="p-0 sm:p-0">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="border-b bg-muted/40">
              <tr>
                {["Include", "Job", "Step", "Retry", "Calls", "Cacheable system", "Other input", "Search-result input", "Output", "Web searches"].map((h) => <th key={h} className={th}>{h}</th>)}
              </tr>
            </thead>
            <tbody>
              {state.steps.map((s) => (
                <tr key={s.id} className={cn("border-b last:border-0", !s.include && "opacity-50")}>
                  <td className={td}><Checkbox checked={s.include} onCheckedChange={(v) => upd(s.id, { include: !!v })} aria-label={`Include ${s.name}`} /></td>
                  <td className={cn(td, "whitespace-nowrap text-xs text-muted-foreground")}>{JOB_LABEL[s.job]}</td>
                  <td className={cn(td, "whitespace-nowrap font-medium")}>{s.name}</td>
                  <td className={cn(td, "whitespace-nowrap font-mono text-xs")}>{KIND_LABEL[s.kind]}</td>
                  <td className={td}><NumInput value={s.calls} onChange={(v) => upd(s.id, { calls: v ?? 0 })} /></td>
                  <td className={td}><NumInput value={s.system} onChange={(v) => upd(s.id, { system: v ?? 0 })} /></td>
                  <td className={td}><NumInput value={s.input} onChange={(v) => upd(s.id, { input: v ?? 0 })} /></td>
                  <td className={td}><NumInput value={s.searchInput} onChange={(v) => upd(s.id, { searchInput: v ?? 0 })} /></td>
                  <td className={td}><NumInput value={s.output} onChange={(v) => upd(s.id, { output: v ?? 0 })} /></td>
                  <td className={td}><NumInput value={s.searches} onChange={(v) => upd(s.id, { searches: v ?? 0 })} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
      <Panel
        title="Slides job (per course)"
        tip="Slides get no prompt-caching benefit: only system prompts are cacheable and they are tiny. Total = modules × (planner + slide calls × slide + validator) × retry factor."
        actions={
          <label className="flex items-center gap-2 text-sm">
            <Checkbox checked={sl.include} onCheckedChange={(v) => us({ include: !!v })} /> Include
          </label>
        }
      >
        <div className={cn("grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5", !sl.include && "opacity-50")}>
          {slideFields.map(([k, l]) => (
            <div key={k} className="space-y-1">
              <Label className="text-xs text-muted-foreground">{l}</Label>
              <NumInput value={sl[k] as number} onChange={(v) => us({ [k]: v ?? 0 } as Partial<SlidesProfile>)} />
            </div>
          ))}
        </div>
      </Panel>
    </div>
  );
}

export function ScenarioEditor() {
  const { state, setState } = useStore();
  const sc = state.scenario;
  const u = (patch: Partial<Scenario>) => setState((s) => ({ ...s, scenario: { ...s.scenario, ...patch } }));
  const vols: [keyof Scenario, string][] = [
    ["outlines", "Outlines / month"],
    ["refines", "Refines / month"],
    ["slidesJobs", "Slides jobs / month"],
    ["assessments", "Assessments / month"],
  ];
  return (
    <Panel title="Scenario controls" tip="Monthly volumes and reliability/caching assumptions used by every page.">
      <div className="grid gap-5 lg:grid-cols-2">
        <div className="grid grid-cols-2 gap-3">
          {vols.map(([k, l]) => (
            <div key={k} className="space-y-1">
              <Label className="text-xs text-muted-foreground">{l}</Label>
              <NumInput value={sc[k] as number} onChange={(v) => u({ [k]: Math.max(0, v ?? 0) } as Partial<Scenario>)} />
            </div>
          ))}
          <div className="space-y-1">
            <Label className="text-xs text-muted-foreground">Max JSON attempts</Label>
            <NumInput value={sc.maxAttempts} onChange={(v) => u({ maxAttempts: Math.max(1, Math.min(10, v ?? 3)) })} />
          </div>
          <div className="space-y-1">
            <Label className="flex items-center gap-1 text-xs text-muted-foreground">
              Structured-output failure % <InfoTip>Projected per-attempt failure rate used when the Structured output toggle is ON.</InfoTip>
            </Label>
            <NumInput value={+(sc.structuredFailRate * 100).toFixed(2)} onChange={(v) => u({ structuredFailRate: Math.max(0, Math.min(100, v ?? 0)) / 100 })} />
          </div>
        </div>
        <div className="space-y-5">
          <div className="space-y-2">
            <div className="flex justify-between text-sm"><span className="flex items-center gap-1">JSON failure rate per attempt <InfoTip>Share of structured JSON attempts that return invalid JSON and get retried.</InfoTip></span><span className="tabular font-medium">{(sc.failRate * 100).toFixed(0)}%</span></div>
            <Slider min={0} max={60} step={1} value={[sc.failRate * 100]} onValueChange={([v]) => u({ failRate: (v ?? 0) / 100 })} />
          </div>
          <div className="space-y-2">
            <div className="flex justify-between text-sm"><span className="flex items-center gap-1">Cache hit rate <InfoTip>Share of cacheable calls that land within the cache window of an earlier call with the identical prefix.</InfoTip></span><span className="tabular font-medium">{(sc.hitRate * 100).toFixed(0)}%</span></div>
            <Slider min={0} max={100} step={1} value={[sc.hitRate * 100]} onValueChange={([v]) => u({ hitRate: (v ?? 0) / 100 })} />
          </div>
          <div className="flex flex-wrap items-center gap-5">
            <label className="flex items-center gap-2 text-sm"><Switch checked={sc.caching} onCheckedChange={(v) => u({ caching: v })} /> Prompt caching</label>
            <label className="flex items-center gap-2 text-sm"><Switch checked={sc.structured} onCheckedChange={(v) => u({ structured: v })} /> Structured output</label>
            <div className="flex items-center gap-2 text-sm">
              Cache window
              <Select value={sc.window} onValueChange={(v) => u({ window: v as "5m" | "1h" })}>
                <SelectTrigger className="h-8 w-[100px]"><SelectValue /></SelectTrigger>
                <SelectContent><SelectItem value="5m">5 min</SelectItem><SelectItem value="1h">1 hour</SelectItem></SelectContent>
              </Select>
            </div>
          </div>
        </div>
      </div>
    </Panel>
  );
}

export function FixedCostsEditor() {
  const { state, setState } = useStore();
  return (
    <Panel
      title="Fixed monthly costs (USD)"
      tip="Flat monthly costs added to the all-in total."
      actions={
        <Button size="sm" variant="outline" onClick={() => setState((s) => ({ ...s, fixed: [...s.fixed, { id: uid(), name: "New cost", amount: 0 }] }))}>
          <Plus className="mr-1 h-4 w-4" /> Add
        </Button>
      }
    >
      <div className="space-y-2">
        {state.fixed.map((f) => (
          <div key={f.id} className="flex items-center gap-2">
            <Input value={f.name} onChange={(e) => setState((s) => ({ ...s, fixed: s.fixed.map((x) => (x.id === f.id ? { ...x, name: e.target.value } : x)) }))} className="h-8" />
            <NumInput value={f.amount} className="w-28" onChange={(v) => setState((s) => ({ ...s, fixed: s.fixed.map((x) => (x.id === f.id ? { ...x, amount: v ?? 0 } : x)) }))} />
            <Button size="icon" variant="ghost" className="h-8 w-8 shrink-0" aria-label={`Delete ${f.name}`} onClick={() => setState((s) => ({ ...s, fixed: s.fixed.filter((x) => x.id !== f.id) }))}>
              <Trash2 className="h-4 w-4" />
            </Button>
          </div>
        ))}
        <p className="pt-1 text-xs text-muted-foreground">Zoho, Bitrix and Google Drive have no per-call cost here.</p>
      </div>
    </Panel>
  );
}

export function AssumptionsDrawer() {
  return (
    <Sheet>
      <SheetTrigger asChild>
        <Button variant="outline" size="sm"><SlidersHorizontal className="mr-1.5 h-4 w-4" /> Edit assumptions</Button>
      </SheetTrigger>
      <SheetContent side="right" className="w-full overflow-y-auto sm:max-w-3xl">
        <SheetHeader>
          <SheetTitle>Assumptions</SheetTitle>
          <SheetDescription>Changes save automatically and update every page.</SheetDescription>
        </SheetHeader>
        <Tabs defaultValue="scenario" className="mt-4">
          <TabsList className="flex-wrap">
            <TabsTrigger value="scenario">Scenario</TabsTrigger>
            <TabsTrigger value="prices">Prices</TabsTrigger>
            <TabsTrigger value="profile">Call profile</TabsTrigger>
            <TabsTrigger value="fixed">Fixed costs</TabsTrigger>
          </TabsList>
          <TabsContent value="scenario"><ScenarioEditor /></TabsContent>
          <TabsContent value="prices"><PriceSheetEditor /></TabsContent>
          <TabsContent value="profile"><CallProfileEditor /></TabsContent>
          <TabsContent value="fixed"><FixedCostsEditor /></TabsContent>
        </Tabs>
      </SheetContent>
    </Sheet>
  );
}

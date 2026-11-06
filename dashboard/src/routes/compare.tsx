import { createFileRoute } from "@tanstack/react-router";
import { Bar, BarChart, CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { TriangleAlert } from "lucide-react";
import { PageHeader } from "@/components/app/AppShell";
import { AssumptionsDrawer } from "@/components/app/editors";
import { EmptyState, HowCalculated, InfoTip, NumInput, Panel, axisProps, tooltipStyle } from "@/components/app/bits";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Slider } from "@/components/ui/slider";
import { Switch } from "@/components/ui/switch";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { useStore } from "@/lib/cost/store";
import { compute } from "@/lib/cost/calc";
import { int, money, pct } from "@/lib/cost/format";
import type { CompareSettings, PriceRow } from "@/lib/cost/model";

export const Route = createFileRoute("/compare")({
  head: () => ({
    meta: [
      { title: "Price Comparison — Outline AI Cost Console" },
      { name: "description", content: "Compare models' cost per outline, refine and month against your current model." },
      { property: "og:title", content: "Price Comparison — Outline AI Cost Console" },
      { property: "og:description", content: "Compare models' cost per outline, refine and month against your current model." },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary" },
    ],
  }),
  component: Compare,
});

const C = ["var(--chart-1)", "var(--chart-3)", "var(--chart-4)", "var(--chart-5)", "var(--chart-6)", "var(--chart-2)", "var(--chart-7)"];

function Compare() {
  const { state, setState, priced } = useStore();
  const cs = state.compare;
  const u = (p: Partial<CompareSettings>) => setState((s) => ({ ...s, compare: { ...s.compare, ...p } }));
  const sel = priced.filter((p) => cs.selected.includes(p.id));
  const base = priced.find((p) => p.id === cs.baselineId) ?? priced[0];
  const run = (p: PriceRow, caching = cs.caching) => compute(state, p, { caching, outputMult: cs.outputMult, searchesOverride: cs.searches });
  const metric = (r: ReturnType<typeof run>) => cs.workload === "outline" ? r.perOutline : cs.workload === "refine" ? r.perRefine : cs.workload === "slides" ? r.perSlides : r.monthlyApi;

  const header = <PageHeader title="Price Comparison" desc="Compare priced models on the same workload." actions={<AssumptionsDrawer />} />;
  if (!priced.length || !base) return <>{header}<EmptyState title="No priced models">Enter input and output prices in the price sheet.</EmptyState></>;

  const baseR = run(base);
  const rows = sel.map((p) => ({ p, r: run(p) })).sort((a, b) => metric(a.r) - metric(b.r));
  const cheapest = rows[0];
  const bars = sel.map((p) => ({ name: p.model, "Caching ON": metric(run(p, true)), "Caching OFF": metric(run(p, false)) }));
  const maxN = Math.max(400, state.scenario.outlines * 2);
  const vol = Array.from({ length: 11 }, (_, i) => {
    const n = Math.round((maxN * i) / 10);
    const o: Record<string, number> = { n };
    rows.forEach(({ p, r }) => (o[p.model] = r.monthlyApi + (n - state.scenario.outlines) * r.perOutline));
    return o;
  });
  const savePerMonth = cheapest ? baseR.monthlyApi - cheapest.r.monthlyApi : 0;
  const savePerOutline = cheapest ? baseR.perOutline - cheapest.r.perOutline : 0;
  const providers = ["Anthropic", "OpenAI"].map((pv) => {
    const list = priced.filter((p) => p.provider === pv).map((p) => ({ p, r: run(p) })).sort((a, b) => a.r.monthlyApi - b.r.monthlyApi);
    return { pv, best: list[0] };
  });
  const openaiPriced = !!providers[1]?.best;

  return (
    <>
      {header}
      <div className="flex items-start gap-2 rounded-lg border border-warning/40 bg-warning/10 px-4 py-3 text-sm">
        <TriangleAlert className="mt-0.5 h-4 w-4 shrink-0 text-warning" />
        A cheaper model can change output quality - compare sample outputs before switching.
      </div>

      <Panel title="Controls">
        <div className="grid gap-5 lg:grid-cols-3">
          <div className="space-y-2">
            <div className="text-xs font-medium text-muted-foreground">Models (priced only)</div>
            <div className="flex flex-wrap gap-x-4 gap-y-2">
              {priced.map((p) => (
                <label key={p.id} className="flex items-center gap-2 text-sm">
                  <Checkbox checked={cs.selected.includes(p.id)} onCheckedChange={(v) => u({ selected: v ? [...cs.selected, p.id] : cs.selected.filter((x) => x !== p.id) })} /> {p.model}
                </label>
              ))}
            </div>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div className="space-y-1"><div className="text-xs text-muted-foreground">Workload</div>
              <Select value={cs.workload} onValueChange={(v) => u({ workload: v as CompareSettings["workload"] })}><SelectTrigger><SelectValue /></SelectTrigger><SelectContent><SelectItem value="outline">Per outline</SelectItem><SelectItem value="refine">Per refine</SelectItem><SelectItem value="monthly">Monthly scenario</SelectItem><SelectItem value="slides">Slides job</SelectItem></SelectContent></Select></div>
            <div className="space-y-1"><div className="text-xs text-muted-foreground">Current model (baseline)</div>
              <Select value={base.id} onValueChange={(v) => u({ baselineId: v })}><SelectTrigger><SelectValue /></SelectTrigger><SelectContent>{priced.map((p) => <SelectItem key={p.id} value={p.id}>{p.model}</SelectItem>)}</SelectContent></Select></div>
            <label className="col-span-2 flex items-center gap-2 text-sm"><Switch checked={cs.caching} onCheckedChange={(v) => u({ caching: v })} /> Prompt caching {cs.caching ? "ON" : "OFF"}</label>
          </div>
          <div className="space-y-4">
            <div className="space-y-2"><div className="flex justify-between text-sm"><span>Output-length multiplier</span><span className="tabular font-medium">{cs.outputMult.toFixed(2)}×</span></div><Slider min={0.5} max={1.5} step={0.05} value={[cs.outputMult]} onValueChange={([v]) => u({ outputMult: v ?? 1 })} /></div>
            <div className="space-y-2"><div className="flex justify-between text-sm"><span>Web searches per research call</span><span className="tabular font-medium">{cs.searches}</span></div><Slider min={0} max={10} step={1} value={[cs.searches]} onValueChange={([v]) => u({ searches: v ?? 0 })} /></div>
          </div>
        </div>
      </Panel>

      {!rows.length ? <EmptyState title="Select at least one model" /> : (
        <>
          <Panel title="Ranked comparison" tip="Sorted by the selected workload. Savings compare monthly Anthropic spend to the baseline." bodyClass="p-0 sm:p-0">
            <div className="overflow-x-auto"><table className="w-full text-sm tabular"><thead className="border-b bg-muted/40 text-xs text-muted-foreground"><tr>{["#", "Model", "Input $", "Output $", "Per outline", "Per refine", "Monthly", "% vs current", "Savings / mo", "Quality notes"].map((h) => <th key={h} className="whitespace-nowrap px-3 py-2 text-left font-medium">{h}</th>)}</tr></thead>
              <tbody>{rows.map(({ p, r }, i) => {
                const d = metric(baseR) ? metric(r) / metric(baseR) - 1 : 0;
                const sv = baseR.monthlyApi - r.monthlyApi;
                return <tr key={p.id} className="border-b last:border-0">
                  <td className="px-3 py-2">{i + 1}</td>
                  <td className="whitespace-nowrap px-3 font-medium">{p.model} {i === 0 && <Badge variant="success" className="ml-1">Cheapest</Badge>} {p.id === base.id && <Badge variant="muted" className="ml-1">Current</Badge>}</td>
                  <td className="px-3">{money(p.input)}</td><td className="px-3">{money(p.output)}</td>
                  <td className="px-3">{money(r.perOutline)}</td><td className="px-3">{money(r.perRefine)}</td><td className="px-3 font-semibold">{money(r.monthlyApi)}</td>
                  <td className={`px-3 ${d < 0 ? "text-success" : d > 0 ? "text-destructive" : ""}`}>{d > 0 ? "+" : ""}{pct(d)}</td>
                  <td className={`px-3 ${sv > 0 ? "text-success" : sv < 0 ? "text-destructive" : ""}`}>{money(sv, true)}</td>
                  <td className="px-3"><Input value={p.qualityNotes} placeholder="Your notes…" className="h-8 min-w-[180px]" onChange={(e) => setState((s) => ({ ...s, prices: s.prices.map((x) => (x.id === p.id ? { ...x, qualityNotes: e.target.value } : x)) }))} /></td>
                </tr>;
              })}</tbody></table></div>
          </Panel>

          <div className="grid gap-4 xl:grid-cols-2">
            <Panel title={`Cost (${cs.workload}) — caching ON vs OFF`}>
              <div className="h-72"><ResponsiveContainer><BarChart data={bars}><CartesianGrid stroke="var(--border)" vertical={false} /><XAxis dataKey="name" {...axisProps} /><YAxis {...axisProps} tickFormatter={(v) => money(v)} /><Tooltip {...tooltipStyle} formatter={(v: number) => money(v)} /><Legend wrapperStyle={{ fontSize: 11 }} /><Bar dataKey="Caching ON" fill="var(--chart-1)" radius={4} /><Bar dataKey="Caching OFF" fill="var(--chart-2)" radius={4} /></BarChart></ResponsiveContainer></div>
            </Panel>
            <Panel title="Monthly cost vs outlines per month">
              <div className="h-72"><ResponsiveContainer><LineChart data={vol}><CartesianGrid stroke="var(--border)" vertical={false} /><XAxis dataKey="n" {...axisProps} /><YAxis {...axisProps} tickFormatter={(v) => money(v)} /><Tooltip {...tooltipStyle} formatter={(v: number) => money(v)} /><Legend wrapperStyle={{ fontSize: 11 }} />{rows.map(({ p }, i) => <Line key={p.id} dataKey={p.model} stroke={C[i % C.length]} strokeWidth={2} dot={false} />)}</LineChart></ResponsiveContainer></div>
            </Panel>
            <Panel title="Break-even" tip="Payback = one-off switching cost ÷ savings per outline.">
              {cheapest && cheapest.p.id !== base.id && savePerMonth > 0 ? (
                <div className="space-y-3 text-sm">
                  <p>Switching from <b>{base.model}</b> to <b>{cheapest.p.model}</b> saves <span className="font-semibold text-success">{money(savePerMonth)}/month</span> ({money(savePerOutline)} per outline).</p>
                  <div className="flex items-center gap-2">One-off switching cost $ <NumInput value={cs.switchCost} className="w-28" onChange={(v) => u({ switchCost: v ?? 0 })} /><InfoTip>E.g. prompt re-tuning and QA time.</InfoTip></div>
                  <p>Payback at <b>{savePerOutline > 0 ? int(Math.ceil(cs.switchCost / savePerOutline)) : "—"}</b> outlines.</p>
                </div>
              ) : <EmptyState title="The current model is already the cheapest selected" />}
            </Panel>
            <Panel title="Provider comparison (Anthropic vs OpenAI)">
              {openaiPriced ? (
                <div className="grid grid-cols-2 gap-4">{providers.map(({ pv, best }) => <div key={pv} className="rounded-lg border p-3"><div className="text-xs text-muted-foreground">Cheapest {pv}</div>{best ? <><div className="font-medium">{best.p.model}</div><div className="tabular text-xl font-semibold">{money(best.r.monthlyApi)}<span className="text-xs font-normal text-muted-foreground">/mo</span></div><div className="text-xs text-muted-foreground">{money(best.r.perOutline)} per outline</div></> : <div className="text-sm text-muted-foreground">No priced model</div>}</div>)}</div>
              ) : <EmptyState title="OpenAI is not priced yet">Enter GPT-5.4 prices in the price sheet to compare providers.</EmptyState>}
            </Panel>
          </div>
        </>
      )}

      <HowCalculated>
        <p>Each model uses the same call profile and scenario as Cost Analysis, with this page's caching switch, output tokens × multiplier, and the research step's searches replaced by the slider value.</p>
        <p>% vs current = model cost ÷ baseline cost − 1 for the chosen workload. Savings/month = baseline monthly − model monthly (Anthropic tokens + web search, fixed costs excluded).</p>
      </HowCalculated>
    </>
  );
}

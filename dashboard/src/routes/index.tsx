import { createFileRoute } from "@tanstack/react-router";
import { useMemo } from "react";
import { Bar, BarChart, CartesianGrid, Cell, Legend, Line, LineChart, Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { toast } from "sonner";
import { PageHeader } from "@/components/app/AppShell";
import { AssumptionsDrawer } from "@/components/app/editors";
import { EmptyState, HowCalculated, InfoTip, Kpi, Panel, axisProps, tooltipStyle } from "@/components/app/bits";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { useStore } from "@/lib/cost/store";
import { breakEvenHitRate, compute, ledger, total, writeMult } from "@/lib/cost/calc";
import { applyLogAverages } from "@/lib/cost/logs";
import { int, money, pct } from "@/lib/cost/format";
import type { Scenario } from "@/lib/cost/model";

export const Route = createFileRoute("/")({
  head: () => ({
    meta: [
      { title: "Cost Analysis — Outline AI Cost Console" },
      { name: "description", content: "Per-outline and monthly Anthropic API cost, caching impact and retry waste." },
      { property: "og:title", content: "Cost Analysis — Outline AI Cost Console" },
      { property: "og:description", content: "Per-outline and monthly Anthropic API cost, caching impact and retry waste." },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary" },
    ],
  }),
  component: CostAnalysis,
});

const C = ["var(--chart-1)", "var(--chart-2)", "var(--chart-3)", "var(--chart-4)", "var(--chart-5)", "var(--chart-6)", "var(--chart-7)"];

function CostAnalysis() {
  const { state, setState, current, priced, byLabel } = useStore();
  const sc = state.scenario;
  const u = (p: Partial<Scenario>) => setState((s) => ({ ...s, scenario: { ...s.scenario, ...p } }));
  const useLogs = sc.dataSource === "logs" && byLabel.length > 0;
  const steps = useMemo(() => (useLogs ? applyLogAverages(state.steps, byLabel) : state.steps), [useLogs, state.steps, byLabel]);

  if (!current)
    return (
      <>
        <PageHeader title="Cost Analysis" desc="No priced model available." actions={<AssumptionsDrawer />} />
        <EmptyState title="Enter a price for at least one model">Open Edit assumptions → Prices.</EmptyState>
      </>
    );

  const r = compute(state, current, { steps });
  const on = compute(state, current, { steps, caching: true });
  const off = compute(state, current, { steps, caching: false });
  const structuredOn = compute(state, current, { steps, failRate: sc.structuredFailRate });
  const structuredOff = compute(state, current, { steps, failRate: sc.failRate });
  const cacheNet = off.monthlyApi - on.monthlyApi;
  const be = breakEvenHitRate(current, sc.window);
  const allSpend = r.allIn || 1;
  const lines = ledger(state, r);
  const ledgerTotal = lines.reduce((a, l) => a + l.amount, 0);
  const outputShare = r.monthlyApi ? r.monthly.output / r.monthlyApi : 0;

  const stepData = r.outline.rows.map((x) => ({
    name: x.step.name.replace(" (web search)", ""),
    "Uncached input": x.expected.uncached,
    "Cache write": x.expected.write,
    "Cache read": x.expected.read,
    Output: x.expected.output,
    "Web search": x.expected.search,
  }));
  const fx = (n: string) => state.fixed.filter((f) => f.name.toLowerCase().includes(n)).reduce((a, f) => a + f.amount, 0);
  const known = ["openai", "railway", "supabase", "gamma"];
  const donut = [
    { name: "Anthropic tokens", value: r.monthlyApi - r.monthlySearch },
    { name: "Web search", value: r.monthlySearch },
    { name: "OpenAI fallback", value: fx("openai") },
    { name: "Railway", value: fx("railway") },
    { name: "Supabase", value: fx("supabase") },
    { name: "Gamma", value: fx("gamma") },
    { name: "Other", value: state.fixed.filter((f) => !known.some((k) => f.name.toLowerCase().includes(k))).reduce((a, f) => a + f.amount, 0) },
  ].filter((d) => d.value > 0);
  const maxN = Math.max(400, sc.outlines * 2);
  const volData = Array.from({ length: 11 }, (_, i) => {
    const n = Math.round((maxN * i) / 10);
    return { n, "Caching ON": on.allIn + (n - sc.outlines) * on.perOutline, "Caching OFF": off.allIn + (n - sc.outlines) * off.perOutline };
  });
  const wm = writeMult(current, sc.window);
  const impact = on.outline.rows.filter((x) => x.eligible).map((x) => {
    const ip = (current.input ?? 0) / 1e6;
    const Ct = x.step.system * x.step.calls * x.mult;
    return { name: x.step.name, "Write premium": Ct * ip * (1 - sc.hitRate) * (wm - 1), "Read savings": Ct * ip * sc.hitRate * (1 - current.readMult) };
  });

  const insights = [
    cacheNet >= 0
      ? `Caching saves ${money(cacheNet)}/month at a ${pct(sc.hitRate, 0)} hit rate; break-even is ${pct(be)}.`
      : `Caching costs an extra ${money(-cacheNet)}/month at a ${pct(sc.hitRate, 0)} hit rate — below the ${pct(be)} break-even.`,
    `Output tokens are ${pct(outputShare, 0)} of model spend.`,
    `Failed JSON retries and fallback add ${money(r.wastePerOutline)} per outline (${money(r.wastePerOutline * sc.outlines)}/month).`,
    `Web search is ${pct(r.monthlySearch / allSpend)} of all-in spend.`,
    `Fixed costs are ${pct(r.fixedTotal / allSpend, 0)} of the ${money(r.allIn)} monthly total.`,
  ];

  const csv = () => {
    const rows = [["Group", "Item", "Detail", "USD"], ...lines.map((l) => [l.group, l.name, l.detail, l.amount.toFixed(4)]), ["", "Total", "", ledgerTotal.toFixed(4)]];
    const blob = new Blob([rows.map((x) => x.map((c) => `"${String(c).replace(/"/g, '""')}"`).join(",")).join("\n")], { type: "text/csv" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "outline-ai-costs.csv";
    a.click();
  };
  const copy = () => {
    navigator.clipboard.writeText(
      `Outline AI cost summary (${current.model}, caching ${sc.caching ? "ON" : "OFF"})\nPer outline: ${money(r.perOutline)}\nPer refine: ${money(r.perRefine)}\nMonthly Anthropic: ${money(r.monthlyApi)}\nMonthly all-in: ${money(r.allIn)}\n${insights.map((i) => "- " + i).join("\n")}`,
    );
    toast.success("Summary copied");
  };

  return (
    <>
      <PageHeader title="Cost Analysis" desc={`${current.model} · ${int(sc.outlines)} outlines, ${int(sc.refines)} refines / month`} actions={<AssumptionsDrawer />} />

      <div className="card-surface grid gap-4 p-4 lg:grid-cols-[1.4fr_1fr_1fr_1fr]">
        <div className="flex items-start gap-3">
          <Switch checked={sc.caching} onCheckedChange={(v) => u({ caching: v })} aria-label="Prompt caching" className="mt-1" />
          <div>
            <div className="flex items-center gap-2 text-base font-semibold">Prompt caching <Badge variant={sc.caching ? "success" : "muted"}>{sc.caching ? "ON" : "OFF"}</Badge></div>
            <p className="text-xs text-muted-foreground">Reuses the unchanged system prompt; cached reads bill at ~10%, writes at 125%</p>
          </div>
        </div>
        <label className="flex items-center gap-3 text-sm font-medium">
          <Switch checked={sc.structured} onCheckedChange={(v) => u({ structured: v })} /> Structured output
          <InfoTip>When ON, the projected JSON failure rate becomes {pct(sc.structuredFailRate)} (editable in Scenario).</InfoTip>
        </label>
        <Select value={current.id} onValueChange={(v) => u({ currentModelId: v })}>
          <SelectTrigger aria-label="Current model"><SelectValue /></SelectTrigger>
          <SelectContent>{priced.map((p) => <SelectItem key={p.id} value={p.id}>{p.model}</SelectItem>)}</SelectContent>
        </Select>
        <Select value={sc.dataSource} onValueChange={(v) => u({ dataSource: v as Scenario["dataSource"] })}>
          <SelectTrigger aria-label="Data source"><SelectValue /></SelectTrigger>
          <SelectContent><SelectItem value="estimates">Estimates</SelectItem><SelectItem value="logs">Imported logs</SelectItem></SelectContent>
        </Select>
      </div>
      {sc.dataSource === "logs" && !useLogs && <EmptyState title="No imported logs yet">Import logs on Data & Settings — estimates are used meanwhile.</EmptyState>}

      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-6">
        <Kpi label="Cost per outline" value={money(r.perOutline)} sub={<>Caching {sc.caching ? "saves" : "would save"} {money(off.perOutline - on.perOutline, true)} vs off</>} tip="Expected cost of one outline including JSON retries (1+f+f²) and fallback (f³)." />
        <Kpi label="Cost per refine" value={money(r.perRefine)} tip="Refine call × expected attempts (1+f+f²)." />
        <Kpi label="Monthly Anthropic spend" value={money(r.monthlyApi)} tip="Outlines, refines, slides and assessments × volumes, incl. web search." />
        <Kpi label="Monthly all-in" value={money(r.allIn)} sub={`incl. ${money(r.fixedTotal)} fixed`} tip="Anthropic spend plus fixed monthly costs." />
        <Kpi label="Caching net effect / month" value={money(cacheNet, true)} tone={cacheNet >= 0 ? "success" : "danger"} sub={cacheNet >= 0 ? "saving" : "extra cost"} tip="Monthly Anthropic cost with caching OFF minus with caching ON." />
        <Kpi label="Web-search share" value={pct(r.monthlySearch / allSpend)} tip="Web-search fees ÷ all-in monthly spend." />
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <Panel title="Cost per outline by step" tip="Expected cost incl. retry multipliers, split by billing component.">
          <div className="h-72"><ResponsiveContainer><BarChart data={stepData}><CartesianGrid stroke="var(--border)" vertical={false} /><XAxis dataKey="name" {...axisProps} interval={0} angle={-15} textAnchor="end" height={50} /><YAxis {...axisProps} tickFormatter={(v) => money(v)} /><Tooltip {...tooltipStyle} formatter={(v: number) => money(v)} /><Legend wrapperStyle={{ fontSize: 11 }} />
            {["Uncached input", "Cache write", "Cache read", "Output", "Web search"].map((k, i) => <Bar key={k} dataKey={k} stackId="a" fill={[C[1], C[4], C[2], C[0], C[3]][i]} />)}
          </BarChart></ResponsiveContainer></div>
        </Panel>
        <Panel title="Monthly cost split" tip="All-in monthly spend by category.">
          {donut.length ? <div className="h-72"><ResponsiveContainer><PieChart><Pie data={donut} dataKey="value" nameKey="name" innerRadius="55%" outerRadius="85%" paddingAngle={2}>{donut.map((_, i) => <Cell key={i} fill={C[i % C.length]} />)}</Pie><Tooltip {...tooltipStyle} formatter={(v: number) => money(v)} /><Legend wrapperStyle={{ fontSize: 11 }} /></PieChart></ResponsiveContainer></div> : <EmptyState title="No monthly spend" />}
        </Panel>
        <Panel title="Monthly cost vs outlines per month" tip="All-in monthly cost as outline volume changes; other volumes held constant.">
          <div className="h-72"><ResponsiveContainer><LineChart data={volData}><CartesianGrid stroke="var(--border)" vertical={false} /><XAxis dataKey="n" {...axisProps} /><YAxis {...axisProps} tickFormatter={(v) => money(v)} /><Tooltip {...tooltipStyle} formatter={(v: number) => money(v)} /><Legend wrapperStyle={{ fontSize: 11 }} /><Line dataKey="Caching ON" stroke={C[2]} strokeWidth={2} dot={false} /><Line dataKey="Caching OFF" stroke={C[4]} strokeWidth={2} dot={false} /></LineChart></ResponsiveContainer></div>
        </Panel>
        <Panel title="Caching impact by step (per outline)" tip="Red: extra write premium on misses. Green: savings on cache reads. Steps below the model's minimum cacheable tokens are omitted.">
          {impact.length ? <div className="h-72"><ResponsiveContainer><BarChart data={impact}><CartesianGrid stroke="var(--border)" vertical={false} /><XAxis dataKey="name" {...axisProps} /><YAxis {...axisProps} tickFormatter={(v) => money(v)} /><Tooltip {...tooltipStyle} formatter={(v: number) => money(v)} /><Legend wrapperStyle={{ fontSize: 11 }} /><Bar dataKey="Write premium" fill="var(--destructive)" radius={4} /><Bar dataKey="Read savings" fill="var(--success)" radius={4} /></BarChart></ResponsiveContainer></div> : <EmptyState title="No cache-eligible steps">No system prompt meets {current.model}'s {int(current.minCache)}-token minimum.</EmptyState>}
        </Panel>
        <Panel title="Retry and fallback waste" tip="Extra expected cost per outline from failed JSON attempts and the text fallback.">
          <div className="grid grid-cols-2 gap-4">
            <div><div className="text-xs text-muted-foreground">Now (f = {pct(r.f, 0)})</div><div className="tabular text-2xl font-semibold text-warning">{money(r.wastePerOutline)}</div><div className="text-xs text-muted-foreground">per outline · {money(r.wastePerOutline * sc.outlines)}/mo</div></div>
            <div><div className="text-xs text-muted-foreground">Structured OFF → ON</div><div className="tabular text-sm">{money(structuredOff.wastePerOutline)} → <span className="font-semibold text-success">{money(structuredOn.wastePerOutline)}</span></div><div className="text-xs text-muted-foreground">saves {money((structuredOff.perOutline - structuredOn.perOutline) * sc.outlines + (structuredOff.perRefine - structuredOn.perRefine) * sc.refines)}/mo</div></div>
          </div>
        </Panel>
        <Panel title="Break-even cache hit rate" tip="h* = (writeMult − 1) / (writeMult − readMult). Above it, caching saves money.">
          <Gauge value={sc.hitRate} be={be} />
        </Panel>
      </div>

      <Panel title="Insights">
        <ul className="list-disc space-y-1.5 pl-5 text-sm">{insights.map((i) => <li key={i}>{i}</li>)}</ul>
      </Panel>

      <Panel title="Step-by-step cost (per job)" bodyClass="p-0 sm:p-0">
        <div className="overflow-x-auto"><table className="w-full text-sm tabular"><thead className="border-b bg-muted/40 text-xs text-muted-foreground"><tr>{["Step", "×", "Cached", "Input", "Output", "Searches", "Uncached $", "Write $", "Read $", "Output $", "Search $", "Expected $", "% of job"].map((h) => <th key={h} className="whitespace-nowrap px-3 py-2 text-left font-medium">{h}</th>)}</tr></thead>
          <tbody>{[...r.outline.rows, ...r.refine.rows, ...r.assessment.rows].map((x) => {
            const jt = x.step.job === "outline" ? r.perOutline : x.step.job === "refine" ? r.perRefine : r.perAssessment;
            const e = x.expected;
            return <tr key={x.step.id} className="border-b last:border-0"><td className="whitespace-nowrap px-3 py-2 font-medium">{x.step.name}</td><td className="px-3">{x.mult.toFixed(3)}</td><td className="px-3">{int(x.step.system)}</td><td className="px-3">{int(x.step.input + x.step.searchInput)}</td><td className="px-3">{int(x.step.output)}</td><td className="px-3">{x.step.searches}</td><td className="px-3">{money(e.uncached)}</td><td className="px-3">{money(e.write)}</td><td className="px-3">{money(e.read)}</td><td className="px-3">{money(e.output)}</td><td className="px-3">{money(e.search)}</td><td className="px-3 font-semibold">{money(total(e))}</td><td className="px-3">{pct(jt ? total(e) / jt : 0)}</td></tr>;
          })}</tbody></table></div>
      </Panel>

      {useLogs && (
        <Panel title="Actual vs estimate (tokens per call)" bodyClass="p-0 sm:p-0">
          <div className="overflow-x-auto"><table className="w-full text-sm tabular"><thead className="border-b bg-muted/40 text-xs text-muted-foreground"><tr>{["Step", "Est. cached", "Actual cached", "Est. input", "Actual input", "Est. output", "Actual output"].map((h) => <th key={h} className="px-3 py-2 text-left font-medium">{h}</th>)}</tr></thead>
            <tbody>{state.steps.map((e) => { const a = steps.find((s) => s.id === e.id)!; const has = byLabel.some((b) => b.key === e.label); return <tr key={e.id} className="border-b last:border-0"><td className="px-3 py-2 font-medium">{e.name}</td><td className="px-3">{int(e.system)}</td><td className="px-3">{has ? int(a.system) : "—"}</td><td className="px-3">{int(e.input + e.searchInput)}</td><td className="px-3">{has ? int(a.input) : "—"}</td><td className="px-3">{int(e.output)}</td><td className="px-3">{has ? int(a.output) : "—"}</td></tr>; })}</tbody></table></div>
        </Panel>
      )}

      <Panel title="All costs ledger (monthly)" actions={<><Button size="sm" variant="outline" onClick={csv}>Export CSV</Button><Button size="sm" variant="outline" onClick={copy}>Copy summary</Button></>} bodyClass="p-0 sm:p-0">
        <div className="overflow-x-auto"><table className="w-full text-sm tabular"><thead className="border-b bg-muted/40 text-xs text-muted-foreground"><tr>{["Group", "Item", "Detail", "Monthly $"].map((h) => <th key={h} className="px-3 py-2 text-left font-medium">{h}</th>)}</tr></thead>
          <tbody>{lines.map((l, i) => <tr key={i} className="border-b"><td className="whitespace-nowrap px-3 py-2 text-muted-foreground">{l.group}</td><td className="whitespace-nowrap px-3">{l.name}</td><td className="whitespace-nowrap px-3 text-xs text-muted-foreground">{l.detail}</td><td className="px-3">{money(l.amount)}</td></tr>)}
            <tr className="bg-muted/40 font-semibold"><td className="px-3 py-2" colSpan={3}>Total</td><td className="px-3">{money(ledgerTotal)}</td></tr></tbody></table></div>
      </Panel>

      <HowCalculated>
        <p>Per step with cacheable tokens C, other input U, output O, searches S. Caching OFF: <code>input = (C+U)·inputPrice</code>. Caching ON and C ≥ min cacheable: <code>input = U·inputPrice + C·inputPrice·(h·read + (1−h)·write)</code>; below the minimum caching has no effect.</p>
        <p><code>output = O·outputPrice</code>, <code>search = S·searchPrice/1000</code>. Prices are per 1M tokens.</p>
        <p>Expected JSON calls per outline <code>= 1 + f + f²</code> (max 3 attempts); fallback probability <code>= f³</code>. Refine retries the same way. Structured output ON replaces f with its projected rate.</p>
        <p>Monthly = outlines·perOutline + refines·perRefine + slides·perSlides + assessments·perAssessment; all-in adds fixed costs. Break-even <code>h* = (write − 1)/(write − read)</code>. Slides bill all tokens as uncached input. Imported-logs mode replaces token estimates with per-label averages (cached = cache_write + cache_read).</p>
      </HowCalculated>
    </>
  );
}

function Gauge({ value, be }: { value: number; be: number }) {
  const ok = value >= be;
  const pt = (v: number): [number, number] => { const a = Math.PI * (1 - v); return [100 + 80 * Math.cos(a), 100 - 80 * Math.sin(a)]; };
  const [vx, vy] = pt(value);
  const [bx, by] = pt(be);
  return (
    <div className="flex flex-col items-center">
      <svg viewBox="0 0 200 115" className="w-full max-w-xs">
        <path d="M20 100 A80 80 0 0 1 180 100" fill="none" stroke="var(--muted)" strokeWidth="16" strokeLinecap="round" />
        <path d={`M20 100 A80 80 0 0 1 ${vx} ${vy}`} fill="none" stroke={ok ? "var(--success)" : "var(--destructive)"} strokeWidth="16" strokeLinecap="round" />
        <line x1={100 + 64 * Math.cos(Math.PI * (1 - be))} y1={100 - 64 * Math.sin(Math.PI * (1 - be))} x2={bx + 12 * Math.cos(Math.PI * (1 - be))} y2={by - 12 * Math.sin(Math.PI * (1 - be))} stroke="var(--foreground)" strokeWidth="2.5" />
        <text x="100" y="95" textAnchor="middle" className="fill-foreground" fontSize="22" fontWeight="600">{pct(value, 0)}</text>
      </svg>
      <p className="text-sm">Break-even <span className="font-semibold">{pct(be)}</span> · <span className={ok ? "text-success" : "text-destructive"}>{ok ? "above — caching pays off" : "below — caching costs more"}</span></p>
    </div>
  );
}

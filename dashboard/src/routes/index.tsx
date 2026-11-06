import { createFileRoute } from "@tanstack/react-router";
import { useMemo } from "react";
import { Bar, BarChart, CartesianGrid, Cell, Legend, Line, LineChart, Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { Lock } from "lucide-react";
import { toast } from "sonner";
import { PageHeader } from "@/components/app/AppShell";
import { AssumptionsDrawer } from "@/components/app/editors";
import { EmptyState, HowCalculated, InfoTip, Kpi, NumInput, Panel, axisProps, tooltipStyle } from "@/components/app/bits";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { useStore } from "@/lib/cost/store";
import { useData } from "@/lib/cost/data";
import { monthlyCosts, sumMonths } from "@/lib/cost/dbcost";
import { breakEvenHitRate, compute, ledger, monthlyActual, projectMonths, total, writeMult } from "@/lib/cost/calc";
import { applyLogAverages, hitRate } from "@/lib/cost/logs";
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
  const { state, setState, current, priced, byLabel, logs } = useStore();
  const { summary, month } = useData();
  const sc = state.scenario;
  const u = (p: Partial<Scenario>) => setState((s) => ({ ...s, scenario: { ...s.scenario, ...p } }));
  const useLogs = sc.dataSource === "logs" && byLabel.length > 0;
  const steps = useMemo(() => (useLogs ? applyLogAverages(state.steps, byLabel) : state.steps), [useLogs, state.steps, byLabel]);

  if (!current)
    return (
      <>
        <PageHeader title="Cost Analysis" desc="The production model has no price yet." actions={<AssumptionsDrawer />} />
        <EmptyState title="Enter a price for the production model">Open Edit assumptions → Prices and fill in Claude Sonnet 4.6.</EmptyState>
      </>
    );

  // Month filter: with the database connected, volumes become that month's real counts.
  const monthRow = summary && month !== "all" ? summary.by_month.find((m) => m.month === month) : undefined;
  const view = monthRow
    ? { ...state, scenario: { ...sc, outlines: monthRow.zoho.completed + monthRow.bitrix.completed, refines: monthRow.zoho.refines + monthRow.bitrix.refines } }
    : state;
  const vsc = view.scenario;
  const r = compute(view, current, { steps });
  const on = compute(view, current, { steps, caching: true });
  const off = compute(view, current, { steps, caching: false });
  const structuredOn = compute(view, current, { steps, failRate: sc.structuredFailRate });
  const structuredOff = compute(view, current, { steps, failRate: sc.failRate });
  const dbRows = summary ? monthlyCosts(summary, priced, current, sc.window, compute(state, current, { steps }).perOutline, compute(state, current, { steps }).perRefine) : [];
  const dbShown = monthRow ? dbRows.find((x) => x.month === month) : dbRows.length ? sumMonths(dbRows) : undefined;
  const dbTotal = sumMonths(dbRows);
  const cacheNet = off.monthlyApi - on.monthlyApi;
  const be = breakEvenHitRate(current, sc.window);
  const allSpend = r.allIn || 1;
  const lines = ledger(view, r);
  const ledgerTotal = lines.reduce((a, l) => a + l.amount, 0);
  const outputShare = r.monthlyApi ? r.monthly.output / r.monthlyApi : 0;
  const proj = projectMonths(state, compute(state, current, { steps }));
  const projSum = proj.reduce(
    (a, x) => ({ outlines: a.outlines + x.outlines, refines: a.refines + x.refines, anthropic: a.anthropic + x.anthropic, fixed: a.fixed + x.fixed, total: a.total + x.total }),
    { outlines: 0, refines: 0, anthropic: 0, fixed: 0, total: 0 },
  );
  const actual = monthlyActual(logs.records, priced, current, sc.window);
  const actualSum = actual.rows.reduce(
    (a, x) => ({ outlines: a.outlines + x.outlines, refines: a.refines + x.refines, calls: a.calls + x.calls, input: a.input + x.input, output: a.output + x.output, cw: a.cw + x.cacheWrite, cr: a.cr + x.cacheRead, searches: a.searches + x.searches, cost: a.cost + x.cost, noCache: a.noCache + x.noCache }),
    { outlines: 0, refines: 0, calls: 0, input: 0, output: 0, cw: 0, cr: 0, searches: 0, cost: 0, noCache: 0 },
  );

  const stepData = r.outline.rows.map((x) => ({
    name: x.step.name.replace(" (web search)", ""),
    "Uncached input": x.expected.uncached,
    "Cache write": x.expected.write,
    "Cache read": x.expected.read,
    Output: x.expected.output,
    "Web search": x.expected.search,
  }));
  const fx = (n: string) => state.fixed.filter((f) => f.name.toLowerCase().includes(n)).reduce((a, f) => a + f.amount, 0);
  const known = ["openai", "railway", "supabase"];
  const donut = [
    { name: "Anthropic tokens", value: r.monthlyApi - r.monthlySearch },
    { name: "Web search", value: r.monthlySearch },
    { name: "OpenAI fallback", value: fx("openai") },
    { name: "Railway", value: fx("railway") },
    { name: "Supabase", value: fx("supabase") },
    { name: "Other", value: state.fixed.filter((f) => !known.some((k) => f.name.toLowerCase().includes(k))).reduce((a, f) => a + f.amount, 0) },
  ].filter((d) => d.value > 0);
  const maxN = Math.max(400, vsc.outlines * 2);
  const volData = Array.from({ length: 11 }, (_, i) => {
    const n = Math.round((maxN * i) / 10);
    return { n, "Caching ON": on.allIn + (n - vsc.outlines) * on.perOutline, "Caching OFF": off.allIn + (n - vsc.outlines) * off.perOutline };
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
    `Failed JSON retries and fallback add ${money(r.wastePerOutline)} per outline (${money(r.wastePerOutline * vsc.outlines)}/month).`,
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
      <PageHeader title="Cost Analysis" desc={monthRow ? `${current.model} · ${month}: ${int(vsc.outlines)} outlines, ${int(vsc.refines)} refinements (actual counts from the database)` : `${current.model} · ${int(sc.outlines)} outlines, ${int(sc.refines)} refines / month`} actions={<AssumptionsDrawer />} />

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
        <div className="flex h-9 items-center gap-2 rounded-md border bg-muted/40 px-3 text-sm font-medium" title="The production model is fixed and cannot be changed here.">
          <Lock className="h-3.5 w-3.5 shrink-0 text-muted-foreground" /> <span className="truncate">{current.model}</span>
        </div>
        <Select value={sc.dataSource} onValueChange={(v) => u({ dataSource: v as Scenario["dataSource"] })}>
          <SelectTrigger aria-label="Data source"><SelectValue /></SelectTrigger>
          <SelectContent><SelectItem value="estimates">Estimates</SelectItem><SelectItem value="logs">Imported logs</SelectItem></SelectContent>
        </Select>
      </div>
      {sc.dataSource === "logs" && !useLogs && <EmptyState title="No imported logs yet">Import logs on Data & Settings — estimates are used meanwhile.</EmptyState>}

      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <Kpi label="Cost per outline" value={money(r.perOutline)} sub={<>Caching {sc.caching ? "saves" : "would save"} {money(off.perOutline - on.perOutline, true)} vs off</>} tip="Expected cost of one outline including JSON retries (1+f+f²) and fallback (f³)." />
        <Kpi label="Cost per refine" value={money(r.perRefine)} tip="Refine call × expected attempts (1+f+f²)." />
        <Kpi label="Monthly Anthropic spend" value={money(r.monthlyApi)} tip="Outlines and refines × monthly volumes, incl. web search." />
        <Kpi label="Monthly all-in" value={money(r.allIn)} sub={`incl. ${money(r.fixedTotal)} fixed`} tip="Anthropic spend plus fixed monthly costs." />
        <Kpi label="Caching net effect / month" value={money(cacheNet, true)} tone={cacheNet >= 0 ? "success" : "danger"} sub={cacheNet >= 0 ? "saving" : "extra cost"} tip="Monthly Anthropic cost with caching OFF minus with caching ON." />
        <Kpi label="Web-search share" value={pct(r.monthlySearch / allSpend)} tip="Web-search fees ÷ all-in monthly spend." />
        <Kpi label="12-month total" value={money(projSum.total)} sub={`${money(projSum.anthropic)} Anthropic + ${money(projSum.fixed)} fixed`} tip="All-in cost over the next 12 months at the scenario volumes and monthly growth." />
        {dbShown ? (
          <Kpi label={monthRow ? `Actual spend · ${month}` : "Actual spend · all months"} value={money(dbShown.total)} sub={`${money(dbShown.recorded)} recorded + ${money(dbShown.estimated)} estimated`} tip="From the live database: recorded token usage priced exactly, plus an estimate for requests that ran before usage recording started." />
        ) : (
          <Kpi label={logs.records.length ? "Actual spend (imported logs)" : "Actual spend"} value={logs.records.length ? money(actualSum.cost) : "—"} sub={logs.records.length ? `${int(actualSum.outlines)} outlines, ${int(actualSum.refines)} refines` : "Connect the database or import logs"} tip="Real cost of all imported outline and refine calls, priced from their actual token counts." />
        )}
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
            <div><div className="text-xs text-muted-foreground">Now (f = {pct(r.f, 0)})</div><div className="tabular text-2xl font-semibold text-warning">{money(r.wastePerOutline)}</div><div className="text-xs text-muted-foreground">per outline · {money(r.wastePerOutline * vsc.outlines)}/mo</div></div>
            <div><div className="text-xs text-muted-foreground">Structured OFF → ON</div><div className="tabular text-sm">{money(structuredOff.wastePerOutline)} → <span className="font-semibold text-success">{money(structuredOn.wastePerOutline)}</span></div><div className="text-xs text-muted-foreground">saves {money((structuredOff.perOutline - structuredOn.perOutline) * vsc.outlines + (structuredOff.perRefine - structuredOn.perRefine) * vsc.refines)}/mo</div></div>
          </div>
        </Panel>
        <Panel title="Break-even cache hit rate" tip="h* = (writeMult − 1) / (writeMult − readMult). Above it, caching saves money.">
          <Gauge value={sc.hitRate} be={be} />
        </Panel>
      </div>

      <Panel
        title="Monthly analysis — next 12 months"
        tip="Projection from the scenario volumes and per-outline / per-refine costs, with the monthly growth rate below."
        actions={<div className="flex items-center gap-2 text-sm">Growth / month <NumInput value={sc.monthlyGrowth} className="w-20" onChange={(v) => u({ monthlyGrowth: Math.max(-50, Math.min(200, v ?? 0)) })} /> %</div>}
      >
        <div className="h-64">
          <ResponsiveContainer><BarChart data={proj}><CartesianGrid stroke="var(--border)" vertical={false} /><XAxis dataKey="month" {...axisProps} /><YAxis {...axisProps} tickFormatter={(v) => money(v)} /><Tooltip {...tooltipStyle} formatter={(v: number) => money(v)} /><Legend wrapperStyle={{ fontSize: 11 }} /><Bar dataKey="anthropic" name="Anthropic" stackId="m" fill={C[0]} /><Bar dataKey="fixed" name="Fixed costs" stackId="m" fill={C[6]} radius={[4, 4, 0, 0]} /></BarChart></ResponsiveContainer>
        </div>
        <div className="mt-4 overflow-x-auto rounded-lg border"><table className="w-full text-sm tabular"><thead className="border-b bg-muted/40 text-xs text-muted-foreground"><tr>{["Month", "Outlines", "Refines", "Anthropic $", "Fixed $", "Total $", "Cumulative $"].map((h) => <th key={h} className="whitespace-nowrap px-3 py-2 text-left font-medium">{h}</th>)}</tr></thead>
          <tbody>{proj.map((x) => <tr key={x.month} className="border-b last:border-0"><td className="whitespace-nowrap px-3 py-2 font-medium">{x.month}</td><td className="px-3">{int(Math.round(x.outlines))}</td><td className="px-3">{int(Math.round(x.refines))}</td><td className="px-3">{money(x.anthropic)}</td><td className="px-3">{money(x.fixed)}</td><td className="px-3 font-semibold">{money(x.total)}</td><td className="px-3 text-muted-foreground">{money(x.cumulative)}</td></tr>)}
            <tr className="bg-muted/40 font-semibold"><td className="px-3 py-2">12-month total</td><td className="px-3">{int(Math.round(projSum.outlines))}</td><td className="px-3">{int(Math.round(projSum.refines))}</td><td className="px-3">{money(projSum.anthropic)}</td><td className="px-3">{money(projSum.fixed)}</td><td className="px-3">{money(projSum.total)}</td><td className="px-3" /></tr></tbody></table></div>
      </Panel>

      {summary && (
        <Panel
          title="Monthly spend from the database"
          tip="Requests are counted from the live database. Recorded = priced from the real tokens Anthropic billed. Estimated = average unit cost for requests that ran before usage recording."
          bodyClass="p-0 sm:p-0"
        >
          <div className="overflow-x-auto">
            <table className="tabular w-full text-sm">
              <thead className="border-b bg-muted/40 text-xs text-muted-foreground">
                <tr>{["Month", "Requests", "Completed", "Failed", "Refinements", "Zoho", "Bitrix", "Recorded $", "Estimated $", "Total $"].map((h) => <th key={h} className="whitespace-nowrap px-3 py-2 text-left font-medium">{h}</th>)}</tr>
              </thead>
              <tbody>
                {[...dbRows].reverse().map((x) => (
                  <tr key={x.month} className={`border-b ${x.month === month ? "bg-accent/60 font-medium" : ""}`}>
                    <td className="whitespace-nowrap px-3 py-2 font-medium">{x.month}</td><td className="px-3">{int(x.outlines)}</td><td className="px-3">{int(x.completed)}</td><td className="px-3">{int(x.failed)}</td><td className="px-3">{int(x.refines)}</td><td className="px-3">{int(x.zoho.outlines)}</td><td className="px-3">{int(x.bitrix.outlines)}</td><td className="px-3">{money(x.recorded)}</td><td className="px-3">{money(x.estimated)}</td><td className="px-3 font-semibold">{money(x.total)}</td>
                  </tr>
                ))}
                <tr className="bg-muted/40 font-semibold">
                  <td className="px-3 py-2">Total</td><td className="px-3">{int(dbTotal.outlines)}</td><td className="px-3">{int(dbTotal.completed)}</td><td className="px-3">{int(dbTotal.failed)}</td><td className="px-3">{int(dbTotal.refines)}</td><td className="px-3">{int(dbTotal.zoho.outlines)}</td><td className="px-3">{int(dbTotal.bitrix.outlines)}</td><td className="px-3">{money(dbTotal.recorded)}</td><td className="px-3">{money(dbTotal.estimated)}</td><td className="px-3">{money(dbTotal.total)}</td>
                </tr>
              </tbody>
            </table>
          </div>
          <p className="px-4 py-3 text-xs text-muted-foreground">Requests = every outline request (slides excluded); Zoho and Bitrix split all requests. Only completed outlines and refinements are priced; failed requests are not. Pick a month at the top to see that month's cost analysis.</p>
        </Panel>
      )}

      <Panel title="Actual monthly spend (imported logs)" tip="Real spend from imported usage logs, priced from actual token counts per call. Outlines = learning_objectives calls; refines = refine_outline calls (retries included)." bodyClass="p-0 sm:p-0">
        {actual.rows.length === 0 ? (
          <div className="p-5"><EmptyState title="No dated outline or refine usage lines yet">Paste Railway logs on Data & Settings to see real month-by-month spend.</EmptyState></div>
        ) : (
          <>
            <div className="overflow-x-auto"><table className="w-full text-sm tabular"><thead className="border-b bg-muted/40 text-xs text-muted-foreground"><tr>{["Month", "Outlines", "Refines", "Calls", "Input tok", "Output tok", "Cache hit", "Searches", "Spend $", "Without caching $", "Caching saved $", "All-in $ (+ fixed)"].map((h) => <th key={h} className="whitespace-nowrap px-3 py-2 text-left font-medium">{h}</th>)}</tr></thead>
              <tbody>{actual.rows.map((x) => <tr key={x.month} className="border-b last:border-0"><td className="whitespace-nowrap px-3 py-2 font-medium">{x.month}</td><td className="px-3">{int(x.outlines)}</td><td className="px-3">{int(x.refines)}</td><td className="px-3">{int(x.calls)}</td><td className="px-3">{int(x.input + x.cacheWrite + x.cacheRead)}</td><td className="px-3">{int(x.output)}</td><td className="px-3">{pct(hitRate(x) ?? 0, 0)}</td><td className="px-3">{int(x.searches)}</td><td className="px-3 font-semibold">{money(x.cost)}</td><td className="px-3">{money(x.noCache)}</td><td className={`px-3 ${x.noCache - x.cost >= 0 ? "text-success" : "text-destructive"}`}>{money(x.noCache - x.cost, true)}</td><td className="px-3">{money(x.cost + r.fixedTotal)}</td></tr>)}
                <tr className="bg-muted/40 font-semibold"><td className="px-3 py-2">Total</td><td className="px-3">{int(actualSum.outlines)}</td><td className="px-3">{int(actualSum.refines)}</td><td className="px-3">{int(actualSum.calls)}</td><td className="px-3">{int(actualSum.input + actualSum.cw + actualSum.cr)}</td><td className="px-3">{int(actualSum.output)}</td><td className="px-3">{pct(hitRate({ cacheWrite: actualSum.cw, cacheRead: actualSum.cr }) ?? 0, 0)}</td><td className="px-3">{int(actualSum.searches)}</td><td className="px-3">{money(actualSum.cost)}</td><td className="px-3">{money(actualSum.noCache)}</td><td className="px-3">{money(actualSum.noCache - actualSum.cost, true)}</td><td className="px-3">{money(actualSum.cost + r.fixedTotal * actual.rows.length)}</td></tr></tbody></table></div>
            <p className="px-4 py-3 text-xs text-muted-foreground">
              {logs.ignored ? `${logs.ignored} usage line(s) from other features (slides, assessments) ignored. ` : ""}
              {actual.undated ? `${actual.undated} line(s) without a date are not in this table. ` : ""}
              Each call is priced with its own model's price row; fixed costs are added once per month shown.
            </p>
          </>
        )}
      </Panel>

      <Panel title="Insights">
        <ul className="list-disc space-y-1.5 pl-5 text-sm">{insights.map((i) => <li key={i}>{i}</li>)}</ul>
      </Panel>

      <Panel title="Step-by-step cost (per job)" bodyClass="p-0 sm:p-0">
        <div className="overflow-x-auto"><table className="w-full text-sm tabular"><thead className="border-b bg-muted/40 text-xs text-muted-foreground"><tr>{["Step", "×", "Cached", "Input", "Output", "Searches", "Uncached $", "Write $", "Read $", "Output $", "Search $", "Expected $", "% of job"].map((h) => <th key={h} className="whitespace-nowrap px-3 py-2 text-left font-medium">{h}</th>)}</tr></thead>
          <tbody>{[...r.outline.rows, ...r.refine.rows].map((x) => {
            const jt = x.step.job === "outline" ? r.perOutline : r.perRefine;
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
        <p>Monthly = outlines·perOutline + refines·perRefine; all-in adds fixed costs. Break-even <code>h* = (write − 1)/(write − read)</code>. Imported-logs mode replaces token estimates with per-label averages (cached = cache_write + cache_read).</p>
        <p>12-month projection: month k uses volume × (1 + growth)^k. Actual monthly spend prices every imported call from its real tokens (<code>input·in + cache_write·in·write + cache_read·in·read + output·out + searches·fee</code>); outlines = learning_objectives calls, refines = refine_outline calls (retries included). Only outline and refine calls are counted.</p>
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

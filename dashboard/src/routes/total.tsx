import { createFileRoute } from "@tanstack/react-router";
import { useMemo } from "react";
import { Bar, BarChart, CartesianGrid, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { PageHeader } from "@/components/app/AppShell";
import { EmptyState, HowCalculated, Kpi, Panel, axisProps, tooltipStyle } from "@/components/app/bits";
import { RequireData } from "@/components/app/connect";
import { Button } from "@/components/ui/button";
import { compute } from "@/lib/cost/calc";
import { useData } from "@/lib/cost/data";
import { monthlyCosts, sumMonths } from "@/lib/cost/dbcost";
import { int, money, pct } from "@/lib/cost/format";
import { useStore } from "@/lib/cost/store";

export const Route = createFileRoute("/total")({
  head: () => ({
    meta: [
      { title: "Total — Outline AI Cost Console" },
      { name: "description", content: "All-time totals: requests, refinements and cost across every month." },
    ],
  }),
  component: TotalPage,
});

function TotalPage() {
  return (
    <>
      <PageHeader title="Total" desc="Everything so far, across all months and both sources." />
      <RequireData>
        <TotalBody />
      </RequireData>
    </>
  );
}

function TotalBody() {
  const { summary } = useData();
  const { state, current, priced } = useStore();
  const est = useMemo(() => (current ? compute(state, current) : null), [state, current]);
  const rows = useMemo(
    () => (summary && current && est ? monthlyCosts(summary, priced, current, state.scenario.window, est.perOutline, est.perRefine) : []),
    [summary, current, est, priced, state.scenario.window],
  );
  if (!summary || !current || !est) return <EmptyState title="The production model has no price yet">Fill in Claude Sonnet 4.6 on Data & Settings.</EmptyState>;
  const t = sumMonths(rows);
  const fixed = est.fixedTotal * rows.length;
  const recordedShare = t.total ? t.recorded / t.total : 0;
  const chart = rows.map((r) => ({ month: r.month, Zoho: r.zoho.outlines, Bitrix: r.bitrix.outlines, Recorded: r.recorded, Estimated: r.estimated }));

  const csv = () => {
    const head = ["Month", "Requests", "Completed", "Failed", "In progress", "Zoho", "Bitrix", "Refinements", "Recorded $", "Estimated $", "Total $"];
    const lines = [...rows, t].map((r) => [r.month, r.outlines, r.completed, r.failed, r.other, r.zoho.outlines, r.bitrix.outlines, r.refines, r.recorded.toFixed(4), r.estimated.toFixed(4), r.total.toFixed(4)]);
    const blob = new Blob([[head, ...lines].map((l) => l.map((c) => `"${String(c)}"`).join(",")).join("\n")], { type: "text/csv" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "outline-ai-totals.csv";
    a.click();
  };

  return (
    <>
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <Kpi label="Total requests" value={int(t.outlines)} sub={`${int(t.completed)} completed · ${int(t.failed)} failed${t.other ? ` · ${int(t.other)} in progress` : ""}`} tip="Every outline request in the database (slides jobs excluded)." />
        <Kpi label="Refinements" value={int(t.refines)} sub={`${pct(t.completed ? t.refines / t.completed : 0, 0)} of completed outlines`} tip="Every later version of an outline (version 2 and up)." />
        <Kpi label="Total cost" value={money(t.total)} sub={`${money(t.recorded)} recorded + ${money(t.estimated)} estimated`} tip="Recorded token usage priced exactly, plus an estimate (average unit cost) for requests that ran before usage recording." />
        <Kpi label="Average per completed outline" value={money(t.completed ? t.total / t.completed : 0)} sub={`incl. refinements`} tip="Total cost ÷ completed outlines." />
        <Kpi label="Zoho" value={int(t.zoho.outlines)} sub={`${int(t.zoho.refines)} refinements`} tip="Requests that came from Zoho CRM." />
        <Kpi label="Bitrix" value={int(t.bitrix.outlines)} sub={`${int(t.bitrix.refines)} refinements`} tip="Requests that came from Bitrix tasks." />
        <Kpi label="Recorded share of cost" value={pct(recordedShare, 0)} tone={recordedShare > 0.5 ? "success" : "warning"} sub={`${int(t.recordedOutlines)} outlines with recorded usage`} tip="How much of the total is priced from real token usage rather than estimated." />
        <Kpi label="Fixed costs so far" value={money(fixed)} sub={`${money(est.fixedTotal)} × ${rows.length} months`} tip="Monthly fixed costs from the assumptions, once per month with requests." />
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <Panel title="Requests per month">
          <div className="h-72"><ResponsiveContainer><BarChart data={chart}><CartesianGrid stroke="var(--border)" vertical={false} /><XAxis dataKey="month" {...axisProps} /><YAxis {...axisProps} allowDecimals={false} /><Tooltip {...tooltipStyle} /><Legend wrapperStyle={{ fontSize: 11 }} /><Bar dataKey="Zoho" stackId="r" fill="var(--chart-1)" /><Bar dataKey="Bitrix" stackId="r" fill="var(--chart-4)" radius={[4, 4, 0, 0]} /></BarChart></ResponsiveContainer></div>
        </Panel>
        <Panel title="Cost per month" tip="Recorded = priced from real tokens. Estimated = average unit cost for requests without recorded usage.">
          <div className="h-72"><ResponsiveContainer><BarChart data={chart}><CartesianGrid stroke="var(--border)" vertical={false} /><XAxis dataKey="month" {...axisProps} /><YAxis {...axisProps} tickFormatter={(v) => money(v)} /><Tooltip {...tooltipStyle} formatter={(v: number) => money(v)} /><Legend wrapperStyle={{ fontSize: 11 }} /><Bar dataKey="Recorded" stackId="c" fill="var(--chart-3)" /><Bar dataKey="Estimated" stackId="c" fill="var(--chart-2)" radius={[4, 4, 0, 0]} /></BarChart></ResponsiveContainer></div>
        </Panel>
      </div>

      <Panel title="Month by month" actions={<Button size="sm" variant="outline" onClick={csv}>Export CSV</Button>} bodyClass="p-0 sm:p-0">
        <div className="overflow-x-auto">
          <table className="tabular w-full text-sm">
            <thead className="border-b bg-muted/40 text-xs text-muted-foreground">
              <tr>{["Month", "Requests", "Completed", "Failed", "Zoho", "Bitrix", "Refinements", "Recorded $", "Estimated $", "Total $"].map((h) => <th key={h} className="whitespace-nowrap px-3 py-2 text-left font-medium">{h}</th>)}</tr>
            </thead>
            <tbody>
              {[...rows].reverse().map((r) => (
                <tr key={r.month} className="border-b">
                  <td className="whitespace-nowrap px-3 py-2 font-medium">{r.month}</td><td className="px-3">{int(r.outlines)}</td><td className="px-3">{int(r.completed)}</td><td className="px-3">{int(r.failed)}</td><td className="px-3">{int(r.zoho.outlines)}</td><td className="px-3">{int(r.bitrix.outlines)}</td><td className="px-3">{int(r.refines)}</td><td className="px-3">{money(r.recorded)}</td><td className="px-3">{money(r.estimated)}</td><td className="px-3 font-semibold">{money(r.total)}</td>
                </tr>
              ))}
              <tr className="bg-muted/40 font-semibold">
                <td className="px-3 py-2">Total</td><td className="px-3">{int(t.outlines)}</td><td className="px-3">{int(t.completed)}</td><td className="px-3">{int(t.failed)}</td><td className="px-3">{int(t.zoho.outlines)}</td><td className="px-3">{int(t.bitrix.outlines)}</td><td className="px-3">{int(t.refines)}</td><td className="px-3">{money(t.recorded)}</td><td className="px-3">{money(t.estimated)}</td><td className="px-3">{money(t.total)}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </Panel>

      <HowCalculated>
        <p>Requests and refinements are counted from the database (<code>course_jobs</code> excluding slides, and <code>course_versions</code> with version ≥ 2). Month = the month the request or refinement was created (UTC).</p>
        <p>Cost per month = recorded token usage priced with the production model's prices + an estimate for what has no recorded usage: <code>(completed outlines − outlines with usage) × cost per outline + (refinements − refinements with usage) × cost per refine</code>. The estimate uses the call profile on Data & Settings, so it is only as accurate as those assumptions. Failed requests are not estimated.</p>
      </HowCalculated>
    </>
  );
}

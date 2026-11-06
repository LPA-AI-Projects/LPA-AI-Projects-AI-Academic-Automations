import { createFileRoute } from "@tanstack/react-router";
import { useRef } from "react";
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { toast } from "sonner";
import { TriangleAlert } from "lucide-react";
import { PageHeader } from "@/components/app/AppShell";
import { CallProfileEditor, FixedCostsEditor, PriceSheetEditor, ScenarioEditor } from "@/components/app/editors";
import { EmptyState, HowCalculated, Kpi, Panel, axisProps, tooltipStyle } from "@/components/app/bits";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { useStore, mergeState } from "@/lib/cost/store";
import { aggregate, hitRate, perDay, sampleLogs, type Agg } from "@/lib/cost/logs";
import { int, pct } from "@/lib/cost/format";

export const Route = createFileRoute("/settings")({
  head: () => ({
    meta: [
      { title: "Data & Settings — Outline AI Cost Console" },
      { name: "description", content: "Import Anthropic usage logs and edit prices, call profile, scenario and fixed costs." },
      { property: "og:title", content: "Data & Settings — Outline AI Cost Console" },
      { property: "og:description", content: "Import Anthropic usage logs and edit prices, call profile, scenario and fixed costs." },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary" },
    ],
  }),
  component: Settings,
});

function AggTable({ rows, keyLabel }: { rows: Agg[]; keyLabel: string }) {
  return (
    <div className="overflow-x-auto"><table className="w-full text-sm tabular"><thead className="border-b bg-muted/40 text-xs text-muted-foreground"><tr>{[keyLabel, "Calls", "Input", "Output", "Cache write", "Cache read", "Searches", "Avg in/call", "Avg out/call", "Hit rate", "max_tokens"].map((h) => <th key={h} className="whitespace-nowrap px-3 py-2 text-left font-medium">{h}</th>)}</tr></thead>
      <tbody>{rows.map((a) => <tr key={a.key} className="border-b last:border-0"><td className="whitespace-nowrap px-3 py-2 font-medium">{a.key}</td><td className="px-3">{a.calls}</td><td className="px-3">{int(a.input)}</td><td className="px-3">{int(a.output)}</td><td className="px-3">{int(a.cacheWrite)}</td><td className="px-3">{int(a.cacheRead)}</td><td className="px-3">{a.searches}</td><td className="px-3">{int((a.input + a.cacheWrite + a.cacheRead) / a.calls)}</td><td className="px-3">{int(a.output / a.calls)}</td><td className="px-3">{pct(hitRate(a))}</td><td className={`px-3 ${a.maxTokens ? "font-semibold text-warning" : ""}`}>{a.maxTokens}</td></tr>)}</tbody></table></div>
  );
}

function Settings() {
  const { state, setState, logs, byLabel, replace, reset } = useStore();
  const fileRef = useRef<HTMLInputElement>(null);
  const jsonRef = useRef<HTMLInputElement>(null);
  const recs = logs.records;
  const byModel = aggregate(recs, "model");
  const tot = byLabel.reduce((a, x) => ({ calls: a.calls + x.calls, cw: a.cw + x.cacheWrite, cr: a.cr + x.cacheRead, mt: a.mt + x.maxTokens, out: a.out + x.output }), { calls: 0, cw: 0, cr: 0, mt: 0, out: 0 });
  const days = perDay(recs);
  const setLog = (t: string) => setState((s) => ({ ...s, logText: t }));

  return (
    <>
      <PageHeader title="Data & Settings" desc="Import usage logs and edit every assumption in one place." />

      <Panel title="Log import" tip="Parses lines containing 'Anthropic usage | label=… model=… input=… output=… cache_write=… cache_read=… web_searches=… stop=… structured=…'. Text before it is ignored; None counts as 0."
        actions={<>
          <Button size="sm" variant="outline" onClick={() => fileRef.current?.click()}>Upload file</Button>
          <Button size="sm" variant="outline" onClick={() => { setLog(sampleLogs()); toast.success("Sample data loaded"); }}>Load sample data</Button>
          <Button size="sm" variant="ghost" onClick={() => setLog("")}>Clear</Button>
          <input ref={fileRef} type="file" accept=".log,.txt,text/plain" className="hidden" onChange={async (e) => { const f = e.target.files?.[0]; if (f) setLog(await f.text()); e.target.value = ""; }} />
        </>}>
        <Textarea value={state.logText} onChange={(e) => setLog(e.target.value)} placeholder="Paste log lines here…" className="h-40 font-mono text-xs" />
        <p className="mt-2 text-xs text-muted-foreground">{recs.length} outline / refine usage lines parsed{logs.ignored ? `, ${logs.ignored} from other features (slides, assessments) ignored` : ""}{logs.skipped ? `, ${logs.skipped} non-usage lines skipped` : ""}.</p>
      </Panel>

      {recs.length === 0 ? <EmptyState title="No log data yet">Paste logs, upload a file, or load the sample.</EmptyState> : (
        <>
          <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
            <Kpi label="Calls" value={int(tot.calls)} tip="Number of parsed usage lines." />
            <Kpi label="Output tokens" value={int(tot.out)} tip="Sum of output tokens across all calls." />
            <Kpi label="Cache hit rate" value={pct(hitRate({ cacheWrite: tot.cw, cacheRead: tot.cr }))} tip="cache_read ÷ (cache_read + cache_write)." />
            <Kpi label="Stopped at max_tokens" value={tot.mt} tone={tot.mt ? "warning" : "default"} sub={tot.mt ? "Truncated outputs — consider raising max_tokens" : undefined} tip="Calls with stop=max_tokens; these often cause invalid JSON and retries." />
          </div>
          {tot.mt > 0 && <div className="flex items-center gap-2 rounded-lg border border-warning/40 bg-warning/10 px-4 py-2 text-sm"><TriangleAlert className="h-4 w-4 text-warning" /> {tot.mt} call(s) hit max_tokens.</div>}
          <Panel title="Per label" bodyClass="p-0 sm:p-0"><AggTable rows={byLabel} keyLabel="Label" /></Panel>
          <div className="grid gap-4 xl:grid-cols-2">
            <Panel title="Per model" bodyClass="p-0 sm:p-0"><AggTable rows={byModel} keyLabel="Model" /></Panel>
            <Panel title="Calls per day">
              {days.length ? <div className="h-56"><ResponsiveContainer><BarChart data={days}><CartesianGrid stroke="var(--border)" vertical={false} /><XAxis dataKey="date" {...axisProps} /><YAxis {...axisProps} allowDecimals={false} /><Tooltip {...tooltipStyle} /><Bar dataKey="calls" fill="var(--chart-1)" radius={4} /></BarChart></ResponsiveContainer></div> : <EmptyState title="No dates found in log lines" />}
            </Panel>
          </div>
        </>
      )}

      <ScenarioEditor />
      <PriceSheetEditor />
      <CallProfileEditor />
      <div className="grid gap-4 lg:grid-cols-2">
        <FixedCostsEditor />
        <Panel title="Import / export settings">
          <div className="flex flex-wrap gap-2">
            <Button variant="outline" onClick={() => { const a = document.createElement("a"); a.href = URL.createObjectURL(new Blob([JSON.stringify(state, null, 2)], { type: "application/json" })); a.download = "outline-ai-cost-settings.json"; a.click(); }}>Export JSON</Button>
            <Button variant="outline" onClick={() => jsonRef.current?.click()}>Import JSON</Button>
            <Button variant="destructive" onClick={() => { if (confirm("Reset all inputs to defaults?")) { reset(); toast.success("Reset to defaults"); } }}>Reset to defaults</Button>
            <input ref={jsonRef} type="file" accept="application/json,.json" className="hidden" onChange={async (e) => { const f = e.target.files?.[0]; e.target.value = ""; if (!f) return; try { replace(mergeState(JSON.parse(await f.text()))); toast.success("Settings imported"); } catch { toast.error("That file isn't valid settings JSON"); } }} />
          </div>
          <p className="mt-3 text-xs text-muted-foreground">All inputs are saved in this browser automatically.</p>
        </Panel>
      </div>

      <HowCalculated>
        <p>Per label and model: totals, average tokens per call (input + cache_write + cache_read), and cache hit rate <code>cache_read / (cache_read + cache_write)</code>.</p>
        <p>With data source = Imported logs, each call-profile step uses its label's averages: cacheable = avg(cache_write + cache_read), other input = avg(input), output = avg(output), searches = avg(web_searches).</p>
      </HowCalculated>
    </>
  );
}

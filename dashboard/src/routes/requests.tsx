import { createFileRoute } from "@tanstack/react-router";
import { useEffect, useMemo, useState } from "react";
import { ChevronLeft, ChevronRight, Loader2, Search } from "lucide-react";
import { PageHeader } from "@/components/app/AppShell";
import { EmptyState, Panel } from "@/components/app/bits";
import { RequireData } from "@/components/app/connect";
import { RequestDetailSheet, type CostContext } from "@/components/app/request-detail";
import { SourceBadge, StatusBadge } from "@/components/app/request-badges";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { ApiError, apiGet, fmtDate, type RequestList } from "@/lib/cost/api";
import { compute } from "@/lib/cost/calc";
import { useData } from "@/lib/cost/data";
import { requestCost } from "@/lib/cost/dbcost";
import { int, money } from "@/lib/cost/format";
import { useStore } from "@/lib/cost/store";

export const Route = createFileRoute("/requests")({
  head: () => ({
    meta: [
      { title: "Requests — Outline AI Cost Console" },
      { name: "description", content: "Every outline request: source, inputs, refinements, responsible person and cost." },
    ],
  }),
  component: RequestsPage,
});

const PAGE = 50;

function useDebounced<T>(value: T, ms: number): T {
  const [v, setV] = useState(value);
  useEffect(() => {
    const t = setTimeout(() => setV(value), ms);
    return () => clearTimeout(t);
  }, [value, ms]);
  return v;
}

function RequestsPage() {
  return (
    <>
      <PageHeader title="Requests" desc="Every outline request with its inputs, source, refinements and cost. Click a row for the full picture." />
      <RequireData>
        <RequestsTable />
      </RequireData>
    </>
  );
}

function RequestsTable() {
  const { apiKey, month } = useData();
  const { state, current } = useStore();
  const [source, setSource] = useState("all");
  const [status, setStatus] = useState("all");
  const [refined, setRefined] = useState("all");
  const [q, setQ] = useState("");
  const [offset, setOffset] = useState(0);
  const [data, setData] = useState<RequestList | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [openId, setOpenId] = useState<string | null>(null);
  const dq = useDebounced(q, 350);

  useEffect(() => setOffset(0), [month, source, status, refined, dq]);

  useEffect(() => {
    let stale = false;
    setLoading(true);
    setError(null);
    apiGet<RequestList>("/requests", apiKey, { month, source, status, refined, q: dq, limit: PAGE, offset })
      .then((d) => !stale && setData(d))
      .catch((e) => !stale && setError(e instanceof ApiError || e instanceof Error ? e.message : "Could not load requests."))
      .finally(() => !stale && setLoading(false));
    return () => {
      stale = true;
    };
  }, [apiKey, month, source, status, refined, dq, offset]);

  const est = useMemo(() => (current ? compute(state, current) : null), [state, current]);
  const cost: CostContext | null = current && est ? { price: current, window: state.scenario.window, perOutline: est.perOutline, perRefine: est.perRefine } : null;
  if (!cost) return <EmptyState title="The production model has no price yet">Open Data & Settings → price sheet and fill in Claude Sonnet 4.6.</EmptyState>;

  const total = data?.total ?? 0;
  const from = total ? offset + 1 : 0;
  const to = Math.min(total, offset + PAGE);
  const reset = () => {
    setSource("all");
    setStatus("all");
    setRefined("all");
    setQ("");
  };

  return (
    <>
      <Panel title="Filters" tip="The month comes from the filter at the top of the page.">
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
          <div className="relative lg:col-span-2">
            <Search className="pointer-events-none absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
            <Input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Search course, company or record id…" aria-label="Search" className="pl-8" />
          </div>
          <Select value={source} onValueChange={setSource}>
            <SelectTrigger aria-label="Source"><SelectValue /></SelectTrigger>
            <SelectContent><SelectItem value="all">All sources</SelectItem><SelectItem value="zoho">Zoho</SelectItem><SelectItem value="bitrix">Bitrix</SelectItem></SelectContent>
          </Select>
          <Select value={status} onValueChange={setStatus}>
            <SelectTrigger aria-label="Status"><SelectValue /></SelectTrigger>
            <SelectContent><SelectItem value="all">Any status</SelectItem><SelectItem value="completed">Completed</SelectItem><SelectItem value="failed">Failed</SelectItem><SelectItem value="in_progress">In progress</SelectItem></SelectContent>
          </Select>
          <Select value={refined} onValueChange={setRefined}>
            <SelectTrigger aria-label="Refinements"><SelectValue /></SelectTrigger>
            <SelectContent><SelectItem value="all">With or without refinements</SelectItem><SelectItem value="yes">Has refinements</SelectItem><SelectItem value="no">No refinements</SelectItem></SelectContent>
          </Select>
        </div>
      </Panel>

      <Panel
        title={month === "all" ? "All requests" : `Requests in ${month}`}
        actions={
          <>
            {loading && <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />}
            <span className="text-xs text-muted-foreground tabular">{from.toLocaleString("en-US")}–{to.toLocaleString("en-US")} of {total.toLocaleString("en-US")}</span>
            <Button size="icon" variant="outline" className="h-8 w-8" aria-label="Previous page" disabled={offset === 0 || loading} onClick={() => setOffset(Math.max(0, offset - PAGE))}><ChevronLeft className="h-4 w-4" /></Button>
            <Button size="icon" variant="outline" className="h-8 w-8" aria-label="Next page" disabled={offset + PAGE >= total || loading} onClick={() => setOffset(offset + PAGE)}><ChevronRight className="h-4 w-4" /></Button>
          </>
        }
        bodyClass="p-0 sm:p-0"
      >
        {error ? (
          <div className="p-5 text-sm text-destructive">{error}</div>
        ) : data && data.items.length === 0 ? (
          <div className="p-5">
            <EmptyState title="No requests match these filters"><Button variant="link" size="sm" onClick={reset}>Clear filters</Button></EmptyState>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="border-b bg-muted/40 text-xs text-muted-foreground">
                <tr>{["Created", "Source", "Record / task", "Course", "Company", "Status", "Refinements", "Cost"].map((h) => <th key={h} className="whitespace-nowrap px-3 py-2 text-left font-medium">{h}</th>)}</tr>
              </thead>
              <tbody>
                {(data?.items ?? []).map((r) => {
                  const c = requestCost(r, cost.price, cost.window, cost.perOutline, cost.perRefine);
                  return (
                    <tr
                      key={r.id}
                      tabIndex={0}
                      onClick={() => setOpenId(r.id)}
                      onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && (e.preventDefault(), setOpenId(r.id))}
                      className="cursor-pointer border-b outline-none last:border-0 hover:bg-muted/50 focus-visible:bg-muted/50"
                    >
                      <td className="whitespace-nowrap px-3 py-2 text-xs text-muted-foreground">{fmtDate(r.created_at)}</td>
                      <td className="px-3"><SourceBadge source={r.source} /></td>
                      <td className="tabular whitespace-nowrap px-3 font-mono text-xs">{r.record_id}</td>
                      <td className="max-w-[280px] truncate px-3 font-medium" title={r.course_name}>{r.course_name || "—"}</td>
                      <td className="max-w-[200px] truncate px-3 text-muted-foreground" title={r.company_name}>{r.company_name || "—"}</td>
                      <td className="px-3"><StatusBadge status={r.status} /></td>
                      <td className="px-3">{r.refinements ? <Badge variant="warning">{int(r.refinements)}</Badge> : <span className="text-muted-foreground">—</span>}</td>
                      <td className="tabular whitespace-nowrap px-3" title={c.exact ? "From recorded token usage" : "Estimated at the average unit cost (no recorded usage)"}>
                        {c.total > 0 ? <>{c.exact ? "" : "~"}{money(c.total)}</> : <span className="text-muted-foreground">—</span>}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
        <p className="border-t px-4 py-3 text-xs text-muted-foreground">
          Costs shown with ~ are estimates at the average unit cost; exact figures appear once a request has recorded token usage. Times are shown in your local timezone.
        </p>
      </Panel>

      <RequestDetailSheet id={openId} onClose={() => setOpenId(null)} cost={cost} />
    </>
  );
}

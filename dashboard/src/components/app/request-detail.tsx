import { useEffect, useState, type ReactNode } from "react";
import { ExternalLink, Loader2, User } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { ApiError, apiGet, fmtDate, type RequestDetail } from "@/lib/cost/api";
import { useData } from "@/lib/cost/data";
import { priceForModel, usageCost } from "@/lib/cost/dbcost";
import { int, money } from "@/lib/cost/format";
import type { PriceRow } from "@/lib/cost/model";
import { SourceBadge, StatusBadge } from "./request-badges";

export interface CostContext {
  price: PriceRow;
  prices: PriceRow[];
  window: "5m" | "1h";
  perOutline: number;
  perRefine: number;
}

const label = (k: string) => k.replace(/_/g, " ").replace(/^./, (c) => c.toUpperCase());

function Value({ v }: { v: unknown }): ReactNode {
  if (Array.isArray(v)) return v.map((x) => (typeof x === "string" ? x : JSON.stringify(x))).join(", ");
  if (v !== null && typeof v === "object") return <code className="text-xs">{JSON.stringify(v)}</code>;
  return String(v);
}

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="space-y-2">
      <h4 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">{title}</h4>
      {children}
    </section>
  );
}

/** Slide-over with everything about one request: inputs, source, refinements, responsible person and cost. */
export function RequestDetailSheet({ id, onClose, cost }: { id: string | null; onClose: () => void; cost: CostContext }) {
  const { apiKey } = useData();
  const [detail, setDetail] = useState<RequestDetail | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!id) return;
    let stale = false;
    setDetail(null);
    setError(null);
    apiGet<RequestDetail>(`/requests/${id}`, apiKey)
      .then((d) => !stale && setDetail(d))
      .catch((e) => !stale && setError(e instanceof ApiError || e instanceof Error ? e.message : "Could not load this request."));
    return () => {
      stale = true;
    };
  }, [id, apiKey]);

  const priceOf = (model: string) => priceForModel(model, cost.prices, cost.price);
  const callsCost = detail?.usage_calls.reduce((a, c) => a + usageCost(c, priceOf(c.model), cost.window), 0) ?? 0;
  const refinements = detail?.versions.filter((v) => v.is_refinement) ?? [];

  return (
    <Sheet open={!!id} onOpenChange={(o) => !o && onClose()}>
      <SheetContent side="right" className="w-full overflow-y-auto sm:max-w-2xl">
        <SheetHeader>
          <SheetTitle>{detail ? String(detail.inputs["course_name"] ?? "Request") : "Request"}</SheetTitle>
          <SheetDescription>{detail ? String(detail.inputs["company_name"] ?? "") || "No company given" : "Loading…"}</SheetDescription>
        </SheetHeader>

        {!detail && !error && (
          <div className="flex items-center gap-2 py-10 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" /> Loading request…
          </div>
        )}
        {error && <p className="py-6 text-sm text-destructive">{error}</p>}

        {detail && (
          <div className="mt-5 space-y-6 pb-8">
            <div className="flex flex-wrap items-center gap-2">
              <SourceBadge source={detail.source} />
              <StatusBadge status={detail.status} />
              <Badge variant={detail.refinements ? "warning" : "muted"}>{detail.refinements ? `${detail.refinements} refinement${detail.refinements > 1 ? "s" : ""}` : "No refinements"}</Badge>
              <span className="text-xs text-muted-foreground">{fmtDate(detail.created_at)}</span>
            </div>

            <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1.5 text-sm">
              <dt className="text-muted-foreground">{detail.source === "bitrix" ? "Bitrix task" : "Zoho record"}</dt>
              <dd className="tabular break-all font-mono text-xs">{detail.record_id}</dd>
              {detail.responsible && (
                <>
                  <dt className="text-muted-foreground">Responsible</dt>
                  <dd className="flex items-center gap-1.5 font-medium"><User className="h-3.5 w-3.5 text-muted-foreground" />{detail.responsible.name ?? "—"}</dd>
                  {detail.responsible.created_by && (
                    <>
                      <dt className="text-muted-foreground">Created by</dt>
                      <dd>{detail.responsible.created_by}</dd>
                    </>
                  )}
                  <dt className="text-muted-foreground">Looked up from</dt>
                  <dd className="text-xs text-muted-foreground">{detail.responsible.from} (live)</dd>
                </>
              )}
            </dl>

            {detail.error && (
              <Section title="Error">
                <pre className="max-h-40 overflow-auto whitespace-pre-wrap rounded-md border border-destructive/30 bg-destructive/10 p-3 text-xs">{detail.error}</pre>
              </Section>
            )}

            <Section title="Inputs received">
              {Object.keys(detail.inputs).length === 0 ? (
                <p className="text-sm text-muted-foreground">No inputs were stored for this request.</p>
              ) : (
                <dl className="divide-y rounded-lg border">
                  {Object.entries(detail.inputs).map(([k, v]) => (
                    <div key={k} className="grid grid-cols-1 gap-0.5 px-3 py-2 sm:grid-cols-[170px_1fr] sm:gap-3">
                      <dt className="text-xs font-medium text-muted-foreground">{label(k)}</dt>
                      <dd className="whitespace-pre-wrap break-words text-sm"><Value v={v} /></dd>
                    </div>
                  ))}
                </dl>
              )}
            </Section>

            <Section title={`Refinements (${refinements.length})`}>
              {detail.versions.length === 0 ? (
                <p className="text-sm text-muted-foreground">No outline was saved for this request.</p>
              ) : (
                <ol className="space-y-3 border-l pl-4">
                  {detail.versions.map((v) => (
                    <li key={v.version} className="relative">
                      <span className="absolute -left-[21px] top-1 h-2.5 w-2.5 rounded-full border bg-background" style={{ borderColor: v.is_refinement ? "var(--warning)" : "var(--primary)" }} />
                      <div className="flex flex-wrap items-center gap-2 text-sm">
                        <span className="font-medium">{v.is_refinement ? `Refinement → v${v.version}` : `v${v.version} · original outline`}</span>
                        <span className="text-xs text-muted-foreground">{fmtDate(v.created_at)}</span>
                        {v.pdf_url && (
                          <a href={v.pdf_url} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1 text-xs text-primary hover:underline">
                            PDF <ExternalLink className="h-3 w-3" />
                          </a>
                        )}
                      </div>
                      {v.feedback && <p className="mt-1 whitespace-pre-wrap rounded-md bg-muted/60 px-3 py-2 text-sm">{v.feedback}</p>}
                    </li>
                  ))}
                </ol>
              )}
            </Section>

            <Section title="Cost">
              {detail.usage_calls.length > 0 ? (
                <div className="space-y-2">
                  <div className="overflow-x-auto rounded-lg border">
                    <table className="tabular w-full text-xs">
                      <thead className="border-b bg-muted/40 text-muted-foreground">
                        <tr>{["Call", "Input", "Cache w/r", "Output", "Searches", "Cost"].map((h) => <th key={h} className="px-2 py-1.5 text-left font-medium">{h}</th>)}</tr>
                      </thead>
                      <tbody>
                        {detail.usage_calls.map((c, i) => (
                          <tr key={i} className="border-b last:border-0">
                            <td className="whitespace-nowrap px-2 py-1.5">
                              {c.label}
                              <div className="text-[10px] text-muted-foreground">{c.model}</div>
                            </td>
                            <td className="px-2">{int(c.input)}</td>
                            <td className="px-2">{int(c.cache_write)} / {int(c.cache_read)}</td>
                            <td className="px-2">{int(c.output)}</td>
                            <td className="px-2">{c.searches}</td>
                            <td className="px-2 font-medium">{money(usageCost(c, priceOf(c.model), cost.window))}</td>
                          </tr>
                        ))}
                        <tr className="bg-muted/40 font-semibold"><td className="px-2 py-1.5" colSpan={5}>Recorded total</td><td className="px-2">{money(callsCost)}</td></tr>
                      </tbody>
                    </table>
                  </div>
                  <p className="text-xs text-muted-foreground">Each call uses that model's list price. Claude Sonnet 4.6 is $3 input and $15 output per million tokens; cache writes are 1.25× and cache reads 0.1× the input price. Web search is $10 per 1,000 searches.</p>
                </div>
              ) : (
                <p className="rounded-md border border-dashed px-3 py-2 text-sm text-muted-foreground">
                  No token usage was recorded for this request (it ran before usage recording started). Estimated: outline ≈ <b>{money(detail.status === "completed" ? cost.perOutline : 0)}</b>
                  {detail.refinements > 0 && <> + {detail.refinements} refinement{detail.refinements > 1 ? "s" : ""} ≈ <b>{money(detail.refinements * cost.perRefine)}</b></>}.
                </p>
              )}
            </Section>

            {detail.pdf_url && (
              <a href={detail.pdf_url} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1.5 text-sm text-primary hover:underline">
                Open the generated outline PDF <ExternalLink className="h-3.5 w-3.5" />
              </a>
            )}
          </div>
        )}
      </SheetContent>
    </Sheet>
  );
}

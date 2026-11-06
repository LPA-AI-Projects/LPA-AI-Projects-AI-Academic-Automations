import { useState, type FormEvent, type ReactNode } from "react";
import { KeyRound, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useData } from "@/lib/cost/data";
import { fmtDate } from "@/lib/cost/api";
import { Panel } from "./bits";

/** Asks for the backend API key (kept in this browser tab only) and loads the live data. */
export function ConnectCard() {
  const { connect, status, error } = useData();
  const [key, setKey] = useState("");
  const submit = (e: FormEvent) => {
    e.preventDefault();
    void connect(key);
  };
  return (
    <Panel title="Connect to the live database" tip="The dashboard reads requests from your backend (which reads the database). The backend's API key is the access key.">
      <form onSubmit={submit} className="flex max-w-xl flex-col gap-3">
        <p className="text-sm text-muted-foreground">
          Enter the backend access key (<code className="rounded bg-muted px-1 font-mono text-xs">API_SECRET_KEY</code>) to load real requests. It is kept in this
          browser tab only and is cleared when you close the tab.
        </p>
        <div className="flex gap-2">
          <Input type="password" autoComplete="off" value={key} onChange={(e) => setKey(e.target.value)} placeholder="Access key" aria-label="Access key" />
          <Button type="submit" disabled={!key.trim() || status === "loading"}>
            {status === "loading" ? <Loader2 className="mr-1.5 h-4 w-4 animate-spin" /> : <KeyRound className="mr-1.5 h-4 w-4" />} Connect
          </Button>
        </div>
        {error && <p className="text-sm text-destructive">{error}</p>}
      </form>
    </Panel>
  );
}

/** Renders children once connected; otherwise the key form (or a loading state). */
export function RequireData({ children }: { children: ReactNode }) {
  const { connected, status, summary } = useData();
  if (connected && summary) return <>{children}</>;
  if (connected && status === "loading")
    return (
      <div className="flex items-center gap-2 py-10 text-sm text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" /> Loading live data…
      </div>
    );
  return <ConnectCard />;
}

export function DatabasePanel() {
  const { connected, status, error, summary, disconnect, refresh } = useData();
  if (!connected || !summary) return <ConnectCard />;
  const rec = summary.usage_recording;
  const jobs = summary.by_month.reduce((a, m) => a + m.zoho.outlines + m.bitrix.outlines, 0);
  return (
    <Panel
      title="Live database"
      tip="Requests are counted from the course_jobs and course_versions tables. Token usage is recorded per call from the moment usage recording was deployed."
      actions={
        <>
          <Button size="sm" variant="outline" onClick={() => void refresh()} disabled={status === "loading"}>
            {status === "loading" && <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" />} Refresh
          </Button>
          <Button size="sm" variant="ghost" onClick={disconnect}>Disconnect</Button>
        </>
      }
    >
      <div className="space-y-1 text-sm">
        <p>
          <span className="font-medium text-success">Connected.</span> {jobs.toLocaleString("en-US")} outline requests across {summary.months.length} months.
        </p>
        <p className="text-muted-foreground">
          {rec.available ? `Token usage recorded since ${fmtDate(rec.first_recorded)}; older requests are estimated.` : "Token usage recording has not produced data yet, so costs are estimates until new requests run on the updated backend."}
        </p>
        {error && <p className="text-destructive">{error}</p>}
      </div>
    </Panel>
  );
}

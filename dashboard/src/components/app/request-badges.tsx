import { Badge } from "@/components/ui/badge";
import type { Source } from "@/lib/cost/api";

export function SourceBadge({ source }: { source: Source }) {
  return source === "bitrix" ? <Badge variant="warning">Bitrix</Badge> : <Badge variant="secondary">Zoho</Badge>;
}

export function StatusBadge({ status }: { status: string }) {
  if (status === "completed") return <Badge variant="success">Completed</Badge>;
  if (status === "failed") return <Badge variant="danger">Failed</Badge>;
  return <Badge variant="muted">{status || "unknown"}</Badge>;
}

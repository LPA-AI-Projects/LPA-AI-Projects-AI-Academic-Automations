import { useState, type ReactNode } from "react";
import { ChevronDown, Info } from "lucide-react";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";

export function InfoTip({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <button type="button" aria-label="More info" className={cn("inline-flex text-muted-foreground hover:text-foreground", className)}>
          <Info className="h-3.5 w-3.5" />
        </button>
      </TooltipTrigger>
      <TooltipContent className="max-w-xs text-xs leading-relaxed">{children}</TooltipContent>
    </Tooltip>
  );
}

export function Panel({
  title,
  tip,
  actions,
  children,
  className,
  bodyClass,
}: {
  title?: ReactNode;
  tip?: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
  className?: string;
  bodyClass?: string;
}) {
  return (
    <section className={cn("card-surface min-w-0", className)}>
      {(title || actions) && (
        <header className="flex flex-wrap items-center justify-between gap-2 border-b px-4 py-3 sm:px-5">
          <h3 className="flex items-center gap-1.5 text-sm font-semibold">
            {title}
            {tip && <InfoTip>{tip}</InfoTip>}
          </h3>
          {actions && <div className="flex flex-wrap items-center gap-2">{actions}</div>}
        </header>
      )}
      <div className={cn("p-4 sm:p-5", bodyClass)}>{children}</div>
    </section>
  );
}

export function Kpi({
  label,
  value,
  sub,
  tip,
  tone = "default",
}: {
  label: string;
  value: ReactNode;
  sub?: ReactNode;
  tip: ReactNode;
  tone?: "default" | "success" | "danger" | "warning";
}) {
  return (
    <div className="card-surface flex flex-col gap-1 p-4">
      <div className="flex items-center gap-1.5 text-xs font-medium text-muted-foreground">
        {label}
        <InfoTip>{tip}</InfoTip>
      </div>
      <div
        className={cn(
          "tabular text-2xl font-semibold tracking-tight",
          tone === "success" && "text-success",
          tone === "danger" && "text-destructive",
          tone === "warning" && "text-warning",
        )}
      >
        {value}
      </div>
      {sub && <div className="text-xs text-muted-foreground">{sub}</div>}
    </div>
  );
}

export function HowCalculated({ children }: { children: ReactNode }) {
  const [open, setOpen] = useState(false);
  return (
    <Collapsible open={open} onOpenChange={setOpen} className="card-surface">
      <CollapsibleTrigger className="flex w-full items-center justify-between px-5 py-3 text-sm font-semibold">
        How this is calculated
        <ChevronDown className={cn("h-4 w-4 transition-transform", open && "rotate-180")} />
      </CollapsibleTrigger>
      <CollapsibleContent className="space-y-2 border-t px-5 py-4 text-sm leading-relaxed text-muted-foreground [&_code]:rounded [&_code]:bg-muted [&_code]:px-1 [&_code]:font-mono [&_code]:text-xs [&_code]:text-foreground">
        {children}
      </CollapsibleContent>
    </Collapsible>
  );
}

export function NumInput({
  value,
  onChange,
  nullable,
  step,
  className,
  placeholder,
  ...rest
}: {
  value: number | null;
  onChange: (v: number | null) => void;
  nullable?: boolean;
  step?: number;
  className?: string;
  placeholder?: string;
  "aria-label"?: string;
}) {
  return (
    <Input
      type="number"
      inputMode="decimal"
      step={step ?? "any"}
      value={value == null ? "" : value}
      placeholder={placeholder}
      onChange={(e) => {
        const t = e.target.value;
        if (t === "") onChange(nullable ? null : 0);
        else {
          const n = Number(t);
          if (Number.isFinite(n)) onChange(n);
        }
      }}
      className={cn("tabular h-8 min-w-[72px] px-2 text-sm", className)}
      {...rest}
    />
  );
}

export function EmptyState({ title, children }: { title: string; children?: ReactNode }) {
  return (
    <div className="flex flex-col items-center justify-center gap-1 rounded-lg border border-dashed px-4 py-10 text-center">
      <p className="text-sm font-medium">{title}</p>
      {children && <div className="text-xs text-muted-foreground">{children}</div>}
    </div>
  );
}

export const tooltipStyle = {
  contentStyle: {
    background: "var(--popover)",
    border: "1px solid var(--border)",
    borderRadius: 8,
    color: "var(--popover-foreground)",
    fontSize: 12,
  },
  itemStyle: { color: "var(--popover-foreground)" },
  labelStyle: { color: "var(--muted-foreground)" },
};
export const axisProps = {
  stroke: "var(--muted-foreground)",
  fontSize: 11,
  tickLine: false,
  axisLine: false,
} as const;

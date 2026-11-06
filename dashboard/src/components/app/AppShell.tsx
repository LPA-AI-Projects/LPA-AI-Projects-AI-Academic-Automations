import { useEffect, useState, type ReactNode } from "react";
import { Link, useRouterState } from "@tanstack/react-router";
import { BarChart3, CalendarDays, Database, ListChecks, Menu, Moon, RotateCcw, Scale, Sigma, Sun, TriangleAlert } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Sheet, SheetContent, SheetTitle } from "@/components/ui/sheet";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { useData } from "@/lib/cost/data";
import { useStore } from "@/lib/cost/store";
import { toast } from "sonner";

const NAV = [
  { to: "/", label: "Cost Analysis", icon: BarChart3 },
  { to: "/compare", label: "Price Comparison", icon: Scale },
  { to: "/requests", label: "Requests", icon: ListChecks },
  { to: "/total", label: "Total", icon: Sigma },
  { to: "/settings", label: "Data & Settings", icon: Database },
] as const;

function useTheme() {
  const [dark, setDark] = useState(false);
  useEffect(() => {
    const saved = localStorage.getItem("oacc-theme");
    const d = saved ? saved === "dark" : window.matchMedia("(prefers-color-scheme: dark)").matches;
    setDark(d);
  }, []);
  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
  }, [dark]);
  const toggle = () =>
    setDark((d) => {
      localStorage.setItem("oacc-theme", d ? "light" : "dark");
      return !d;
    });
  return { dark, toggle };
}

function NavList({ onNav }: { onNav?: () => void }) {
  const { reset } = useStore();
  return (
    <div className="flex h-full flex-col gap-1 p-3">
      <div className="mb-4 flex items-center gap-2.5 px-2 pt-1">
        <div className="brand-mark grid h-8 w-8 place-items-center rounded-lg text-sm font-bold text-primary-foreground">$</div>
        <div className="leading-tight">
          <div className="text-sm font-semibold">Outline AI</div>
          <div className="text-xs text-muted-foreground">Cost Console</div>
        </div>
      </div>
      {NAV.map(({ to, label, icon: Icon }) => (
        <Link
          key={to}
          to={to}
          onClick={onNav}
          activeOptions={{ exact: true }}
          className="flex items-center gap-2.5 rounded-lg px-3 py-2 text-sm font-medium text-sidebar-foreground transition-colors hover:bg-sidebar-accent"
          activeProps={{ className: "bg-sidebar-accent text-sidebar-accent-foreground" }}
        >
          <Icon className="h-4 w-4" />
          {label}
        </Link>
      ))}
      <div className="mt-auto pt-4">
        <Button
          variant="ghost"
          size="sm"
          className="w-full justify-start text-muted-foreground"
          onClick={() => {
            if (confirm("Reset all inputs to defaults? This clears your edits and imported logs.")) {
              reset();
              toast.success("Reset to defaults");
            }
          }}
        >
          <RotateCcw className="mr-2 h-4 w-4" /> Reset to defaults
        </Button>
      </div>
    </div>
  );
}

function MonthFilter() {
  const { months, month, setMonth, connected } = useData();
  return (
    <div className="flex flex-wrap items-center gap-2 border-b bg-card px-4 py-2 text-sm">
      <CalendarDays className="h-4 w-4 text-muted-foreground" />
      <span className="text-muted-foreground">Month</span>
      <Select value={month} onValueChange={setMonth} disabled={!months.length}>
        <SelectTrigger aria-label="Month filter" className="h-8 w-[170px]"><SelectValue /></SelectTrigger>
        <SelectContent>
          <SelectItem value="all">All months</SelectItem>
          {months.map((m) => <SelectItem key={m} value={m}>{m}</SelectItem>)}
        </SelectContent>
      </Select>
      {!connected && (
        <Link to="/settings" className="text-xs text-primary hover:underline">Connect the database to filter by month</Link>
      )}
    </div>
  );
}

export function AppShell({ children }: { children: ReactNode }) {
  const { dark, toggle } = useTheme();
  const [open, setOpen] = useState(false);
  const path = useRouterState({ select: (s) => s.location.pathname });
  const showMonth = !/\/(compare|settings|total)\/?$/.test(path);
  return (
    <div className="flex min-h-screen bg-background">
      <aside className="sticky top-0 hidden h-screen w-60 shrink-0 border-r border-sidebar-border bg-sidebar md:block">
        <NavList />
      </aside>
      <Sheet open={open} onOpenChange={setOpen}>
        <SheetContent side="left" className="w-64 bg-sidebar p-0">
          <SheetTitle className="sr-only">Navigation</SheetTitle>
          <NavList onNav={() => setOpen(false)} />
        </SheetContent>
      </Sheet>
      <div className="flex min-w-0 flex-1 flex-col">
        <div className="flex items-center gap-2 border-b border-warning/30 bg-warning/10 px-4 py-2 text-xs text-foreground sm:text-sm">
          <TriangleAlert className="h-4 w-4 shrink-0 text-warning" />
          <span className="flex-1">
            Prices and token counts are editable assumptions - verify prices on each provider's pricing page.
          </span>
          <Button variant="ghost" size="icon" className="h-7 w-7" onClick={toggle} aria-label="Toggle dark mode">
            {dark ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
          </Button>
        </div>
        {showMonth && <MonthFilter />}
        <div className="flex items-center gap-2 border-b px-4 py-2 md:hidden">
          <Button variant="ghost" size="icon" onClick={() => setOpen(true)} aria-label="Open menu">
            <Menu className="h-5 w-5" />
          </Button>
          <span className="text-sm font-semibold">Outline AI Cost Console</span>
        </div>
        <main className="mx-auto w-full max-w-[1400px] flex-1 space-y-5 p-4 sm:p-6">{children}</main>
      </div>
    </div>
  );
}

export function PageHeader({ title, desc, actions }: { title: string; desc: string; actions?: ReactNode }) {
  return (
    <div className="flex flex-wrap items-end justify-between gap-3">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">{title}</h1>
        <p className="text-sm text-muted-foreground">{desc}</p>
      </div>
      {actions}
    </div>
  );
}

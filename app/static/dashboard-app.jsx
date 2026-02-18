const { useEffect, useMemo, useState } = React;
const Icons = window.lucideReact || {};

const {
  LayoutDashboard,
  Users,
  Store,
  CalendarCheck2,
  CalendarDays,
  Search,
  Plus,
  Upload,
  Clock3,
  ChevronRight,
  Inbox,
  ArrowUpDown,
  Filter,
} = Icons;

const navItems = [
  { label: "Dashboard", href: "/" },
  { label: "12-Month Planner", href: "/calendar" },
  { label: "CVM View", href: "/cvm" },
  { label: "Customers", href: "/customers" },
  { label: "Products", href: "/products" },
  { label: "Visit Events", href: "/events" },
  { label: "Import Workbook", href: "/import" },
];

function cx(...classes) {
  return classes.filter(Boolean).join(" ");
}

function formatDate(value) {
  if (!value) return "-";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" });
}

function formatCompactDate(value) {
  if (!value) return "-";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

function isBetween(value, start, end) {
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return false;
  return d >= start && d <= end;
}

function AppHeader() {
  const path = window.location.pathname;

  return (
    <header className="sticky top-0 z-30 border-b border-slate-200/80 bg-white/85 backdrop-blur">
      <div className="mx-auto flex w-full max-w-7xl items-center gap-4 px-4 py-3 sm:px-6 lg:px-8">
        <a href="/" className="flex items-center gap-2 rounded-md px-1 py-1 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-sky-500">
          <span className="inline-flex h-8 w-8 items-center justify-center rounded-lg bg-sky-600 text-white">
            {LayoutDashboard ? <LayoutDashboard size={16} /> : "P"}
          </span>
          <span className="text-sm font-semibold tracking-wide text-slate-900">Planner Dashboard</span>
        </a>

        <nav className="hidden min-w-0 flex-1 items-center gap-1 overflow-auto md:flex" aria-label="Primary navigation">
          {navItems.map((item) => {
            const active = path === item.href || (item.href !== "/" && path.startsWith(item.href));
            return (
              <a
                key={item.href}
                href={item.href}
                className={cx(
                  "whitespace-nowrap rounded-md px-3 py-2 text-sm font-medium transition",
                  "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-sky-500",
                  active
                    ? "bg-sky-100 text-sky-800"
                    : "text-slate-600 hover:bg-slate-100 hover:text-slate-900"
                )}
              >
                {item.label}
              </a>
            );
          })}
        </nav>

        <div className="ml-auto flex items-center gap-2">
          <a
            href="/import"
            className="inline-flex items-center gap-2 rounded-md border border-slate-300 bg-white px-3 py-2 text-sm font-medium text-slate-700 shadow-sm transition hover:bg-slate-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-sky-500"
          >
            {Upload ? <Upload size={16} /> : null}
            Import
          </a>
          <a
            href="/cvm"
            className="inline-flex items-center gap-2 rounded-md bg-sky-600 px-3 py-2 text-sm font-semibold text-white shadow-sm transition hover:bg-sky-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-sky-500"
          >
            {Plus ? <Plus size={16} /> : null}
            New Visit
          </a>
        </div>
      </div>

      <div className="mx-auto w-full max-w-7xl px-4 pb-3 sm:px-6 lg:hidden">
        <nav className="flex gap-2 overflow-auto" aria-label="Primary navigation mobile">
          {navItems.map((item) => {
            const active = path === item.href || (item.href !== "/" && path.startsWith(item.href));
            return (
              <a
                key={item.href + "-mobile"}
                href={item.href}
                className={cx(
                  "whitespace-nowrap rounded-md px-3 py-1.5 text-xs font-medium",
                  active ? "bg-sky-100 text-sky-800" : "bg-slate-100 text-slate-700"
                )}
              >
                {item.label}
              </a>
            );
          })}
        </nav>
      </div>
    </header>
  );
}

function Skeleton({ className = "" }) {
  return <div className={cx("animate-pulse rounded-md bg-slate-200", className)} />;
}

function StatCard({ href, icon: Icon, label, value, helper }) {
  return (
    <a
      href={href}
      className="group block rounded-xl border border-slate-200 bg-white p-4 shadow-sm transition hover:-translate-y-0.5 hover:shadow-soft focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-sky-500"
    >
      <div className="flex items-start justify-between">
        <div>
          <p className="text-xs font-medium uppercase tracking-wide text-slate-500">{label}</p>
          <p className="mt-2 text-3xl font-semibold text-slate-900">{value}</p>
        </div>
        <span className="inline-flex h-9 w-9 items-center justify-center rounded-lg bg-slate-100 text-slate-700 group-hover:bg-sky-100 group-hover:text-sky-700">
          {Icon ? <Icon size={18} /> : null}
        </span>
      </div>
      <p className="mt-3 text-sm text-slate-500">{helper}</p>
    </a>
  );
}

function StatusBadge({ status }) {
  const normalized = String(status || "").toLowerCase();
  const styles = normalized === "completed"
    ? "bg-emerald-50 text-emerald-700 ring-emerald-200"
    : normalized === "cancelled"
      ? "bg-rose-50 text-rose-700 ring-rose-200"
      : "bg-sky-50 text-sky-700 ring-sky-200";

  return (
    <span className={cx("inline-flex items-center rounded-full px-2.5 py-1 text-xs font-medium ring-1 ring-inset", styles)}>
      {status || "Planned"}
    </span>
  );
}

function Next7DaysTimeline({ events, today }) {
  const blocks = useMemo(() => {
    const start = new Date(today + "T00:00:00");
    return Array.from({ length: 7 }).map((_, i) => {
      const d = new Date(start);
      d.setDate(d.getDate() + i);
      const key = d.toISOString().slice(0, 10);
      const dayEvents = events.filter((e) => e.event_date === key);
      return {
        key,
        label: d.toLocaleDateString(undefined, { weekday: "short" }),
        dateLabel: d.toLocaleDateString(undefined, { month: "short", day: "numeric" }),
        items: dayEvents,
      };
    });
  }, [events, today]);

  return (
    <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
      <div className="mb-4 flex items-center justify-between">
        <h2 className="text-base font-semibold text-slate-900">Next 7 Days</h2>
        <span className="text-xs text-slate-500">Mini timeline</span>
      </div>
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-7">
        {blocks.map((block) => (
          <div key={block.key} className="rounded-lg border border-slate-200 bg-slate-50/60 p-3">
            <div className="flex items-center justify-between">
              <p className="text-xs font-semibold text-slate-700">{block.label}</p>
              <p className="text-xs text-slate-500">{block.dateLabel}</p>
            </div>
            <div className="mt-3 space-y-2">
              {block.items.length === 0 ? (
                <p className="text-xs text-slate-400">No visits</p>
              ) : (
                block.items.slice(0, 3).map((event, idx) => (
                  <div key={idx} className="rounded-md bg-white px-2 py-1 text-xs text-slate-700 ring-1 ring-slate-200">
                    <p className="truncate font-medium">{event.customer_name}</p>
                    <p className="text-[11px] text-slate-500">{event.status}</p>
                  </div>
                ))
              )}
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function UpcomingEventsTable({ events, loading }) {
  const [query, setQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [rangePreset, setRangePreset] = useState("7");
  const [sortBy, setSortBy] = useState("date-asc");

  const today = new Date();
  today.setHours(0, 0, 0, 0);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();

    let rows = [...events];

    if (statusFilter !== "all") {
      rows = rows.filter((r) => String(r.status || "").toLowerCase() === statusFilter);
    }

    if (rangePreset !== "all") {
      const start = new Date(today);
      let end = new Date(today);
      if (rangePreset === "today") {
        end = new Date(today);
      } else if (rangePreset === "7") {
        end.setDate(end.getDate() + 7);
      } else if (rangePreset === "30") {
        end.setDate(end.getDate() + 30);
      }
      rows = rows.filter((r) => isBetween((r.event_date || "") + "T00:00:00", start, end));
    }

    if (q) {
      rows = rows.filter((r) =>
        [r.customer_name, r.cust_code, r.territory, r.status]
          .filter(Boolean)
          .join(" ")
          .toLowerCase()
          .includes(q)
      );
    }

    rows.sort((a, b) => {
      if (sortBy === "date-desc") {
        return String(b.event_date).localeCompare(String(a.event_date));
      }
      if (sortBy === "customer") {
        return String(a.customer_name || "").localeCompare(String(b.customer_name || ""));
      }
      if (sortBy === "status") {
        return String(a.status || "").localeCompare(String(b.status || ""));
      }
      return String(a.event_date).localeCompare(String(b.event_date));
    });

    return rows;
  }, [events, query, rangePreset, sortBy, statusFilter]);

  return (
    <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
      <div className="mb-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <h2 className="text-base font-semibold text-slate-900">Upcoming Events</h2>
        <span className="text-xs text-slate-500">{filtered.length} result{filtered.length === 1 ? "" : "s"}</span>
      </div>

      <div className="mb-4 grid gap-2 md:grid-cols-4">
        <label className="relative block">
          <span className="sr-only">Search events</span>
          <span className="pointer-events-none absolute inset-y-0 left-3 flex items-center text-slate-400">
            {Search ? <Search size={16} /> : null}
          </span>
          <input
            aria-label="Search upcoming events"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search customer, code, territory"
            className="w-full rounded-md border border-slate-300 bg-white py-2 pl-9 pr-3 text-sm text-slate-900 placeholder:text-slate-400 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-sky-500"
          />
        </label>

        <label className="relative block">
          <span className="pointer-events-none absolute inset-y-0 left-3 flex items-center text-slate-400">
            {Filter ? <Filter size={16} /> : null}
          </span>
          <select
            aria-label="Filter by status"
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
            className="w-full rounded-md border border-slate-300 bg-white py-2 pl-9 pr-3 text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-sky-500"
          >
            <option value="all">Status: All</option>
            <option value="planned">Planned</option>
            <option value="completed">Completed</option>
            <option value="cancelled">Cancelled</option>
          </select>
        </label>

        <select
          aria-label="Date range preset"
          value={rangePreset}
          onChange={(e) => setRangePreset(e.target.value)}
          className="w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-sky-500"
        >
          <option value="today">Today</option>
          <option value="7">7 days</option>
          <option value="30">30 days</option>
          <option value="all">All dates</option>
        </select>

        <label className="relative block">
          <span className="pointer-events-none absolute inset-y-0 left-3 flex items-center text-slate-400">
            {ArrowUpDown ? <ArrowUpDown size={16} /> : null}
          </span>
          <select
            aria-label="Sort events"
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value)}
            className="w-full rounded-md border border-slate-300 bg-white py-2 pl-9 pr-3 text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-sky-500"
          >
            <option value="date-asc">Sort: Date ↑</option>
            <option value="date-desc">Sort: Date ↓</option>
            <option value="customer">Sort: Customer</option>
            <option value="status">Sort: Status</option>
          </select>
        </label>
      </div>

      <div className="overflow-hidden rounded-lg border border-slate-200">
        <table className="min-w-full divide-y divide-slate-200" aria-label="Upcoming events table">
          <thead className="bg-slate-50">
            <tr>
              <th className="px-4 py-2 text-left text-xs font-semibold uppercase tracking-wide text-slate-500">Date</th>
              <th className="px-4 py-2 text-left text-xs font-semibold uppercase tracking-wide text-slate-500">Customer</th>
              <th className="px-4 py-2 text-left text-xs font-semibold uppercase tracking-wide text-slate-500">Territory</th>
              <th className="px-4 py-2 text-left text-xs font-semibold uppercase tracking-wide text-slate-500">Status</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-200 bg-white">
            {loading ? (
              Array.from({ length: 6 }).map((_, i) => (
                <tr key={i}>
                  <td className="px-4 py-3"><Skeleton className="h-4 w-24" /></td>
                  <td className="px-4 py-3"><Skeleton className="h-4 w-44" /></td>
                  <td className="px-4 py-3"><Skeleton className="h-4 w-24" /></td>
                  <td className="px-4 py-3"><Skeleton className="h-6 w-20" /></td>
                </tr>
              ))
            ) : filtered.length === 0 ? (
              <tr>
                <td colSpan={4} className="px-6 py-12 text-center">
                  <div className="mx-auto mb-3 inline-flex h-10 w-10 items-center justify-center rounded-full bg-slate-100 text-slate-500">
                    {Inbox ? <Inbox size={18} /> : null}
                  </div>
                  <p className="text-sm font-medium text-slate-900">No upcoming events found</p>
                  <p className="mt-1 text-sm text-slate-500">Adjust filters or create a new visit to get started.</p>
                  <a
                    href="/cvm"
                    className="mt-4 inline-flex items-center gap-2 rounded-md bg-sky-600 px-3 py-2 text-sm font-medium text-white hover:bg-sky-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-sky-500"
                  >
                    {Plus ? <Plus size={16} /> : null}
                    Create Visit
                  </a>
                </td>
              </tr>
            ) : (
              filtered.map((event, idx) => (
                <tr
                  key={`${event.id || idx}-${event.event_date}`}
                  role="button"
                  tabIndex={0}
                  aria-label={`Open event for ${event.customer_name} on ${event.event_date}`}
                  onClick={() => {}}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" || e.key === " ") {
                      e.preventDefault();
                    }
                  }}
                  className="cursor-default transition hover:bg-slate-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-sky-500"
                >
                  <td className="px-4 py-3 text-sm text-slate-700">{formatDate(event.event_date)}</td>
                  <td className="px-4 py-3 text-sm text-slate-900">
                    <div className="font-medium">{event.customer_name}</div>
                    <div className="text-xs text-slate-500">{event.cust_code || "-"}</div>
                  </td>
                  <td className="px-4 py-3 text-sm text-slate-700">{event.territory || "-"}</td>
                  <td className="px-4 py-3"><StatusBadge status={event.status || "Planned"} /></td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function DashboardApp({ data }) {
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const timer = setTimeout(() => setLoading(false), 450);
    return () => clearTimeout(timer);
  }, []);

  const counts = data.counts || {};
  const settings = data.settings || {};
  const upcoming = data.upcoming || [];

  const statCards = [
    {
      label: "Customers",
      value: counts.customers ?? 0,
      helper: (counts.customers || 0) > 0 ? "Active customer records" : "No customers yet",
      href: "/customers",
      icon: Users,
    },
    {
      label: "Stores",
      value: counts.stores ?? 0,
      helper: (counts.stores || 0) > 0 ? "Connected locations" : "No stores yet",
      href: "/customers",
      icon: Store,
    },
    {
      label: "Visit Events",
      value: counts.cvm_entries ?? 0,
      helper: "Tracked in CVM",
      href: "/cvm",
      icon: CalendarCheck2,
    },
    {
      label: "Calendar Year",
      value: settings.calendar_year ?? "-",
      helper: `Week starts ${settings.week_start_day || "monday"}`,
      href: "/calendar",
      icon: CalendarDays,
    },
  ];

  return (
    <div className="min-h-screen bg-slate-50">
      <AppHeader />

      <main className="mx-auto w-full max-w-7xl space-y-6 px-4 py-6 sm:px-6 lg:px-8">
        <section className="space-y-4">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-semibold text-slate-900">Planner Dashboard</h1>
              <p className="text-sm text-slate-500">Monitor visits, workload, and upcoming commitments.</p>
            </div>
            <p className="hidden items-center gap-2 text-sm text-slate-500 md:flex">
              {Clock3 ? <Clock3 size={16} /> : null}
              {formatDate(data.today)}
            </p>
          </div>

          <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
            {loading
              ? Array.from({ length: 4 }).map((_, i) => (
                  <div key={i} className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
                    <Skeleton className="h-4 w-24" />
                    <Skeleton className="mt-3 h-8 w-20" />
                    <Skeleton className="mt-4 h-4 w-36" />
                  </div>
                ))
              : statCards.map((card) => <StatCard key={card.label} {...card} />)}
          </div>

          {!loading && (counts.stores || 0) === 0 ? (
            <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-amber-900">
              <div className="flex flex-col items-start justify-between gap-3 sm:flex-row sm:items-center">
                <div>
                  <p className="text-sm font-semibold">Add your first store</p>
                  <p className="text-sm text-amber-800">Set up a location profile to improve visit planning context.</p>
                </div>
                <a
                  href="/customers"
                  className="inline-flex items-center gap-1 rounded-md border border-amber-300 bg-white px-3 py-2 text-sm font-medium text-amber-900 hover:bg-amber-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-amber-500"
                >
                  Go to Customers
                  {ChevronRight ? <ChevronRight size={16} /> : null}
                </a>
              </div>
            </div>
          ) : null}
        </section>

        <Next7DaysTimeline events={upcoming} today={data.today} />
        <UpcomingEventsTable events={upcoming} loading={loading} />
      </main>
    </div>
  );
}

const initialData = (() => {
  try {
    const raw = document.getElementById("dashboard-data")?.textContent || "{}";
    return JSON.parse(raw);
  } catch (_) {
    return {};
  }
})();

const root = ReactDOM.createRoot(document.getElementById("dashboard-root"));
root.render(<DashboardApp data={initialData} />);

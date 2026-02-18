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
  Menu,
  X,
  CheckCircle2,
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
  const d = new Date(value + (String(value).includes("T") ? "" : "T00:00:00"));
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" });
}

function parseDate(value) {
  if (!value) return null;
  const d = new Date(String(value).includes("T") ? value : value + "T00:00:00");
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

function isBetween(value, start, end) {
  const d = parseDate(value);
  if (!d) return false;
  return d >= start && d <= end;
}

function touchTarget() {
  return "min-h-11";
}

function NavLinks({ mobile = false, onNavigate }) {
  const path = window.location.pathname;
  return navItems.map((item) => {
    const active = path === item.href || (item.href !== "/" && path.startsWith(item.href));
    return (
      <a
        key={item.href + (mobile ? "-mobile" : "-desktop")}
        href={item.href}
        onClick={onNavigate}
        className={cx(
          touchTarget(),
          "inline-flex items-center rounded-xl px-3.5 text-sm font-medium transition",
          "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]",
          mobile && "w-full",
          active
            ? "bg-sky-100 text-sky-900"
            : "text-slate-600 hover:bg-slate-100 hover:text-slate-900 active:bg-slate-200"
        )}
      >
        {item.label}
      </a>
    );
  });
}

function SideNavSheet({ open, onClose }) {
  useEffect(() => {
    if (!open) return undefined;

    const onEscape = (event) => {
      if (event.key === "Escape") onClose();
    };

    document.body.style.overflow = "hidden";
    window.addEventListener("keydown", onEscape);
    return () => {
      document.body.style.overflow = "";
      window.removeEventListener("keydown", onEscape);
    };
  }, [open, onClose]);

  return (
    <div
      className={cx(
        "fixed inset-0 z-50 transition",
        open ? "pointer-events-auto" : "pointer-events-none"
      )}
      aria-hidden={!open}
    >
      <button
        type="button"
        aria-label="Close menu"
        onClick={onClose}
        className={cx(
          "absolute inset-0 bg-slate-900/35 transition-opacity",
          open ? "opacity-100" : "opacity-0"
        )}
      />
      <aside
        role="dialog"
        aria-modal="true"
        aria-label="Navigation menu"
        className={cx(
          "absolute right-0 top-0 h-full w-[min(88vw,360px)] bg-white p-5 shadow-2xl ring-1 ring-slate-200 transition-transform",
          open ? "translate-x-0" : "translate-x-full"
        )}
      >
        <div className="mb-4 flex items-center justify-between">
          <p className="text-sm font-semibold text-slate-900">Menu</p>
          <button
            type="button"
            aria-label="Close menu"
            onClick={onClose}
            className={cx(
              touchTarget(),
              "inline-flex w-11 items-center justify-center rounded-lg border border-slate-200 text-slate-600 hover:bg-slate-100",
              "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
            )}
          >
            {X ? <X size={18} /> : "X"}
          </button>
        </div>

        <nav className="grid gap-2" aria-label="Mobile navigation">
          <NavLinks mobile onNavigate={onClose} />
        </nav>

        <div className="mt-6 grid gap-2 border-t border-slate-200 pt-4">
          <a
            href="/import"
            className={cx(
              touchTarget(),
              "inline-flex items-center justify-center gap-2 rounded-xl border border-slate-300 bg-white px-3 text-sm font-medium text-slate-700",
              "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
            )}
          >
            {Upload ? <Upload size={16} /> : null}
            Import
          </a>
          <a
            href="/cvm"
            className={cx(
              touchTarget(),
              "inline-flex items-center justify-center gap-2 rounded-xl bg-sky-600 px-3 text-sm font-semibold text-white hover:bg-sky-700",
              "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
            )}
          >
            {Plus ? <Plus size={16} /> : null}
            New Visit
          </a>
        </div>
      </aside>
    </div>
  );
}

function AppHeader() {
  const [menuOpen, setMenuOpen] = useState(false);

  return (
    <>
      <header className="sticky top-0 z-40 border-b border-slate-200/90 bg-white/85 backdrop-blur-md supports-[backdrop-filter]:bg-white/72">
        <div className="mx-auto flex w-full max-w-7xl items-center gap-3 px-4 py-3 sm:px-6 lg:px-8">
          <a
            href="/"
            className={cx(
              touchTarget(),
              "inline-flex items-center gap-2 rounded-xl px-2 font-semibold tracking-[0.01em] text-slate-900",
              "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
            )}
            aria-label="Go to dashboard"
          >
            <span className="inline-flex h-8 w-8 items-center justify-center rounded-lg bg-sky-600 text-white">
              {LayoutDashboard ? <LayoutDashboard size={16} /> : "P"}
            </span>
            <span className="hidden sm:inline">Planner Dashboard</span>
          </a>

          <nav className="hidden items-center gap-1 lg:flex" aria-label="Primary navigation">
            <NavLinks />
          </nav>

          <button
            type="button"
            aria-label="Open menu"
            onClick={() => setMenuOpen(true)}
            className={cx(
              touchTarget(),
              "inline-flex w-11 items-center justify-center rounded-xl border border-slate-200 text-slate-700 lg:hidden",
              "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
            )}
          >
            {Menu ? <Menu size={18} /> : "Menu"}
          </button>

          <div className="ml-auto hidden items-center gap-2 sm:flex">
            <a
              href="/import"
              className={cx(
                touchTarget(),
                "inline-flex items-center gap-2 rounded-xl border border-slate-300 bg-white px-4 text-sm font-medium text-slate-700",
                "hover:bg-slate-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
              )}
            >
              {Upload ? <Upload size={16} /> : null}
              Import
            </a>
            <a
              href="/cvm"
              className={cx(
                touchTarget(),
                "inline-flex items-center gap-2 rounded-xl bg-sky-600 px-4 text-sm font-semibold text-white",
                "hover:bg-sky-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
              )}
            >
              {Plus ? <Plus size={16} /> : null}
              New Visit
            </a>
          </div>
        </div>
      </header>

      <SideNavSheet open={menuOpen} onClose={() => setMenuOpen(false)} />
    </>
  );
}

function SectionHeader({ title, helper, right }) {
  return (
    <div className="mb-4 flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
      <div>
        <h2 className="text-lg font-semibold text-slate-900">{title}</h2>
        {helper ? <p className="mt-1 text-sm text-slate-500">{helper}</p> : null}
      </div>
      {right ? <div>{right}</div> : null}
    </div>
  );
}

function Skeleton({ className = "" }) {
  return <div className={cx("animate-pulse rounded-xl bg-slate-200", className)} />;
}

function StatCard({ href, icon: Icon, label, value, helper }) {
  return (
    <a
      href={href}
      className={cx(
        "group block rounded-2xl border border-slate-200 bg-white p-4 shadow-sm transition",
        "hover:-translate-y-0.5 hover:shadow-md active:bg-slate-50",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
      )}
    >
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">{label}</p>
          <p className="mt-2 text-3xl font-semibold leading-none text-slate-900">{value}</p>
        </div>
        <span className="inline-flex h-11 w-11 items-center justify-center rounded-xl bg-slate-100 text-slate-700 transition group-hover:bg-sky-100 group-hover:text-sky-700">
          {Icon ? <Icon size={18} /> : null}
        </span>
      </div>
      <p className="mt-3 text-sm text-slate-500">{helper}</p>
    </a>
  );
}

function EmptyState({ title, helper, ctaHref, ctaLabel }) {
  return (
    <div className="px-6 py-12 text-center">
      <div className="mx-auto mb-3 inline-flex h-11 w-11 items-center justify-center rounded-full bg-slate-100 text-slate-500">
        {Inbox ? <Inbox size={18} /> : null}
      </div>
      <p className="text-sm font-semibold text-slate-900">{title}</p>
      <p className="mt-1 text-sm text-slate-500">{helper}</p>
      <a
        href={ctaHref}
        className={cx(
          touchTarget(),
          "mt-4 inline-flex items-center gap-2 rounded-xl bg-sky-600 px-4 text-sm font-medium text-white hover:bg-sky-700",
          "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
        )}
      >
        {Plus ? <Plus size={16} /> : null}
        {ctaLabel}
      </a>
    </div>
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
    <span className={cx("inline-flex min-h-7 items-center rounded-full px-3 text-xs font-semibold ring-1 ring-inset", styles)}>
      {status || "Planned"}
    </span>
  );
}

function VisitsTrendChart({ series, loading }) {
  const points = useMemo(() => {
    const safe = Array.isArray(series) && series.length > 0 ? series : [];
    const maxY = Math.max(
      1,
      ...safe.map((item) => Math.max(Number(item.planned || 0), Number(item.completed || 0)))
    );
    const width = 640;
    const height = 260;
    const padX = 34;
    const padY = 24;

    const getPolyline = (key) =>
      safe
        .map((item, idx) => {
          const x = padX + (idx * (width - padX * 2)) / Math.max(1, safe.length - 1);
          const y = height - padY - (Number(item[key] || 0) / maxY) * (height - padY * 2);
          return `${x},${y}`;
        })
        .join(" ");

    const ticks = [0, 0.25, 0.5, 0.75, 1].map((ratio) => {
      const y = height - padY - ratio * (height - padY * 2);
      return {
        y,
        value: Math.round(maxY * ratio),
      };
    });

    return {
      safe,
      width,
      height,
      plannedLine: getPolyline("planned"),
      completedLine: getPolyline("completed"),
      ticks,
    };
  }, [series]);

  return (
    <section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
      <SectionHeader
        title="Visits Over 12 Months"
        helper="Planned vs completed from CVM entries"
        right={
          <span className="inline-flex items-center gap-3 text-xs text-slate-500">
            <span className="inline-flex items-center gap-1.5">
              <span className="h-2.5 w-2.5 rounded-full bg-sky-500" /> Planned
            </span>
            <span className="inline-flex items-center gap-1.5">
              <span className="h-2.5 w-2.5 rounded-full bg-emerald-500" /> Completed
            </span>
          </span>
        }
      />

      {loading ? (
        <Skeleton className="h-56 w-full" />
      ) : points.safe.length === 0 ? (
        <EmptyState
          title="No visit data yet"
          helper="Import workbook data or add visits to see a trend chart."
          ctaHref="/import"
          ctaLabel="Import Workbook"
        />
      ) : (
        <div>
          <div className="overflow-x-auto" style={{ WebkitOverflowScrolling: "touch" }}>
            <svg
              viewBox={`0 0 ${points.width} ${points.height}`}
              className="h-56 w-full min-w-[580px]"
              aria-label="Planned vs completed visits over 12 months"
              role="img"
            >
              {points.ticks.map((tick, idx) => (
                <g key={idx}>
                  <line x1="34" x2={points.width - 34} y1={tick.y} y2={tick.y} stroke="#e2e8f0" strokeDasharray="4 4" />
                  <text x="6" y={tick.y + 4} fill="#64748b" fontSize="11">
                    {tick.value}
                  </text>
                </g>
              ))}
              <polyline fill="none" stroke="#0ea5e9" strokeWidth="3" points={points.plannedLine} />
              <polyline fill="none" stroke="#10b981" strokeWidth="3" points={points.completedLine} />
              {points.safe.map((item, idx) => {
                const x = 34 + (idx * (points.width - 68)) / Math.max(1, points.safe.length - 1);
                const showLabel = idx % 2 === 0 || idx === points.safe.length - 1;
                return (
                  <g key={item.label + idx}>
                    {showLabel ? (
                      <text x={x} y={points.height - 6} textAnchor="middle" fill="#64748b" fontSize="11">
                        {item.label}
                      </text>
                    ) : null}
                  </g>
                );
              })}
            </svg>
          </div>
        </div>
      )}
    </section>
  );
}

function UpcomingEventsTable({ events, loading }) {
  const [query, setQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [rangePreset, setRangePreset] = useState("7");
  const [sortBy, setSortBy] = useState("date-asc");

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    const start = new Date();
    start.setHours(0, 0, 0, 0);
    let end = new Date(start);

    if (rangePreset === "today") {
      end = new Date(start);
    } else if (rangePreset === "7") {
      end.setDate(end.getDate() + 7);
    } else if (rangePreset === "30") {
      end.setDate(end.getDate() + 30);
    }

    let rows = [...events];

    if (statusFilter !== "all") {
      rows = rows.filter((r) => String(r.status || "").toLowerCase() === statusFilter);
    }

    if (rangePreset !== "all") {
      rows = rows.filter((r) => isBetween(r.event_date || "", start, end));
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
        return String(b.event_date || "").localeCompare(String(a.event_date || ""));
      }
      if (sortBy === "customer") {
        return String(a.customer_name || "").localeCompare(String(b.customer_name || ""));
      }
      if (sortBy === "status") {
        return String(a.status || "").localeCompare(String(b.status || ""));
      }
      return String(a.event_date || "").localeCompare(String(b.event_date || ""));
    });

    return rows;
  }, [events, query, statusFilter, rangePreset, sortBy]);

  return (
    <section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
      <SectionHeader
        title="Upcoming Events"
        helper="Search and filter upcoming planned/completed visits"
        right={<span className="text-xs text-slate-500">{filtered.length} results</span>}
      />

      <div className="mb-4 grid gap-2 md:grid-cols-2 xl:grid-cols-4">
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
            className={cx(
              touchTarget(),
              "w-full rounded-xl border border-slate-300 bg-white py-2 pl-9 pr-3 text-base text-slate-900 placeholder:text-slate-400",
              "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
            )}
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
            className={cx(
              touchTarget(),
              "w-full rounded-xl border border-slate-300 bg-white py-2 pl-9 pr-3 text-base text-slate-900",
              "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
            )}
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
          className={cx(
            touchTarget(),
            "w-full rounded-xl border border-slate-300 bg-white px-3 text-base text-slate-900",
            "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
          )}
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
            className={cx(
              touchTarget(),
              "w-full rounded-xl border border-slate-300 bg-white py-2 pl-9 pr-3 text-base text-slate-900",
              "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
            )}
          >
            <option value="date-asc">Sort: Date ↑</option>
            <option value="date-desc">Sort: Date ↓</option>
            <option value="customer">Sort: Customer</option>
            <option value="status">Sort: Status</option>
          </select>
        </label>
      </div>

      <div className="overflow-auto rounded-xl border border-slate-200" style={{ WebkitOverflowScrolling: "touch" }}>
        <table className="min-w-full divide-y divide-slate-200" aria-label="Upcoming events table">
          <thead className="bg-slate-50">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wide text-slate-500">Date</th>
              <th className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wide text-slate-500">Customer</th>
              <th className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wide text-slate-500">Territory</th>
              <th className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wide text-slate-500">Status</th>
              <th className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wide text-slate-500">View</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-200 bg-white">
            {loading ? (
              Array.from({ length: 6 }).map((_, i) => (
                <tr key={i}>
                  <td className="px-4 py-3"><Skeleton className="h-5 w-24" /></td>
                  <td className="px-4 py-3"><Skeleton className="h-5 w-44" /></td>
                  <td className="px-4 py-3"><Skeleton className="h-5 w-24" /></td>
                  <td className="px-4 py-3"><Skeleton className="h-7 w-20" /></td>
                  <td className="px-4 py-3"><Skeleton className="h-9 w-16" /></td>
                </tr>
              ))
            ) : filtered.length === 0 ? (
              <tr>
                <td colSpan={5}>
                  <EmptyState
                    title="No upcoming events found"
                    helper="Adjust your filters or create a new visit."
                    ctaHref="/cvm"
                    ctaLabel="Create Visit"
                  />
                </td>
              </tr>
            ) : (
              filtered.map((event, idx) => (
                <tr
                  key={`${event.id || idx}-${event.event_date}`}
                  tabIndex={0}
                  aria-label={`Event for ${event.customer_name} on ${event.event_date}`}
                  className={cx(
                    "transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[var(--ring)]",
                    "hover:bg-slate-50"
                  )}
                >
                  <td className="whitespace-nowrap px-4 py-3 text-sm text-slate-700">{formatDate(event.event_date)}</td>
                  <td className="px-4 py-3 text-sm text-slate-900">
                    <div className="font-medium">{event.customer_name}</div>
                    <div className="text-xs text-slate-500">{event.cust_code || "-"}</div>
                  </td>
                  <td className="whitespace-nowrap px-4 py-3 text-sm text-slate-700">{event.territory || "-"}</td>
                  <td className="px-4 py-3"><StatusBadge status={event.status || "Planned"} /></td>
                  <td className="px-4 py-3">
                    <button
                      type="button"
                      aria-label="View event details"
                      className={cx(
                        touchTarget(),
                        "inline-flex min-w-16 items-center justify-center gap-1 rounded-lg border border-slate-300 px-3 text-sm text-slate-700",
                        "hover:bg-slate-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
                      )}
                      onClick={() => {}}
                    >
                      View
                      {ChevronRight ? <ChevronRight size={15} /> : null}
                    </button>
                  </td>
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
    const timer = setTimeout(() => setLoading(false), 420);
    return () => clearTimeout(timer);
  }, []);

  const counts = data.counts || {};
  const settings = data.settings || {};
  const upcoming = data.upcoming || [];
  const visitsByMonth = data.visitsByMonth || [];

  const statCards = [
    {
      label: "Customers",
      value: counts.customers ?? 0,
      helper: (counts.customers || 0) > 0 ? "active records" : "no customers yet",
      href: "/customers",
      icon: Users,
    },
    {
      label: "Stores",
      value: counts.stores ?? 0,
      helper: (counts.stores || 0) > 0 ? "connected locations" : "no stores yet",
      href: "/customers",
      icon: Store,
    },
    {
      label: "Visit Events",
      value: counts.cvm_entries ?? 0,
      helper: "tracked in CVM",
      href: "/cvm",
      icon: CalendarCheck2,
    },
    {
      label: "Calendar Year",
      value: settings.calendar_year ?? "-",
      helper: `week starts ${settings.week_start_day || "monday"}`,
      href: "/calendar",
      icon: CalendarDays,
    },
  ];

  return (
    <div className="min-h-screen bg-[var(--bg)] text-slate-900 antialiased">
      <AppHeader />

      <main className="mx-auto w-full max-w-7xl space-y-6 px-4 py-6 sm:px-6 lg:px-8">
        <section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <SectionHeader
            title="Planner Dashboard"
            helper="Track planning progress, upcoming visits, and activity at a glance."
            right={
              <span className="inline-flex items-center gap-2 text-sm text-slate-500">
                {Clock3 ? <Clock3 size={16} /> : null}
                {formatDate(data.today)}
              </span>
            }
          />

          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            {loading
              ? Array.from({ length: 4 }).map((_, i) => (
                  <div key={i} className="rounded-2xl border border-slate-200 bg-white p-4">
                    <Skeleton className="h-4 w-24" />
                    <Skeleton className="mt-3 h-9 w-24" />
                    <Skeleton className="mt-4 h-4 w-32" />
                  </div>
                ))
              : statCards.map((card) => <StatCard key={card.label} {...card} />)}
          </div>

          {!loading && (counts.stores || 0) === 0 ? (
            <div className="mt-4 rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-amber-900">
              <div className="flex flex-col items-start justify-between gap-3 sm:flex-row sm:items-center">
                <div>
                  <p className="text-sm font-semibold">Add your first store</p>
                  <p className="text-sm text-amber-800">Create a location profile to improve visit planning context.</p>
                </div>
                <a
                  href="/customers"
                  className={cx(
                    touchTarget(),
                    "inline-flex items-center gap-1 rounded-lg border border-amber-300 bg-white px-3 text-sm font-medium text-amber-900",
                    "hover:bg-amber-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-amber-500"
                  )}
                >
                  Go to Customers
                  {ChevronRight ? <ChevronRight size={16} /> : null}
                </a>
              </div>
            </div>
          ) : null}
        </section>

        <div className="grid gap-6 md:grid-cols-12">
          <div className="md:col-span-12 lg:col-span-5">
            <VisitsTrendChart series={visitsByMonth} loading={loading} />
          </div>
          <div className="md:col-span-12 lg:col-span-7">
            <UpcomingEventsTable events={upcoming} loading={loading} />
          </div>
        </div>
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

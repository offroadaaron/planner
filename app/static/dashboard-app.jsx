const { useEffect, useMemo, useState } = React;
const Icons = window.LucideReact || window.lucideReact || {};

const {
  LayoutDashboard,
  Users,
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
  Download,
  Check,
  AlertCircle,
  Minus,
  TrendingUp,
  ChevronDown,
  Sun,
  Moon,
} = Icons;

const navItems = [
  { label: "Dashboard", href: "/" },
  { label: "12-Month Planner", href: "/calendar" },
  { label: "CVM View", href: "/cvm" },
  { label: "Customers", href: "/customers" },
  { label: "Products", href: "/products" },
  { label: "Import Workbook", href: "/import" },
];

const exportReportTypes = [
  { value: "executive_summary", label: "Executive Summary" },
  { value: "visit_detail", label: "Visit Detail Report" },
  { value: "monthly_summary", label: "Monthly Summary" },
  { value: "customer_performance", label: "Customer Performance" },
];

const exportFormats = [
  { value: "csv", label: "CSV" },
  { value: "xlsx", label: "XLSX" },
  { value: "pdf", label: "PDF" },
];

const THEME_STORAGE_KEY = "planner.theme";

function cx(...classes) {
  return classes.filter(Boolean).join(" ");
}

function touchTarget() {
  return "min-h-11";
}

function readStoredTheme() {
  try {
    const stored = window.localStorage.getItem(THEME_STORAGE_KEY);
    if (stored === "dark" || stored === "light") return stored;
  } catch (_) {
    return null;
  }
  return null;
}

function applyTheme(theme) {
  const nextTheme = theme === "dark" ? "dark" : "light";
  document.documentElement.setAttribute("data-theme", nextTheme);
  document.documentElement.classList.toggle("dark", nextTheme === "dark");
}

function useThemePreference() {
  const stored = readStoredTheme();
  const [theme, setTheme] = useState(() => stored || "dark");
  const [hasUserChoice, setHasUserChoice] = useState(() => Boolean(stored));

  useEffect(() => {
    applyTheme(theme);
    try {
      if (hasUserChoice) {
        window.localStorage.setItem(THEME_STORAGE_KEY, theme);
      } else {
        window.localStorage.removeItem(THEME_STORAGE_KEY);
      }
    } catch (_) {}
  }, [theme, hasUserChoice]);

  const toggleTheme = () => {
    setHasUserChoice(true);
    setTheme((current) => (current === "dark" ? "light" : "dark"));
  };

  return { theme, toggleTheme };
}

function formatDate(value) {
  if (!value) return "-";
  const d = new Date(String(value).includes("T") ? value : value + "T00:00:00");
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

function normalizeStatus(status) {
  const normalized = String(status || "").toLowerCase();
  if (normalized === "completed") return "completed";
  if (normalized === "cancelled") return "cancelled";
  return "planned";
}

function rangeBounds(rangePreset, customStart, customEnd) {
  const today = new Date();
  today.setHours(0, 0, 0, 0);

  if (rangePreset === "today") {
    return { start: new Date(today), end: new Date(today) };
  }

  if (rangePreset === "7") {
    const end = new Date(today);
    end.setDate(end.getDate() + 7);
    return { start: new Date(today), end };
  }

  if (rangePreset === "30") {
    const end = new Date(today);
    end.setDate(end.getDate() + 30);
    return { start: new Date(today), end };
  }

  if (rangePreset === "custom") {
    const start = parseDate(customStart || "");
    const end = parseDate(customEnd || "");
    if (start && end) {
      return { start, end };
    }
  }

  return null;
}

function rangeLabel(rangePreset, customStart, customEnd) {
  if (rangePreset === "today") return "Today";
  if (rangePreset === "7") return "Next 7 days";
  if (rangePreset === "30") return "Next 30 days";
  if (rangePreset === "custom") {
    if (customStart && customEnd) return `${formatDate(customStart)} to ${formatDate(customEnd)}`;
    return "Custom";
  }
  return "All dates";
}

function filterAndSortEvents(events, filters) {
  const q = String(filters.query || "").trim().toLowerCase();
  const statusFilter = String(filters.statusFilter || "all").toLowerCase();
  const sortBy = String(filters.sortBy || "date-asc");
  const preset = String(filters.rangePreset || "all");
  const customStart = filters.customStart || "";
  const customEnd = filters.customEnd || "";

  const bounds = rangeBounds(preset, customStart, customEnd);

  let rows = [...events];

  if (statusFilter !== "all") {
    rows = rows.filter((row) => normalizeStatus(row.status) === statusFilter);
  }

  if (bounds) {
    rows = rows.filter((row) => isBetween(row.event_date || "", bounds.start, bounds.end));
  }

  if (q) {
    rows = rows.filter((row) =>
      [row.customer_name, row.cust_code, row.territory, row.status]
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
      return normalizeStatus(a.status).localeCompare(normalizeStatus(b.status));
    }
    return String(a.event_date || "").localeCompare(String(b.event_date || ""));
  });

  return rows;
}

function isTypingTarget(target) {
  if (!target || !(target instanceof HTMLElement)) return false;
  const tag = target.tagName;
  return (
    tag === "INPUT" ||
    tag === "TEXTAREA" ||
    tag === "SELECT" ||
    target.isContentEditable ||
    Boolean(target.closest("[contenteditable='true']"))
  );
}

function useToasts() {
  const [toasts, setToasts] = useState([]);

  const dismiss = (id) => {
    setToasts((items) => items.filter((item) => item.id !== id));
  };

  const push = ({ type = "success", title = "", description = "" }) => {
    const id = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    setToasts((items) => [...items, { id, type, title, description }]);
    window.setTimeout(() => dismiss(id), 3600);
  };

  return { toasts, push, dismiss };
}

function ToastViewport({ toasts, onDismiss }) {
  if (!toasts.length) return null;

  return (
    <div className="pointer-events-none fixed right-4 top-4 z-[100] grid w-[min(92vw,360px)] gap-2">
      {toasts.map((toast) => (
        <div
          key={toast.id}
          className={cx(
            "pointer-events-auto rounded-xl border bg-white p-3 shadow-lg",
            toast.type === "error" ? "border-rose-300" : "border-emerald-300"
          )}
          role="status"
          aria-live="polite"
        >
          <div className="flex items-start gap-2">
            <span
              className={cx(
                "mt-0.5 inline-flex h-5 w-5 items-center justify-center rounded-full",
                toast.type === "error" ? "bg-rose-100 text-rose-700" : "bg-emerald-100 text-emerald-700"
              )}
            >
              {toast.type === "error" ? (AlertCircle ? <AlertCircle size={14} /> : "!") : Check ? <Check size={14} /> : "✓"}
            </span>
            <div className="min-w-0 flex-1">
              <p className="text-sm font-semibold text-slate-900">{toast.title}</p>
              {toast.description ? <p className="mt-0.5 text-sm text-slate-600">{toast.description}</p> : null}
            </div>
            <button
              type="button"
              aria-label="Dismiss notification"
              onClick={() => onDismiss(toast.id)}
              className="rounded-md p-1 text-slate-400 hover:bg-slate-100 hover:text-slate-700"
            >
              {X ? <X size={14} /> : "x"}
            </button>
          </div>
        </div>
      ))}
    </div>
  );
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
            ? "bg-emerald-50 text-emerald-800 ring-1 ring-emerald-200"
            : "text-slate-600 hover:bg-slate-100 hover:text-slate-900 active:bg-slate-200"
        )}
      >
        {item.label}
      </a>
    );
  });
}

function MobileNavSheet({ open, onClose }) {
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
      className={cx("fixed inset-0 z-50 transition", open ? "pointer-events-auto" : "pointer-events-none")}
      aria-hidden={!open}
    >
      <button
        type="button"
        aria-label="Close menu"
        onClick={onClose}
        className={cx("absolute inset-0 bg-slate-900/35 transition-opacity", open ? "opacity-100" : "opacity-0")}
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
              "inline-flex items-center justify-center gap-2 rounded-xl bg-emerald-500 px-3 text-sm font-semibold text-[#0b0f13] hover:bg-emerald-600",
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

function HeaderImportMenu() {
  const [open, setOpen] = useState(false);

  useEffect(() => {
    if (!open) return undefined;
    const onWindowClick = (event) => {
      const target = event.target;
      if (target instanceof HTMLElement && target.closest("[data-import-menu]")) return;
      setOpen(false);
    };
    window.addEventListener("click", onWindowClick);
    return () => window.removeEventListener("click", onWindowClick);
  }, [open]);

  return (
    <div className="relative hidden md:block" data-import-menu>
      <button
        type="button"
        aria-haspopup="menu"
        aria-expanded={open}
        onClick={() => setOpen((current) => !current)}
        className={cx(
          touchTarget(),
          "items-center gap-2 rounded-xl border border-slate-300 bg-white px-4 text-sm font-medium text-slate-700",
          "hidden md:inline-flex hover:bg-slate-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
        )}
      >
        {Upload ? <Upload size={16} /> : null}
        Import
        {ChevronDown ? <ChevronDown size={14} /> : null}
      </button>
      {open ? (
      <div className="absolute right-0 top-[calc(100%+8px)] z-50 grid min-w-[196px] rounded-xl border border-slate-200 bg-white p-1.5 shadow-xl">
        <a
          href="/import"
          onClick={() => setOpen(false)}
          className={cx(
            touchTarget(),
            "inline-flex items-center rounded-lg px-3 text-sm font-medium text-slate-700 hover:bg-slate-100",
            "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
          )}
        >
          Import Workbook
        </a>
        <a
          href="/import#summary"
          onClick={() => setOpen(false)}
          className={cx(
            touchTarget(),
            "inline-flex items-center rounded-lg px-3 text-sm font-medium text-slate-700 hover:bg-slate-100",
            "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
          )}
        >
          View Last Summary
        </a>
      </div>
      ) : null}
    </div>
  );
}

function AppHeader({ theme, onToggleTheme }) {
  const [menuOpen, setMenuOpen] = useState(false);

  return (
    <>
      <header className="sticky top-0 z-40 border-b border-slate-200/90 bg-white/85 backdrop-blur-md supports-[backdrop-filter]:bg-white/72">
        <div className="mx-auto flex w-full max-w-[var(--layout-max)] items-center gap-2 px-4 py-3 sm:px-6 lg:px-8">
          <a
            href="/"
            className={cx(
              touchTarget(),
              "inline-flex items-center gap-2 rounded-xl px-2 font-semibold tracking-[0.01em] text-slate-900",
              "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
            )}
            aria-label="Go to dashboard"
          >
            <span className="inline-flex h-8 w-8 items-center justify-center rounded-lg bg-emerald-500 text-white">
              {LayoutDashboard ? <LayoutDashboard size={16} /> : "P"}
            </span>
            <span className="hidden sm:inline">Planner</span>
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

          <div className="ml-auto flex items-center gap-2">
            <HeaderImportMenu />
            <a
              href="/cvm"
              className={cx(
                touchTarget(),
                "inline-flex items-center gap-2 rounded-xl bg-emerald-500 px-4 text-sm font-semibold text-[#0b0f13]",
                "hover:bg-emerald-600 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
              )}
            >
              {Plus ? <Plus size={16} /> : null}
              New Visit
            </a>
            <button
              type="button"
              onClick={onToggleTheme}
              aria-label="Toggle theme"
              title={theme === "dark" ? "Switch to light mode" : "Switch to dark mode"}
              className={cx(
                touchTarget(),
                "inline-flex w-11 items-center justify-center rounded-xl border border-slate-300 bg-white text-slate-700",
                "hover:bg-slate-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
              )}
            >
              {theme === "dark" ? (Sun ? <Sun size={17} /> : "☀") : Moon ? <Moon size={17} /> : "☾"}
            </button>
            <button
              type="button"
              aria-label="User menu placeholder"
              className={cx(
                touchTarget(),
                "hidden w-11 items-center justify-center rounded-full border border-slate-300 bg-white text-sm font-semibold text-slate-700 sm:inline-flex",
                "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
              )}
            >
              A
            </button>
          </div>
        </div>
      </header>

      <MobileNavSheet open={menuOpen} onClose={() => setMenuOpen(false)} />
    </>
  );
}

function AppShell({ children, theme, onToggleTheme }) {
  return (
    <div className="min-h-screen overflow-x-hidden bg-[var(--bg)] text-slate-900 antialiased">
      <AppHeader theme={theme} onToggleTheme={onToggleTheme} />
      <main className="mx-auto w-full max-w-[var(--layout-max)] space-y-6 px-4 py-6 sm:px-6 lg:px-8">{children}</main>
    </div>
  );
}

function PageHeader({ title, helper, right }) {
  return (
    <div className="mb-4 flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
      <div>
        <h1 className="text-[clamp(1.75rem,1.5rem+1vw,2rem)] font-semibold leading-tight text-slate-900">{title}</h1>
        {helper ? <p className="mt-1 text-sm text-slate-500">{helper}</p> : null}
      </div>
      {right ? <div>{right}</div> : null}
    </div>
  );
}

function CardSection({ children, className = "" }) {
  return <section className={cx("rounded-2xl border border-slate-200 bg-white p-5 shadow-sm", className)}>{children}</section>;
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

function StatCard({ href, icon: Icon, label, value, helper, trend = "Stable" }) {
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
        <span className="inline-flex h-11 w-11 items-center justify-center rounded-xl bg-slate-100 text-slate-700 transition group-hover:bg-emerald-100 group-hover:text-emerald-700">
          {Icon ? <Icon size={18} /> : null}
        </span>
      </div>
      <div className="mt-3 flex items-center justify-between gap-2">
        <p className="text-sm text-slate-500">{helper}</p>
        <span className="inline-flex min-h-7 items-center gap-1 rounded-full border border-slate-200 bg-slate-50 px-2.5 text-xs font-semibold text-slate-600">
          {trend === "Rising" ? (
            TrendingUp ? <TrendingUp size={12} /> : "↗"
          ) : Minus ? (
            <Minus size={12} />
          ) : (
            "•"
          )}
          {trend}
        </span>
      </div>
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
          "mt-4 inline-flex items-center gap-2 rounded-xl bg-emerald-500 px-4 text-sm font-medium text-[#0b0f13] hover:bg-emerald-600",
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
  const normalized = normalizeStatus(status);
  const styles =
    normalized === "completed"
      ? "bg-emerald-50 text-emerald-700 ring-emerald-200"
      : normalized === "cancelled"
        ? "bg-rose-50 text-rose-700 ring-rose-200"
        : "bg-slate-100 text-slate-700 ring-slate-200";

  return (
    <span className={cx("inline-flex min-h-7 items-center rounded-full px-3 text-xs font-semibold ring-1 ring-inset", styles)}>
      {normalized === "completed" ? "Completed" : normalized === "cancelled" ? "Cancelled" : "Planned"}
    </span>
  );
}

function loadExportSettings() {
  const defaults = {
    reportType: "visit_detail",
    format: "csv",
    rangeMode: "use-current",
    customStart: "",
    customEnd: "",
    includeLogo: true,
    includeSummary: true,
  };

  try {
    const raw = window.localStorage.getItem("planner.export.settings.v1");
    if (!raw) return defaults;
    const parsed = JSON.parse(raw);
    return {
      ...defaults,
      ...parsed,
    };
  } catch (_) {
    return defaults;
  }
}

function ExportReportDialog({
  open,
  onOpenChange,
  events,
  filteredEvents,
  currentFilters,
  onToast,
}) {
  const exportService = window.ExportService;
  const [activeTab, setActiveTab] = useState("settings");
  const [isExporting, setIsExporting] = useState(false);
  const [settings, setSettings] = useState(() => loadExportSettings());
  const [selectedColumns, setSelectedColumns] = useState([]);

  useEffect(() => {
    if (!open) return;
    const onEscape = (event) => {
      if (event.key === "Escape") onOpenChange(false);
    };
    window.addEventListener("keydown", onEscape);
    return () => window.removeEventListener("keydown", onEscape);
  }, [open, onOpenChange]);

  useEffect(() => {
    window.localStorage.setItem("planner.export.settings.v1", JSON.stringify(settings));
  }, [settings]);

  const filteredForExport = useMemo(() => {
    if (settings.rangeMode === "use-current") {
      return filteredEvents;
    }

    const overrideFilters = {
      ...currentFilters,
      rangePreset: settings.rangeMode,
      customStart: settings.customStart,
      customEnd: settings.customEnd,
    };

    return filterAndSortEvents(events, overrideFilters);
  }, [settings.rangeMode, settings.customStart, settings.customEnd, filteredEvents, currentFilters, events]);

  const reportRows = useMemo(() => {
    if (!exportService) return [];
    return exportService.buildReportRows(settings.reportType, filteredForExport);
  }, [exportService, settings.reportType, filteredForExport]);

  const availableColumns = useMemo(() => {
    if (!exportService) return [];
    return exportService.getAvailableColumns(reportRows);
  }, [exportService, reportRows]);

  const defaultColumns = useMemo(() => {
    if (!exportService) return [];
    return exportService.getDefaultColumns(settings.reportType, availableColumns);
  }, [exportService, settings.reportType, availableColumns]);

  useEffect(() => {
    setSelectedColumns((current) => {
      const valid = current.filter((column) => availableColumns.includes(column));
      if (valid.length) return valid;
      return [...defaultColumns];
    });
  }, [availableColumns, defaultColumns]);

  const recordCount = filteredForExport.length;
  const reportLabel = exportService ? exportService.getReportLabel(settings.reportType) : "Report";
  const rangeText =
    settings.rangeMode === "use-current"
      ? `Current filter (${rangeLabel(currentFilters.rangePreset, currentFilters.customStart, currentFilters.customEnd)})`
      : rangeLabel(settings.rangeMode, settings.customStart, settings.customEnd);

  const allSelected = availableColumns.length > 0 && selectedColumns.length === availableColumns.length;

  const updateSettings = (next) => {
    setSettings((current) => ({ ...current, ...next }));
  };

  const toggleColumn = (column) => {
    setSelectedColumns((current) =>
      current.includes(column) ? current.filter((item) => item !== column) : [...current, column]
    );
  };

  const runExport = async () => {
    if (!exportService) {
      onToast({
        type: "error",
        title: "Export unavailable",
        description: "Export service is not loaded yet.",
      });
      return;
    }

    setIsExporting(true);

    try {
      await new Promise((resolve) => setTimeout(resolve, 240));

      const generatedLabel = new Date().toLocaleString(undefined, {
        year: "numeric",
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      });

      const result = exportService.exportReport({
        reportType: settings.reportType,
        format: settings.format,
        rows: filteredForExport,
        selectedColumns,
        includeLogo: settings.includeLogo,
        includeSummary: settings.includeSummary,
        dateRangeLabel: rangeText,
        generatedLabel,
      });

      onToast({
        type: "success",
        title: "Export complete",
        description: `${result.title} downloaded as ${settings.format.toUpperCase()}.`,
      });

      onOpenChange(false);
    } catch (error) {
      onToast({
        type: "error",
        title: "Export failed",
        description: error?.message || "Unexpected export error.",
      });
    } finally {
      setIsExporting(false);
    }
  };

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <button
        type="button"
        aria-label="Close export dialog"
        className="absolute inset-0 bg-slate-900/45"
        onClick={() => onOpenChange(false)}
      />

      <section
        role="dialog"
        aria-modal="true"
        aria-label="Export report"
        className="relative z-10 w-[min(96vw,900px)] max-h-[92vh] overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-2xl"
      >
        <header className="flex items-center justify-between border-b border-slate-200 px-5 py-4">
          <div>
            <h3 className="text-lg font-semibold text-slate-900">Export / Reporting</h3>
            <p className="text-sm text-slate-500">{reportLabel} · Exporting {recordCount} records</p>
          </div>
          <button
            type="button"
            aria-label="Close export dialog"
            onClick={() => onOpenChange(false)}
            className={cx(
              touchTarget(),
              "inline-flex w-11 items-center justify-center rounded-lg border border-slate-200 text-slate-600 hover:bg-slate-100",
              "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
            )}
          >
            {X ? <X size={18} /> : "X"}
          </button>
        </header>

        <div className="border-b border-slate-200 px-5 pt-3">
          <div className="inline-flex rounded-xl bg-slate-100 p-1">
            <button
              type="button"
              onClick={() => setActiveTab("settings")}
              className={cx(
                touchTarget(),
                "rounded-lg px-3 text-sm font-medium",
                activeTab === "settings" ? "bg-white text-slate-900 shadow" : "text-slate-600"
              )}
            >
              Settings
            </button>
            <button
              type="button"
              onClick={() => setActiveTab("columns")}
              className={cx(
                touchTarget(),
                "rounded-lg px-3 text-sm font-medium",
                activeTab === "columns" ? "bg-white text-slate-900 shadow" : "text-slate-600"
              )}
            >
              Columns
            </button>
          </div>
        </div>

        <div className="max-h-[62vh] overflow-auto p-5" style={{ WebkitOverflowScrolling: "touch" }}>
          {activeTab === "settings" ? (
            <div className="grid gap-5">
              <div className="grid gap-2">
                <label className="text-sm font-semibold text-slate-700">Report Type</label>
                <select
                  value={settings.reportType}
                  onChange={(event) => updateSettings({ reportType: event.target.value })}
                  className={cx(
                    touchTarget(),
                    "w-full rounded-xl border border-slate-300 bg-white px-3 text-base",
                    "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
                  )}
                >
                  {exportReportTypes.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </div>

              <div className="grid gap-2">
                <p className="text-sm font-semibold text-slate-700">Export Format</p>
                <div className="grid gap-2 sm:grid-cols-3">
                  {exportFormats.map((option) => (
                    <label
                      key={option.value}
                      className={cx(
                        touchTarget(),
                        "flex cursor-pointer items-center gap-2 rounded-xl border px-3",
                        settings.format === option.value
                          ? "border-emerald-300 bg-emerald-50 text-emerald-800"
                          : "border-slate-300 bg-white text-slate-700"
                      )}
                    >
                      <input
                        type="radio"
                        name="export-format"
                        value={option.value}
                        checked={settings.format === option.value}
                        onChange={(event) => updateSettings({ format: event.target.value })}
                      />
                      <span className="text-sm font-medium">{option.label}</span>
                    </label>
                  ))}
                </div>
              </div>

              <div className="grid gap-2">
                <p className="text-sm font-semibold text-slate-700">Date Range</p>
                <div className="grid gap-2 sm:grid-cols-2">
                  {[
                    { value: "use-current", label: "Use current filter" },
                    { value: "today", label: "Today" },
                    { value: "7", label: "7 days" },
                    { value: "30", label: "30 days" },
                    { value: "custom", label: "Custom" },
                  ].map((option) => (
                    <label
                      key={option.value}
                      className={cx(
                        touchTarget(),
                        "flex cursor-pointer items-center gap-2 rounded-xl border px-3",
                        settings.rangeMode === option.value
                          ? "border-emerald-300 bg-emerald-50 text-emerald-800"
                          : "border-slate-300 bg-white text-slate-700"
                      )}
                    >
                      <input
                        type="radio"
                        name="export-range"
                        value={option.value}
                        checked={settings.rangeMode === option.value}
                        onChange={(event) => updateSettings({ rangeMode: event.target.value })}
                      />
                      <span className="text-sm font-medium">{option.label}</span>
                    </label>
                  ))}
                </div>

                {settings.rangeMode === "custom" ? (
                  <div className="grid gap-2 sm:grid-cols-2">
                    <label className="grid gap-1">
                      <span className="text-xs font-medium uppercase tracking-wide text-slate-500">Start</span>
                      <input
                        type="date"
                        value={settings.customStart}
                        onChange={(event) => updateSettings({ customStart: event.target.value })}
                        className={cx(
                          touchTarget(),
                          "rounded-xl border border-slate-300 px-3 text-base",
                          "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
                        )}
                      />
                    </label>
                    <label className="grid gap-1">
                      <span className="text-xs font-medium uppercase tracking-wide text-slate-500">End</span>
                      <input
                        type="date"
                        value={settings.customEnd}
                        onChange={(event) => updateSettings({ customEnd: event.target.value })}
                        className={cx(
                          touchTarget(),
                          "rounded-xl border border-slate-300 px-3 text-base",
                          "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
                        )}
                      />
                    </label>
                  </div>
                ) : null}

                <p className="text-xs text-slate-500">Using: {rangeText}</p>
              </div>

              <div className="grid gap-2 sm:grid-cols-2">
                <label className={cx(touchTarget(), "flex items-center gap-2 rounded-xl border border-slate-300 bg-white px-3")}> 
                  <input
                    type="checkbox"
                    checked={settings.includeLogo}
                    onChange={(event) => updateSettings({ includeLogo: event.target.checked })}
                  />
                  <span className="text-sm font-medium text-slate-700">Include logo</span>
                </label>

                <label className={cx(touchTarget(), "flex items-center gap-2 rounded-xl border border-slate-300 bg-white px-3")}> 
                  <input
                    type="checkbox"
                    checked={settings.includeSummary}
                    onChange={(event) => updateSettings({ includeSummary: event.target.checked })}
                  />
                  <span className="text-sm font-medium text-slate-700">Include summary metrics</span>
                </label>
              </div>
            </div>
          ) : (
            <div className="grid gap-3">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <p className="text-sm font-semibold text-slate-700">
                  Columns ({selectedColumns.length}/{availableColumns.length})
                </p>
                <div className="flex flex-wrap gap-2">
                  <button
                    type="button"
                    onClick={() => setSelectedColumns([...availableColumns])}
                    className={cx(
                      touchTarget(),
                      "rounded-xl border border-slate-300 bg-white px-3 text-sm font-medium text-slate-700",
                      "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
                    )}
                  >
                    Select All
                  </button>
                  <button
                    type="button"
                    onClick={() => setSelectedColumns([...defaultColumns])}
                    className={cx(
                      touchTarget(),
                      "rounded-xl border border-slate-300 bg-white px-3 text-sm font-medium text-slate-700",
                      "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
                    )}
                  >
                    Reset Default
                  </button>
                </div>
              </div>

              <label className={cx(touchTarget(), "flex items-center gap-2 rounded-xl border border-slate-300 bg-white px-3")}> 
                <input
                  type="checkbox"
                  checked={allSelected}
                  onChange={(event) =>
                    setSelectedColumns(event.target.checked ? [...availableColumns] : [...defaultColumns])
                  }
                />
                <span className="text-sm font-medium text-slate-700">Select all available fields</span>
              </label>

              <div className="grid gap-2 sm:grid-cols-2">
                {availableColumns.map((column) => (
                  <label
                    key={column}
                    className={cx(
                      touchTarget(),
                      "flex items-center gap-2 rounded-xl border px-3",
                      selectedColumns.includes(column)
                        ? "border-emerald-300 bg-emerald-50 text-emerald-800"
                        : "border-slate-300 bg-white text-slate-700"
                    )}
                  >
                    <input
                      type="checkbox"
                      checked={selectedColumns.includes(column)}
                      onChange={() => toggleColumn(column)}
                    />
                    <span className="text-sm font-medium">
                      {window.ExportService ? window.ExportService.toTitleCase(column) : column}
                    </span>
                  </label>
                ))}
              </div>
            </div>
          )}
        </div>

        <footer className="flex flex-wrap items-center justify-between gap-3 border-t border-slate-200 px-5 py-4">
          <p className="text-xs text-slate-500">Exporting {recordCount} records</p>
          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              onClick={() => onOpenChange(false)}
              className={cx(
                touchTarget(),
                "rounded-xl border border-slate-300 bg-white px-4 text-sm font-medium text-slate-700",
                "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]"
              )}
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={runExport}
              disabled={isExporting || recordCount === 0 || selectedColumns.length === 0}
              className={cx(
                touchTarget(),
                "inline-flex items-center gap-2 rounded-xl bg-emerald-500 px-4 text-sm font-semibold text-[#0b0f13]",
                "hover:bg-emerald-600 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]",
                "disabled:cursor-not-allowed disabled:opacity-60"
              )}
            >
              {isExporting ? (
                <>
                  <span className="inline-block h-4 w-4 animate-spin rounded-full border-2 border-white/30 border-t-white" />
                  Generating...
                </>
              ) : (
                <>
                  {Download ? <Download size={16} /> : null}
                  Export {settings.format.toUpperCase()}
                </>
              )}
            </button>
          </div>
        </footer>
      </section>
    </div>
  );
}

function VisitsTrendChart({ series, loading, theme }) {
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
    <CardSection>
      <SectionHeader
        title="Visits Over 12 Months"
        helper="Planned vs completed from CVM entries"
        right={
          <span className="inline-flex items-center gap-3 text-xs text-slate-500">
            <span className="inline-flex items-center gap-1.5">
              <span className="h-2.5 w-2.5 rounded-full bg-slate-400" /> Planned
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
        <div
          className="overflow-x-auto rounded-xl border border-slate-100 bg-slate-50/70 p-2 shadow-inner"
          style={{ WebkitOverflowScrolling: "touch" }}
        >
          <svg
            viewBox={`0 0 ${points.width} ${points.height}`}
            className="h-56 w-full min-w-[580px]"
            aria-label="Planned vs completed visits over 12 months"
            role="img"
          >
            {points.ticks.map((tick, idx) => (
              <g key={idx}>
                <line
                  x1="34"
                  x2={points.width - 34}
                  y1={tick.y}
                  y2={tick.y}
                  stroke={theme === "dark" ? "rgba(255,255,255,0.12)" : "#e2e8f0"}
                  strokeDasharray="4 4"
                />
                <text x="6" y={tick.y + 4} fill={theme === "dark" ? "#9aa3b2" : "#64748b"} fontSize="11">
                  {tick.value}
                </text>
              </g>
            ))}
            <polyline fill="none" stroke={theme === "dark" ? "#8a94a8" : "#64748b"} strokeWidth="3" points={points.plannedLine} />
            <polyline fill="none" stroke="#22c55e" strokeWidth="3" points={points.completedLine} />
            {points.safe.map((item, idx) => {
              const x = 34 + (idx * (points.width - 68)) / Math.max(1, points.safe.length - 1);
              const showLabel = idx % 2 === 0 || idx === points.safe.length - 1;
              return (
                <g key={item.label + idx}>
                  {showLabel ? (
                    <text x={x} y={points.height - 6} textAnchor="middle" fill={theme === "dark" ? "#9aa3b2" : "#64748b"} fontSize="11">
                      {item.label}
                    </text>
                  ) : null}
                </g>
              );
            })}
          </svg>
        </div>
      )}
    </CardSection>
  );
}

function UpcomingEventsTable({ events, loading, onToast }) {
  const [query, setQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [rangePreset, setRangePreset] = useState("7");
  const [sortBy, setSortBy] = useState("date-asc");
  const [exportOpen, setExportOpen] = useState(false);

  const currentFilters = useMemo(
    () => ({
      query,
      statusFilter,
      rangePreset,
      sortBy,
      customStart: "",
      customEnd: "",
    }),
    [query, statusFilter, rangePreset, sortBy]
  );

  const filtered = useMemo(() => filterAndSortEvents(events, currentFilters), [events, currentFilters]);

  useEffect(() => {
    const onKeyDown = (event) => {
      if (event.defaultPrevented) return;
      if (event.key.toLowerCase() !== "e") return;
      if (isTypingTarget(event.target)) return;
      event.preventDefault();
      setExportOpen(true);
    };

    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, []);

  return (
    <>
      <CardSection>
        <SectionHeader
          title="Upcoming Events"
          helper="Search and filter upcoming planned/completed visits"
          right={
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-xs text-slate-500">{filtered.length} results</span>
              <button
                type="button"
                onClick={() => setExportOpen(true)}
                disabled={loading}
                aria-label="Open export dialog"
                className={cx(
                  touchTarget(),
                  "inline-flex items-center gap-2 rounded-xl border border-slate-300 bg-white px-3 text-sm font-medium text-slate-700",
                  "hover:bg-slate-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)]",
                  "disabled:cursor-not-allowed disabled:opacity-60"
                )}
              >
                {Download ? <Download size={16} /> : null}
                Export
              </button>
            </div>
          }
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
            <thead className="sticky top-0 z-[1] bg-slate-50">
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
      </CardSection>

      <ExportReportDialog
        open={exportOpen}
        onOpenChange={setExportOpen}
        events={events}
        filteredEvents={filtered}
        currentFilters={currentFilters}
        onToast={onToast}
      />
    </>
  );
}

function DashboardApp({ data }) {
  const [loading, setLoading] = useState(false);
  const { toasts, push, dismiss } = useToasts();
  const { theme, toggleTheme } = useThemePreference();

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    if (!params.has("debug_overflow")) return undefined;

    const scanOverflow = () => {
      const viewport = document.documentElement.clientWidth;
      const offenders = [];
      document.querySelectorAll("body *").forEach((el) => {
        const rect = el.getBoundingClientRect();
        if (rect.width > viewport + 1 || rect.right > viewport + 1 || rect.left < -1) {
          offenders.push({
            node: el.tagName + (el.className ? `.${String(el.className).replace(/\s+/g, ".")}` : ""),
            width: Math.round(rect.width),
            left: Math.round(rect.left),
            right: Math.round(rect.right),
          });
        }
      });
      if (offenders.length) {
        console.group("Overflow debug");
        console.table(offenders.slice(0, 24));
        console.groupEnd();
      } else {
        console.log("Overflow debug: no wide elements found.");
      }
    };

    scanOverflow();
    window.addEventListener("resize", scanOverflow);
    return () => window.removeEventListener("resize", scanOverflow);
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
      trend: (counts.customers || 0) > 0 ? "Rising" : "Stable",
      href: "/customers",
      icon: Users,
    },
    {
      label: "Visit Entries",
      value: counts.cvm_entries ?? 0,
      helper: "tracked in CVM",
      trend: (counts.cvm_entries || 0) > 0 ? "Rising" : "Stable",
      href: "/cvm",
      icon: CalendarCheck2,
    },
    {
      label: "Calendar Year",
      value: settings.calendar_year ?? "-",
      helper: `week starts ${settings.week_start_day || "monday"}`,
      trend: "Stable",
      href: "/calendar",
      icon: CalendarDays,
    },
  ];

  return (
    <AppShell theme={theme} onToggleTheme={toggleTheme}>
      <CardSection>
        <PageHeader
          title="Planner Dashboard"
          helper="Track planning progress, upcoming visits, and activity at a glance."
          right={
            <span className="inline-flex items-center gap-2 text-sm text-slate-500">
              {Clock3 ? <Clock3 size={16} /> : null}
              {formatDate(data.today)}
            </span>
          }
        />

        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {loading
            ? Array.from({ length: 3 }).map((_, i) => (
                <div key={i} className="rounded-2xl border border-slate-200 bg-white p-4">
                  <Skeleton className="h-4 w-24" />
                  <Skeleton className="mt-3 h-9 w-24" />
                  <Skeleton className="mt-4 h-4 w-32" />
                </div>
              ))
            : statCards.map((card) => <StatCard key={card.label} {...card} />)}
        </div>
      </CardSection>

      <div className="grid gap-6 md:grid-cols-12">
        <div className="md:col-span-12 lg:col-span-5">
          <VisitsTrendChart series={visitsByMonth} loading={loading} theme={theme} />
        </div>
        <div className="md:col-span-12 lg:col-span-7">
          <UpcomingEventsTable events={upcoming} loading={loading} onToast={push} />
        </div>
      </div>

      <ToastViewport toasts={toasts} onDismiss={dismiss} />
    </AppShell>
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

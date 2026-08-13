import React from "react";
import ReactDOM from "react-dom/client";
import {
  Activity,
  AlertTriangle,
  ArrowDown,
  ArrowDownUp,
  ArrowUp,
  Bell,
  Check,
  ChevronDown,
  Clock3,
  Download,
  Gauge,
  LayoutDashboard,
  Loader2,
  Lock,
  LogOut,
  RefreshCcw,
  Search,
  Settings,
  ShieldCheck,
  SlidersHorizontal,
  Sparkles,
  Table2,
  X
} from "lucide-react";
import "./styles.css";

type AlertSection = "NOT_LIVE" | "STOPPED_IMPRESSIONS" | "MISSING_OUR_REF" | "ENDED_BUT_IMPRESSIONS";
type PacingSection = "UNDERPACING";
type Page = "margin" | "pacing:underpacing" | "alerts:not_live" | "alerts:stopped_impressions" | "alerts:missing_our_ref" | "alerts:ended_but_impressions" | "trafficking" | "automation" | "admin";
type ApiEnvelope<T> = { rows: T[]; meta: Record<string, string> };
type AnyRow = Record<string, string | number | null>;
type ApiRefreshOptions = { silent?: boolean };
type RowUpdater = (rows: AnyRow[]) => AnyRow[];
type SortDirection = "asc" | "desc";
type SortState = { key: string; direction: SortDirection } | null;
type UserInfo = { username: string; displayName: string };
type AutomationDefaults = {
  campaign_alert: {
    recipients: string;
    subject: string;
    dashboard_base_url: string;
    script: string;
  };
  daily_trafficking: {
    report_email_to: string;
    dry_run_mode: string;
    script: string;
  };
};
type AutomationResult = {
  status: string;
  exit_code: number;
  output: string;
  started_at: string;
  finished_at: string;
};

const TOKEN_KEY = "acquire_ops_token";
const ADMIN_USERNAME = "ashwin@acquirenz.com";
const DELIVERY_PACING_HELP = "Delivery pacing shows how the campaign is doing against expected delivery. A delivery pacing of 50% means that the campaign is only delivery 50% of the goal impressions/clicks/views that it should be delivering to hit the target. Delivery pacing should be above 100% to hit the campaign goal";
const ALERT_SECTIONS: Array<{ key: AlertSection; page: Page; label: string }> = [
  { key: "NOT_LIVE", page: "alerts:not_live", label: "Not live" },
  { key: "STOPPED_IMPRESSIONS", page: "alerts:stopped_impressions", label: "Stopped impressions" },
  { key: "MISSING_OUR_REF", page: "alerts:missing_our_ref", label: "Missing OUR_REF" },
  { key: "ENDED_BUT_IMPRESSIONS", page: "alerts:ended_but_impressions", label: "Ended but impressions" }
];
const PACING_SECTIONS: Array<{ key: PacingSection; page: Page; label: string }> = [
  { key: "UNDERPACING", page: "pacing:underpacing", label: "Underpacing" }
];
const ALERT_PAGE_SIZE = 100;
const EMPTY_ALERT_COUNTS: Record<AlertSection, number> = {
  NOT_LIVE: 0,
  STOPPED_IMPRESSIONS: 0,
  MISSING_OUR_REF: 0,
  ENDED_BUT_IMPRESSIONS: 0
};
const API_CACHE = new Map<string, ApiEnvelope<AnyRow>>();

class ApiError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

function currency(value: unknown) {
  const number = Number(value || 0);
  return new Intl.NumberFormat("en-NZ", {
    style: "currency",
    currency: "NZD",
    maximumFractionDigits: 0
  }).format(number);
}

function pct(value: unknown) {
  if (value === null || value === undefined || value === "") return "N/A";
  return `${(Number(value) * 100).toFixed(1)}%`;
}

function wholePct(value: unknown) {
  if (value === null || value === undefined || value === "") return "N/A";
  return `${Math.round(Number(value) * 100)}%`;
}

function num(value: unknown) {
  return new Intl.NumberFormat("en-NZ").format(Number(value || 0));
}

function downloadCsv(filename: string, rows: AnyRow[]) {
  if (!rows.length) return;
  const headers = Object.keys(rows[0]);
  const csv = [
    headers.join(","),
    ...rows.map((row) =>
      headers
        .map((header) => {
          const raw = row[header] ?? "";
          return `"${String(raw).replaceAll('"', '""')}"`;
        })
        .join(",")
    )
  ].join("\n");
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

async function apiFetch<T>(path: string, options: RequestInit = {}): Promise<T> {
  const token = localStorage.getItem(TOKEN_KEY);
  const headers = new Headers(options.headers);
  headers.set("Content-Type", "application/json");
  if (token) headers.set("Authorization", `Bearer ${token}`);

  const response = await fetch(path, { ...options, headers });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new ApiError(body.detail || `Request failed: ${response.status}`, response.status);
  }
  return response.json() as Promise<T>;
}

function invalidateApiCache(prefix: string) {
  for (const key of API_CACHE.keys()) {
    if (key.startsWith(prefix)) API_CACHE.delete(key);
  }
}

function pageToAlertType(page: Page): AlertSection | null {
  if (page === "alerts:not_live") return "NOT_LIVE";
  if (page === "alerts:stopped_impressions") return "STOPPED_IMPRESSIONS";
  if (page === "alerts:missing_our_ref") return "MISSING_OUR_REF";
  if (page === "alerts:ended_but_impressions") return "ENDED_BUT_IMPRESSIONS";
  return null;
}

function pageToPacingType(page: Page): PacingSection | null {
  if (page === "pacing:underpacing") return "UNDERPACING";
  return null;
}

function buildAlertsPath(alertType: AlertSection, status: string, query: string, page: number) {
  const params = new URLSearchParams({
    alert_type: alertType,
    state: status,
    page: String(page),
    page_size: String(ALERT_PAGE_SIZE)
  });
  if (query.trim()) params.set("query", query.trim());
  return `/api/alerts?${params.toString()}`;
}

function useApiRows<T extends AnyRow>(path: string, enabled = true) {
  const [rows, setRows] = React.useState<T[]>([]);
  const [meta, setMeta] = React.useState<Record<string, string>>({});
  const [loading, setLoading] = React.useState(enabled);
  const [hasLoaded, setHasLoaded] = React.useState(false);
  const hasLoadedRef = React.useRef(false);
  const [error, setError] = React.useState("");
  const refresh = React.useCallback(async () => {
    if (!enabled) {
      setRows([]);
      setMeta({});
      setLoading(false);
      hasLoadedRef.current = false;
      setHasLoaded(false);
      setError("");
      return;
    }
    setLoading(!hasLoadedRef.current);
    setError("");
    try {
      const data = await apiFetch<ApiEnvelope<T>>(path);
      API_CACHE.set(path, data as ApiEnvelope<AnyRow>);
      setRows(data.rows);
      setMeta(data.meta || {});
      hasLoadedRef.current = true;
      setHasLoaded(true);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setLoading(false);
    }
  }, [enabled, path]);

  React.useEffect(() => {
    if (!enabled) {
      setRows([]);
      setMeta({});
      setLoading(false);
      hasLoadedRef.current = false;
      setHasLoaded(false);
      setError("");
      return;
    }
    const cached = API_CACHE.get(path) as ApiEnvelope<T> | undefined;
    if (cached) {
      setRows(cached.rows);
      setMeta(cached.meta || {});
      setLoading(false);
      hasLoadedRef.current = true;
      setHasLoaded(true);
      setError("");
      return;
    }
    void refresh();
  }, [enabled, path, refresh]);

  return { rows, meta, loading, hasLoaded, error, refresh };
}

function useAlertsBootstrap(enabled: boolean) {
  const [rows, setRows] = React.useState<AnyRow[]>([]);
  const [meta, setMeta] = React.useState<Record<string, string>>({});
  const [loading, setLoading] = React.useState(enabled);
  const [hasLoaded, setHasLoaded] = React.useState(false);
  const hasLoadedRef = React.useRef(false);
  const [error, setError] = React.useState("");
  const [lastLoadedAt, setLastLoadedAt] = React.useState("");

  const refresh = React.useCallback(async (options: ApiRefreshOptions = {}) => {
    if (!enabled) {
      setRows([]);
      setMeta({});
      setLoading(false);
      hasLoadedRef.current = false;
      setHasLoaded(false);
      setError("");
      setLastLoadedAt("");
      return;
    }
    if (!options.silent) setLoading(!hasLoadedRef.current);
    setError("");
    try {
      const data = await apiFetch<ApiEnvelope<AnyRow>>("/api/alerts/bootstrap");
      setRows(data.rows);
      setMeta(data.meta || {});
      hasLoadedRef.current = true;
      setHasLoaded(true);
      setLastLoadedAt(new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      if (!options.silent) setLoading(false);
    }
  }, [enabled]);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

  React.useEffect(() => {
    if (!enabled) return undefined;
    const interval = window.setInterval(() => {
      void refresh({ silent: true });
    }, 120_000);
    return () => window.clearInterval(interval);
  }, [enabled, refresh]);

  const updateRows = React.useCallback((updater: RowUpdater) => {
    setRows((currentRows) => updater(currentRows));
  }, []);

  return { rows, meta, loading, hasLoaded, error, refresh, updateRows, lastLoadedAt };
}

function normalizeAlertStatus(status: string) {
  return status === "SNOOZED" ? "ACTIVE" : status;
}

function isBlankSortValue(value: unknown) {
  return value === null || value === undefined || String(value).trim() === "";
}

function parseSortNumber(value: unknown) {
  const text = String(value).trim();
  if (!/^-?[$]?\d[\d,]*(\.\d+)?%?$/.test(text)) return null;
  const number = Number(text.replace(/[$,%]/g, ""));
  return Number.isFinite(number) ? number : null;
}

function parseSortDate(value: unknown) {
  const text = String(value).trim();
  if (!/^\d{4}-\d{2}-\d{2}/.test(text)) return null;
  const time = Date.parse(text);
  return Number.isFinite(time) ? time : null;
}

function compareSortValues(left: unknown, right: unknown) {
  const leftBlank = isBlankSortValue(left);
  const rightBlank = isBlankSortValue(right);
  if (leftBlank && rightBlank) return 0;
  if (leftBlank) return 1;
  if (rightBlank) return -1;

  const leftNumber = parseSortNumber(left);
  const rightNumber = parseSortNumber(right);
  if (leftNumber !== null && rightNumber !== null) return leftNumber - rightNumber;

  const leftDate = parseSortDate(left);
  const rightDate = parseSortDate(right);
  if (leftDate !== null && rightDate !== null) return leftDate - rightDate;

  return String(left).localeCompare(String(right), undefined, { numeric: true, sensitivity: "base" });
}

function sortRows(rows: AnyRow[], sort: SortState) {
  if (!sort) return rows;
  const direction = sort.direction === "asc" ? 1 : -1;
  return rows
    .map((row, index) => ({ row, index }))
    .sort((left, right) => {
      const leftBlank = isBlankSortValue(left.row[sort.key]);
      const rightBlank = isBlankSortValue(right.row[sort.key]);
      if (leftBlank && rightBlank) return left.index - right.index;
      if (leftBlank) return 1;
      if (rightBlank) return -1;
      const comparison = compareSortValues(left.row[sort.key], right.row[sort.key]);
      if (comparison !== 0) return comparison * direction;
      return left.index - right.index;
    })
    .map(({ row }) => row);
}

function nextSort(current: SortState, key: string): SortState {
  if (current?.key !== key) return { key, direction: "asc" };
  return { key, direction: current.direction === "asc" ? "desc" : "asc" };
}

function openAlertCounts(rows: AnyRow[]) {
  const counts: Record<AlertSection, number> = { ...EMPTY_ALERT_COUNTS };
  for (const row of rows) {
    const type = String(row.ALERT_TYPE || "") as AlertSection;
    if (type in counts && row.LIVE_ALERT_STATE === "OPEN") counts[type] += 1;
  }
  return counts;
}

function underpacingCounts(rows: AnyRow[]) {
  return {
    UNDERPACING: rows.filter((row) => row.PACING_BUCKET === "UNDERPACING").length
  };
}

function alertStateCounts(rows: AnyRow[]) {
  return {
    open: rows.filter((row) => row.LIVE_ALERT_STATE === "OPEN").length,
    snoozed: rows.filter((row) => row.LIVE_ALERT_STATE === "ACTIVE").length,
    dismissed: rows.filter((row) => row.LIVE_ALERT_STATE === "DISMISSED").length
  };
}

function App() {
  const [page, setPage] = React.useState<Page>("alerts:not_live");
  const [token, setToken] = React.useState(localStorage.getItem(TOKEN_KEY) || "");
  const [user, setUser] = React.useState<UserInfo | null>(null);
  const [alertsExpanded, setAlertsExpanded] = React.useState(true);
  const [pacingExpanded, setPacingExpanded] = React.useState(true);
  const alertsActive = page.startsWith("alerts:");
  const pacingActive = page.startsWith("pacing:");
  const activeAlertType = pageToAlertType(page);
  const activePacingType = pageToPacingType(page);
  const [alertQuery, setAlertQuery] = React.useState("");
  const [alertStatus, setAlertStatus] = React.useState("OPEN");
  const [alertPage, setAlertPage] = React.useState(1);
  const deferredAlertQuery = React.useDeferredValue(alertQuery);
  const alertsData = useAlertsBootstrap(Boolean(token && alertsActive));
  const pacingData = useApiRows<AnyRow>("/api/pacing", Boolean(token));
  const alertCounts = React.useMemo(() => openAlertCounts(alertsData.rows), [alertsData.rows]);
  const pacingCounts = React.useMemo(() => underpacingCounts(pacingData.rows), [pacingData.rows]);
  const totalOpenAlerts = React.useMemo(
    () => ALERT_SECTIONS.reduce((total, section) => total + (alertCounts[section.key] || 0), 0),
    [alertCounts]
  );
  const totalUnderpacing = React.useMemo(
    () => PACING_SECTIONS.reduce((total, section) => total + (pacingCounts[section.key] || 0), 0),
    [pacingCounts]
  );
  const isAdmin = user?.username.toLowerCase() === ADMIN_USERNAME;

  React.useEffect(() => {
    if (!token) return;
    apiFetch<{ display_name?: string; username?: string }>("/api/auth/me")
      .then((me) => {
        const username = me.username || "";
        setUser({ username, displayName: me.display_name || username || "User" });
      })
      .catch(() => {
        localStorage.removeItem(TOKEN_KEY);
        setToken("");
        setUser(null);
      });
  }, [token]);

  React.useEffect(() => {
    setAlertPage(1);
  }, [activeAlertType, alertStatus, deferredAlertQuery]);

  if (!token) {
    return <Login onLogin={(nextToken, nextUser) => {
      localStorage.setItem(TOKEN_KEY, nextToken);
      setToken(nextToken);
      setUser(nextUser);
    }} />;
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand-block">
          <div className="brand-mark"><Sparkles size={18} /></div>
          <div>
            <div className="brand-title">Acquire Ops</div>
            <div className="brand-subtitle">Trafficking control room</div>
          </div>
        </div>
        <nav className="nav-stack">
          <NavButton active={page === "margin"} icon={<Gauge size={18} />} label="Margin" onClick={() => setPage("margin")} />
          <div className={`nav-section ${pacingActive ? "active" : ""}`}>
            <button
              className={`nav-button nav-section-trigger ${pacingActive ? "active" : ""}`}
              onClick={() => {
                setPacingExpanded((current) => !current);
                if (!pacingActive) setPage("pacing:underpacing");
              }}
            >
              <Sparkles size={18} />
              <span>Pacing</span>
              <span className="nav-count nav-count-parent">{num(totalUnderpacing)}</span>
              <ChevronDown className={`nav-chevron ${pacingExpanded ? "open" : ""}`} size={16} />
            </button>
            {pacingExpanded && (
              <div className="nav-subsection">
                {PACING_SECTIONS.map((section) => (
                  <button
                    key={section.key}
                    className={`nav-subbutton ${page === section.page ? "active" : ""}`}
                    onClick={() => setPage(section.page)}
                  >
                    <span>{section.label}</span>
                    <span className="nav-count">{num(pacingCounts[section.key] || 0)}</span>
                  </button>
                ))}
              </div>
            )}
          </div>
          <div className={`nav-section ${alertsActive ? "active" : ""}`}>
            <button
              className={`nav-button nav-section-trigger ${alertsActive ? "active" : ""}`}
              onClick={() => {
                setAlertsExpanded((current) => !current);
                if (!alertsActive) setPage("alerts:not_live");
              }}
            >
              <Bell size={18} />
              <span>Alerts</span>
              <span className="nav-count nav-count-parent">{num(totalOpenAlerts)}</span>
              <ChevronDown className={`nav-chevron ${alertsExpanded ? "open" : ""}`} size={16} />
            </button>
            {alertsExpanded && (
              <div className="nav-subsection">
                {ALERT_SECTIONS.map((section) => (
                  <button
                    key={section.key}
                    className={`nav-subbutton ${page === section.page ? "active" : ""}`}
                    onClick={() => setPage(section.page)}
                  >
                    <span>{section.label}</span>
                    <span className="nav-count">{num(alertCounts[section.key] || 0)}</span>
                  </button>
                ))}
              </div>
            )}
          </div>
          <NavButton active={page === "trafficking"} icon={<Table2 size={18} />} label="Trafficking" onClick={() => setPage("trafficking")} />
          <NavButton active={page === "automation"} icon={<Activity size={18} />} label="Automation" onClick={() => setPage("automation")} />
          {isAdmin && <NavButton active={page === "admin"} icon={<Settings size={18} />} label="Admin" onClick={() => setPage("admin")} />}
        </nav>
        <div className="user-panel">
          <ShieldCheck size={16} />
          <span>{user?.displayName || "Signed in"}</span>
          <button className="icon-button" title="Sign out" onClick={() => {
            localStorage.removeItem(TOKEN_KEY);
            setToken("");
            setUser(null);
          }}>
            <LogOut size={16} />
          </button>
        </div>
      </aside>
      <main className="workspace">
        {page === "margin" && <MarginPage />}
        {page === "pacing:underpacing" && activePacingType && <PacingPage pacingType={activePacingType} {...pacingData} />}
        {page === "alerts:not_live" && activeAlertType && <AlertsPage alertType={activeAlertType} {...alertsData} query={alertQuery} setQuery={setAlertQuery} status={alertStatus} setStatus={setAlertStatus} page={alertPage} setPage={setAlertPage} />}
        {page === "alerts:stopped_impressions" && activeAlertType && <AlertsPage alertType={activeAlertType} {...alertsData} query={alertQuery} setQuery={setAlertQuery} status={alertStatus} setStatus={setAlertStatus} page={alertPage} setPage={setAlertPage} />}
        {page === "alerts:missing_our_ref" && activeAlertType && <AlertsPage alertType={activeAlertType} {...alertsData} query={alertQuery} setQuery={setAlertQuery} status={alertStatus} setStatus={setAlertStatus} page={alertPage} setPage={setAlertPage} />}
        {page === "alerts:ended_but_impressions" && activeAlertType && <AlertsPage alertType={activeAlertType} {...alertsData} query={alertQuery} setQuery={setAlertQuery} status={alertStatus} setStatus={setAlertStatus} page={alertPage} setPage={setAlertPage} />}
        {page === "trafficking" && <PlaceholderPage title="Trafficking to Asana" body="The next migration slice will bring the Gmail fetch, parse preview, Asana dedupe check, and dry-run downloads into this interface." />}
        {page === "automation" && <PlaceholderPage title="Automation Runs" body="Manual automation triggers should run as background jobs with live status and logs, instead of blocking the interface." />}
        {page === "admin" && <AdminPage isAdmin={isAdmin} />}
      </main>
    </div>
  );
}

function Login({ onLogin }: { onLogin: (token: string, user: UserInfo) => void }) {
  const [username, setUsername] = React.useState("");
  const [password, setPassword] = React.useState("");
  const [error, setError] = React.useState("");
  const [loading, setLoading] = React.useState(false);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setLoading(true);
    setError("");
    try {
      const response = await apiFetch<{ token: string; display_name: string; username: string }>("/api/auth/login", {
        method: "POST",
        body: JSON.stringify({ username, password })
      });
      onLogin(response.token, {
        username: response.username,
        displayName: response.display_name || response.username
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Login failed");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="login-screen">
      <form className="login-panel" onSubmit={submit}>
        <div className="login-mark"><Lock size={22} /></div>
        <h1>Acquire Ops</h1>
        <p>Fast dashboards for margin, alerts, and trafficking workflows.</p>
        <label>
          Username
          <input value={username} onChange={(event) => setUsername(event.target.value)} autoComplete="username" />
        </label>
        <label>
          Password
          <input value={password} onChange={(event) => setPassword(event.target.value)} type="password" autoComplete="current-password" />
        </label>
        {error && <div className="error-line">{error}</div>}
        <button className="primary-button" type="submit" disabled={loading}>
          {loading ? <Loader2 className="spin" size={16} /> : <ShieldCheck size={16} />}
          Sign in
        </button>
      </form>
    </div>
  );
}

function NavButton(props: { active: boolean; icon: React.ReactNode; label: string; onClick: () => void }) {
  return (
    <button className={`nav-button ${props.active ? "active" : ""}`} onClick={props.onClick}>
      {props.icon}
      <span>{props.label}</span>
    </button>
  );
}

function PageHeader(props: { eyebrow: string; title: string; subtitle: string; loading: boolean; onRefresh: () => void; onDownload?: () => void }) {
  return (
    <header className="page-header">
      <div>
        <div className="eyebrow">{props.eyebrow}</div>
        <h1>{props.title}</h1>
        <p>{props.subtitle}</p>
      </div>
      <div className="header-actions">
        {props.onDownload && <button className="ghost-button" onClick={props.onDownload}><Download size={16} /> CSV</button>}
        <button className="ghost-button" onClick={props.onRefresh} disabled={props.loading}>
          {props.loading ? <Loader2 className="spin" size={16} /> : <RefreshCcw size={16} />}
          Refresh
        </button>
      </div>
    </header>
  );
}

function MetricStrip({ metrics }: { metrics: Array<{ label: string; value: string; tone?: string }> }) {
  return (
    <section className="metric-strip">
      {metrics.map((metric) => (
        <div className={`metric ${metric.tone || ""}`} key={metric.label}>
          <span>{metric.label}</span>
          <strong>{metric.value}</strong>
        </div>
      ))}
    </section>
  );
}

function MarginPage() {
  const { rows, meta, loading, hasLoaded, error, refresh } = useApiRows<AnyRow>("/api/margin");
  const [query, setQuery] = React.useState("");
  const [status, setStatus] = React.useState("OPEN");
  const [selected, setSelected] = React.useState<Set<string>>(new Set());
  const [sort, setSort] = React.useState<SortState>(null);

  const filtered = React.useMemo(() => rows.filter((row) => {
    const text = `${row.OUR_REF} ${row.JOB_NUMBER} ${row.CAMPAIGN_NAME} ${row.ADVERTISER_NAME}`.toLowerCase();
    const stateMatch = status === "ALL" || row.MARGIN_SNOOZE_STATE === status;
    return stateMatch && text.includes(query.toLowerCase());
  }), [rows, query, status]);
  const sortedRows = React.useMemo(() => sortRows(filtered, sort), [filtered, sort]);

  const active = rows.filter((row) => row.MARGIN_SNOOZE_STATE === "OPEN");
  const selectedAlerts = rows
    .filter((row) => selected.has(String(row.OUR_REF)))
    .map((row) => ({
      alert_type: "MARGIN_DASHBOARD",
      alert_key: String(row.OUR_REF),
      our_ref: String(row.OUR_REF),
      state_version: String(row.STATE_VERSION || "")
    }));

  return (
    <>
      <PageHeader
        eyebrow={meta.view ? `${meta.project_id}.${meta.dataset}.${meta.view}` : "BigQuery margin view"}
        title="Margin Dashboard"
        subtitle="Live margin, pacing, budget, and snooze state at OUR REF level."
        loading={loading}
        onRefresh={refresh}
        onDownload={() => downloadCsv("margin-dashboard.csv", sortedRows)}
      />
      <MetricStrip metrics={[
        { label: "Active rows", value: num(active.length) },
        { label: "Active budget", value: currency(active.reduce((sum, row) => sum + Number(row.BUDGET || 0), 0)) },
        { label: "Actual nett spend", value: currency(active.reduce((sum, row) => sum + Number(row.ACTUAL_NETT_SPEND || 0), 0)) },
        { label: "Avg margin", value: pct(avg(active.map((row) => Number(row.MARGIN_PCT || 0)))) }
      ]} />
      <Toolbar query={query} setQuery={setQuery} status={status} setStatus={setStatus} statuses={["OPEN", "ACTIVE", "ALL"]} selectedCount={selected.size} />
      <DataState loading={loading && !hasLoaded} error={error} empty={!filtered.length}>
        <DataTable
          rows={sortedRows}
          selected={selected}
          setSelected={setSelected}
          idKey="OUR_REF"
          sort={sort}
          onSort={(key) => setSort((current) => nextSort(current, key))}
          columns={[
            ["OUR_REF", "OUR REF"],
            ["ADVERTISER_NAME", "Advertiser"],
            ["CAMPAIGN_NAME", "Campaign"],
            ["BOOKING_STATUS", "Booking"],
            ["BUDGET", "Budget"],
            ["ACTUAL_NETT_SPEND", "Spend"],
            ["MARGIN_AMOUNT", "Margin"],
            ["MARGIN_PCT", "Margin %"],
            ["PACING_RATIO", "Pace"],
            ["MARGIN_SNOOZE_STATE", "State"]
          ]}
          format={(key, value) => {
            if (["BUDGET", "ACTUAL_NETT_SPEND", "MARGIN_AMOUNT"].includes(key)) return currency(value);
            if (["MARGIN_PCT", "PACING_RATIO"].includes(key)) return pct(value);
            return String(value ?? "");
          }}
        />
      </DataState>
      <ActionDock selectedCount={selected.size} onClear={() => setSelected(new Set())}>
        <SnoozeButton endpoint="/api/margin/snooze" alerts={selectedAlerts} onDone={() => { setSelected(new Set()); void refresh(); }} />
      </ActionDock>
    </>
  );
}

function PacingPage(props: {
  pacingType: PacingSection;
  rows: AnyRow[];
  meta: Record<string, string>;
  loading: boolean;
  hasLoaded: boolean;
  error: string;
  refresh: () => Promise<void>;
}) {
  const [query, setQuery] = React.useState("");
  const [sort, setSort] = React.useState<SortState>({ key: "DELIVERY_PACING_RATIO", direction: "asc" });

  const filtered = React.useMemo(() => props.rows.filter((row) => {
    const text = `${row.OUR_REF} ${row.JOB_NUMBER} ${row.CAMPAIGN_NAME} ${row.ADVERTISER_NAME} ${row.ACCOUNT_MANAGER}`.toLowerCase();
    return row.PACING_BUCKET === props.pacingType && text.includes(query.toLowerCase());
  }), [props.pacingType, props.rows, query]);
  const sortedRows = React.useMemo(() => sortRows(filtered, sort), [filtered, sort]);

  const activeRows = filtered.filter((row) => Number(row.EXPECTED_DELIVERY_TO_DATE || 0) > 0);
  const totalExpected = activeRows.reduce((sum, row) => sum + Number(row.EXPECTED_DELIVERY_TO_DATE || 0), 0);
  const totalActual = activeRows.reduce((sum, row) => sum + Number(row.ACTUAL_DELIVERY || 0), 0);
  const underCount = filtered.filter((row) => row.PACING_STATUS === "UNDER").length;
  const blendedRatio = totalExpected > 0 ? totalActual / totalExpected : null;

  return (
    <>
      <PageHeader
        eyebrow={props.meta.source_table ? `${props.meta.project_id}.${props.meta.dataset}.${props.meta.source_table}` : "BigQuery pacing model"}
        title="Underpacing"
        subtitle="OUR_REF-level pacing, rolled up across data sources with one row per reference."
        loading={props.loading}
        onRefresh={props.refresh}
        onDownload={() => downloadCsv("pacing-dashboard.csv", sortedRows)}
      />
      <MetricStrip metrics={[
        { label: "Rows", value: num(filtered.length) },
        { label: "Expected delivery", value: num(Math.round(totalExpected)) },
        { label: "Actual delivery", value: num(Math.round(totalActual)) },
        { label: "Delivery vs expected", value: blendedRatio === null ? "N/A" : pct(blendedRatio) },
        { label: "Underpacing refs", value: num(underCount), tone: "warn" }
      ]} />
      <section className="toolbar">
        <div className="searchbox">
          <Search size={16} />
          <input
            name="pacing-table-search"
            placeholder="Search refs, jobs, campaigns, advertisers..."
            autoComplete="off"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
          />
        </div>
        <div className="selected-chip"><SlidersHorizontal size={15} /> {filtered.length} rows</div>
      </section>
      <DataState loading={props.loading && !props.hasLoaded} error={props.error} empty={!filtered.length}>
        <DataTable
          rows={sortedRows}
          selected={new Set()}
          setSelected={() => {}}
          idKey="OUR_REF"
          sort={sort}
          onSort={(key) => setSort((current) => nextSort(current, key))}
          columns={[
            ["OUR_REF", "OUR REF"],
            ["ADVERTISER_NAME", "Advertiser"],
            ["CAMPAIGN_NAME", "Campaign"],
            ["LOCATION_TEXT", "Line item"],
            ["__PROGRESS", "Progress"],
            ["DELIVERY_PACING_RATIO", "Delivery Pacing"],
            ["PROPERTY_NAME", "Property"],
            ["JOB_NUMBER", "Job number"],
            ["ACCOUNT_MANAGER", "AM"],
            ["START_DATE", "Start"],
            ["END_DATE", "End"],
            ["TIME_PROGRESS_RATIO", "Time progress"],
            ["GOAL_DELIVERY", "Goal delivery"],
            ["GOAL_TYPE", "Target type"],
            ["EXPECTED_DELIVERY_TO_DATE", "Expected delivery"],
            ["ACTUAL_DELIVERY", "Actual delivery"],
            ["DELIVERY_DELTA", "Short fall"],
            ["DATASOURCE", "Datasource"]
          ]}
          renderCell={(key, value, row) => {
            if (key !== "__PROGRESS") return null;
            return <ProgressCell timeProgress={Number(row.TIME_PROGRESS_RATIO || 0)} deliveryProgress={Number(row.ACTUAL_DELIVERY || 0) / Math.max(Number(row.GOAL_DELIVERY || 0), 1)} />;
          }}
          headerHelp={{ DELIVERY_PACING_RATIO: DELIVERY_PACING_HELP }}
          format={(key, value) => {
            if (["GOAL_DELIVERY", "EXPECTED_DELIVERY_TO_DATE", "ACTUAL_DELIVERY", "DELIVERY_DELTA"].includes(key)) return num(Math.round(Number(value || 0)));
            if (["TIME_PROGRESS_RATIO", "DELIVERY_PACING_RATIO"].includes(key)) return value === null || value === "" ? "N/A" : pct(value);
            return String(value ?? "");
          }}
        />
      </DataState>
    </>
  );
}

function AlertsPage(props: {
  alertType: AlertSection;
  rows: AnyRow[];
  meta: Record<string, string>;
  loading: boolean;
  hasLoaded: boolean;
  error: string;
  refresh: (options?: ApiRefreshOptions) => Promise<void>;
  updateRows: (updater: RowUpdater) => void;
  lastLoadedAt: string;
  query: string;
  setQuery: (value: string) => void;
  status: string;
  setStatus: (value: string) => void;
  page: number;
  setPage: (value: number) => void;
}) {
  const [selected, setSelected] = React.useState<Set<string>>(new Set());
  const [adminPass, setAdminPass] = React.useState("");
  const [actionError, setActionError] = React.useState("");
  const [sort, setSort] = React.useState<SortState>(null);
  const sectionMeta = ALERT_SECTIONS.find((section) => section.key === props.alertType)!;
  const snoozeDetailColumns: Array<[string, string]> = [
    ["ALERT_KEY", "ALERT_KEY"],
    ["ALERT_TYPE", "ALERT_TYPE"],
    ["OUR_REF", "OUR_REF"],
    ["SNOOZE_STATUS", "SNOOZE_STATUS"],
    ["SNOOZE_REASON", "SNOOZE_REASON"],
    ["SNOOZE_START_DATE", "SNOOZE_START_DATE"],
    ["SNOOZE_END_DATE", "SNOOZE_END_DATE"],
    ["SNOOZED_BY", "SNOOZED_BY"],
    ["DISMISSED_BY", "DISMISSED_BY"],
    ["UPDATED_AT", "UPDATED_AT"],
    ["ADVERTISER", "ADVERTISER"],
    ["CAMPAIGN", "CAMPAIGN"]
  ];
  const baseAlertColumns: Array<[string, string]> = [
    ["ALERT_TYPE", "ALERT_TYPE"],
    ["OUR_REF", "OUR_REF"],
    ["JOB_NUMBER", "JOB_NUMBER"],
    ["START_DATE", "START_DATE"],
    ["END_DATE", "END_DATE"],
    ["ADVERTISER", "ADVERTISER"],
    ["CAMPAIGN", "CAMPAIGN"],
    ["LOCATIONTEXT", "LOCATIONTEXT"],
    ["PROPERTYNAME", "PROPERTYNAME"],
    ["BOOKINGSTATUS", "BOOKINGSTATUS"]
  ];
  const deliveryAlertColumns: Array<[string, string]> = [
    ["DATASOURCE", "DATASOURCE"],
    ["ACCOUNT", "ACCOUNT"],
    ["FIRST_MISSING_DATE", "FIRST_MISSING_DATE"],
    ["LAST_MISSING_DATE", "LAST_MISSING_DATE"],
    ["TOTAL_IMPRESSIONS", "TOTAL_IMPRESSIONS"],
    ["TOTAL_CLICKS", "TOTAL_CLICKS"],
    ["TOTAL_COST", "TOTAL_COST"],
    ["ROW_COUNT", "ROW_COUNT"]
  ];
  const alertColumns = props.alertType === "NOT_LIVE" ? baseAlertColumns : [...baseAlertColumns, ...deliveryAlertColumns];
  const isSnoozeDetailView = props.status === "SNOOZED" || props.status === "DISMISSED";
  const apiSupportsFilteredAlerts = Number(props.meta.alert_api_version || 0) >= 3;

  React.useEffect(() => {
    setSelected(new Set());
    setActionError("");
  }, [props.status, props.alertType, props.page]);

  const scopedRows = React.useMemo(
    () => props.rows.filter((row) => row.ALERT_TYPE === props.alertType),
    [props.alertType, props.rows]
  );
  const scopedCounts = React.useMemo(() => alertStateCounts(scopedRows), [scopedRows]);
  const filtered = React.useMemo<AnyRow[]>(() => {
    const normalizedStatus = normalizeAlertStatus(props.status);
    let nextRows = scopedRows;
    if (normalizedStatus !== "ALL") {
      nextRows = nextRows.filter((row) => row.LIVE_ALERT_STATE === normalizedStatus);
    }
    if (props.query.trim()) {
      const q = props.query.trim().toLowerCase();
      nextRows = nextRows.filter((row) => {
        const haystack = [
          row.ALERT_TYPE,
          row.ALERT_KEY,
          row.OUR_REF,
          row.JOB_NUMBER,
          row.ADVERTISER,
          row.CAMPAIGN,
          row.SNOOZE_REASON,
          row.SNOOZED_BY,
          row.DISMISSED_BY
        ].join(" ").toLowerCase();
        return haystack.includes(q);
      });
    }
    return nextRows.map((row, index) => ({ ...row, __row_id: `${row.ALERT_TYPE}:${row.ALERT_KEY}:${row.SOURCE_VIEW || "ROW"}:${index}` }));
  }, [props.query, props.status, scopedRows]);
  const sortedRows = React.useMemo(() => sortRows(filtered, sort), [filtered, sort]);
  const totalRows = sortedRows.length;
  const totalPages = Math.max(1, Math.ceil(totalRows / ALERT_PAGE_SIZE));
  const safePage = Math.min(props.page, totalPages);
  const pageRows = React.useMemo(() => {
    const start = (safePage - 1) * ALERT_PAGE_SIZE;
    return sortedRows.slice(start, start + ALERT_PAGE_SIZE);
  }, [sortedRows, safePage]);

  React.useEffect(() => {
    if (props.page !== safePage) props.setPage(safePage);
  }, [props.page, props.setPage, safePage]);

  const selectedAlerts = pageRows
    .filter((row) => selected.has(String(row.__row_id)))
    .map((row) => ({
      alert_type: String(row.ALERT_TYPE),
      alert_key: String(row.ALERT_KEY),
      our_ref: String(row.OUR_REF),
      state_version: String(row.STATE_VERSION || "")
    }));

  async function postAction(path: string, body: Record<string, unknown>) {
    setActionError("");
    try {
      await apiFetch(path, { method: "POST", body: JSON.stringify(body) });
      invalidateApiCache("/api/alerts");
      setSelected(new Set());
      await props.refresh();
    } catch (err) {
      const message = err instanceof Error ? err.message : "Action failed";
      setActionError(message);
      if (err instanceof ApiError && err.status === 409) await props.refresh();
    }
  }

  function applyOptimisticSnooze(alerts: Array<Record<string, string>>, reason: string, endDate: string | null) {
    const selectedKeys = new Set(alerts.map((alert) => `${alert.alert_type}::${alert.alert_key}`));
    const today = new Date().toISOString().slice(0, 10);
    props.updateRows((rows) => rows.map((row) => {
      const key = `${row.ALERT_TYPE}::${row.ALERT_KEY}`;
      if (!selectedKeys.has(key)) return row;
      return {
        ...row,
        LIVE_ALERT_STATE: "ACTIVE",
        SNOOZE_STATUS: "ACTIVE",
        SNOOZE_REASON: reason,
        SNOOZE_START_DATE: today,
        SNOOZE_END_DATE: endDate || "",
        SNOOZED_BY: "Saving...",
        UPDATED_AT: "Saving..."
      };
    }));
    setSelected(new Set());
  }

  return (
    <>
      <PageHeader
        eyebrow={props.meta.project_id ? `${props.meta.project_id}.${props.meta.dataset}` : "Latest alert snapshot"}
        title={`${sectionMeta.label} alerts`}
        subtitle="Review this alert type and manage active snoozes or dismissals."
        loading={props.loading}
        onRefresh={() => {
          invalidateApiCache("/api/alerts");
          return props.refresh();
        }}
        onDownload={() => downloadCsv(`${sectionMeta.label.toLowerCase().replaceAll(" ", "-")}-alerts.csv`, sortedRows)}
      />
      <MetricStrip metrics={[
        { label: "Open", value: num(scopedCounts.open) },
        { label: "Snoozed", value: num(scopedCounts.snoozed) },
        { label: "Dismissed", value: num(scopedCounts.dismissed) },
        { label: "Latest run", value: props.lastLoadedAt ? `${props.meta.latest_run || "N/A"} · refreshed ${props.lastLoadedAt}` : String(props.meta.latest_run || "N/A") }
      ]} />
      <Toolbar query={props.query} setQuery={props.setQuery} status={props.status} setStatus={(value) => { props.setStatus(value); props.setPage(1); }} statuses={["OPEN", "SNOOZED", "DISMISSED", "ALL"]} selectedCount={selected.size} />
      <DataState loading={props.loading && !props.hasLoaded} error={apiSupportsFilteredAlerts ? props.error : "The API server needs to be restarted to load filtered alert tables."} empty={!pageRows.length}>
        <>
          <DataTable
            rows={pageRows}
            selected={selected}
            setSelected={setSelected}
            idKey="__row_id"
            sort={sort}
            onSort={(key) => {
              setSort((current) => nextSort(current, key));
              props.setPage(1);
            }}
            columns={isSnoozeDetailView ? snoozeDetailColumns : alertColumns}
            format={(key, value, row) => {
              if (["TOTAL_IMPRESSIONS", "TOTAL_CLICKS"].includes(key)) return num(value);
              if (key === "SNOOZE_END_DATE" && !value && String(row.SNOOZE_STATUS || "").toUpperCase() === "ACTIVE") return "Permanent";
              return String(value ?? "");
            }}
          />
          <TablePagination page={safePage} totalPages={totalPages} totalRows={totalRows} pageSize={ALERT_PAGE_SIZE} onPageChange={props.setPage} />
        </>
      </DataState>
      <ActionDock selectedCount={selected.size} onClear={() => setSelected(new Set())}>
        {props.status === "OPEN" ? (
          <SnoozeButton
            endpoint="/api/alerts/snooze"
            alerts={selectedAlerts}
            onSubmitStart={(alerts, reason, endDate) => applyOptimisticSnooze(alerts, reason, endDate)}
            onDone={() => { invalidateApiCache("/api/alerts"); void props.refresh({ silent: true }); }}
            onError={(message) => { setActionError(message); void props.refresh({ silent: true }); }}
            onConflict={() => void props.refresh({ silent: true })}
            inline
          />
        ) : (
          <>
            <SnoozeButton
              endpoint="/api/alerts/snooze"
              alerts={selectedAlerts}
              onSubmitStart={(alerts, reason, endDate) => applyOptimisticSnooze(alerts, reason, endDate)}
              onDone={() => { invalidateApiCache("/api/alerts"); void props.refresh({ silent: true }); }}
              onError={(message) => { setActionError(message); void props.refresh({ silent: true }); }}
              onConflict={() => void props.refresh({ silent: true })}
            />
            <button className="dock-button" disabled={!selected.size} onClick={() => void postAction("/api/alerts/unsnooze", { alerts: selectedAlerts })}>
              <Check size={15} /> Unsnooze
            </button>
            <input
              className="dock-input"
              name="admin-action-pass"
              placeholder="Admin pass"
              type="password"
              autoComplete="new-password"
              value={adminPass}
              onChange={(event) => setAdminPass(event.target.value)}
            />
            <button className="dock-button danger" disabled={!selected.size || !adminPass} onClick={() => void postAction("/api/alerts/dismiss", { alerts: selectedAlerts, reason: "Dismissed in React dashboard", admin_pass: adminPass })}>
              <X size={15} /> Dismiss
            </button>
          </>
        )}
        {actionError && <span className="dock-error">{actionError}</span>}
      </ActionDock>
    </>
  );
}

function TablePagination(props: { page: number; totalPages: number; totalRows: number; pageSize: number; onPageChange: (page: number) => void }) {
  if (props.totalRows <= props.pageSize) return null;
  const start = (props.page - 1) * props.pageSize + 1;
  const end = Math.min(props.totalRows, props.page * props.pageSize);
  return (
    <div className="table-pagination">
      <span>{num(start)}-{num(end)} of {num(props.totalRows)}</span>
      <div className="pagination-actions">
        <button className="ghost-button" disabled={props.page <= 1} onClick={() => props.onPageChange(props.page - 1)}>Previous</button>
        <span>Page {num(props.page)} of {num(props.totalPages)}</span>
        <button className="ghost-button" disabled={props.page >= props.totalPages} onClick={() => props.onPageChange(props.page + 1)}>Next</button>
      </div>
    </div>
  );
}

function Toolbar(props: { query: string; setQuery: (value: string) => void; status: string; setStatus: (value: string) => void; statuses: string[]; selectedCount: number }) {
  return (
    <section className="toolbar">
      <div className="searchbox">
        <Search size={16} />
        <input
          name="alerts-table-search"
          placeholder="Search refs, campaigns, advertisers..."
          autoComplete="off"
          value={props.query}
          onChange={(event) => props.setQuery(event.target.value)}
        />
      </div>
      <div className="segmented">
        {props.statuses.map((status) => (
          <button key={status} className={props.status === status ? "active" : ""} onClick={() => props.setStatus(status)}>{status}</button>
        ))}
      </div>
      <div className="selected-chip"><SlidersHorizontal size={15} /> {props.selectedCount} selected</div>
    </section>
  );
}

function DataTable(props: {
  rows: AnyRow[];
  selected: Set<string>;
  setSelected: (selected: Set<string>) => void;
  idKey: string;
  sort: SortState;
  onSort: (key: string) => void;
  columns: Array<[string, string]>;
  format: (key: string, value: unknown, row: AnyRow) => string;
  renderCell?: (key: string, value: unknown, row: AnyRow) => React.ReactNode | null;
  headerHelp?: Record<string, string>;
}) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th className="select-col"><ArrowDownUp size={14} /></th>
            {props.columns.map(([key, label]) => {
              const active = props.sort?.key === key;
              return (
                <th key={key}>
                  <button
                    className={`sort-header ${active ? "active" : ""}`}
                    type="button"
                    onClick={() => props.onSort(key)}
                    aria-sort={active ? (props.sort?.direction === "asc" ? "ascending" : "descending") : "none"}
                  >
                    <span className="header-label">
                      {label}
                      {props.headerHelp?.[key] && (
                        <span className="header-help" title={props.headerHelp[key]} aria-label={props.headerHelp[key]}>?</span>
                      )}
                    </span>
                    {active ? (
                      props.sort?.direction === "asc" ? <ArrowUp size={13} /> : <ArrowDown size={13} />
                    ) : (
                      <ArrowDownUp size={13} />
                    )}
                  </button>
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {props.rows.map((row) => {
            const id = String(row[props.idKey]);
            const checked = props.selected.has(id);
            return (
              <tr key={id} className={checked ? "selected" : ""}>
                <td className="select-col">
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={(event) => {
                      const next = new Set(props.selected);
                      if (event.target.checked) next.add(id);
                      else next.delete(id);
                      props.setSelected(next);
                    }}
                  />
                </td>
                {props.columns.map(([key]) => {
                  const customCell = props.renderCell?.(key, row[key], row);
                  return (
                    <td key={key} className={key === "DELIVERY_PACING_RATIO" ? "strong-cell" : ""} title={customCell ? "" : String(row[key] ?? "")}>
                      {customCell ?? props.format(key, row[key], row)}
                    </td>
                  );
                })}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function ProgressCell(props: { timeProgress: number; deliveryProgress: number }) {
  return (
    <div className="progress-cell">
      <ProgressBar label="Time" value={props.timeProgress} tone="time" />
      <ProgressBar label="Delivery" value={props.deliveryProgress} tone="delivery" />
    </div>
  );
}

function ProgressBar(props: { label: string; value: number; tone: "time" | "delivery" }) {
  const clamped = Math.max(0, Math.min(props.value, 1));
  return (
    <div className="progress-line">
      <span>{props.label}</span>
      <div className="progress-track" aria-label={`${props.label} ${wholePct(props.value)}`}>
        <div className={`progress-fill ${props.tone}`} style={{ width: `${clamped * 100}%` }} />
      </div>
      <strong>{wholePct(props.value)}</strong>
    </div>
  );
}

function DataState(props: { loading: boolean; error: string; empty: boolean; children: React.ReactNode }) {
  if (props.loading) return <div className="state-box"><Loader2 className="spin" /> Loading live data...</div>;
  if (props.error) return <div className="state-box error"><AlertTriangle /> {props.error}</div>;
  if (props.empty) return <div className="state-box"><Clock3 /> No rows match the current view.</div>;
  return <>{props.children}</>;
}

function SnoozeButton(props: {
  endpoint: string;
  alerts: Array<Record<string, string>>;
  onDone: () => void;
  onSubmitStart?: (alerts: Array<Record<string, string>>, reason: string, endDate: string | null) => void;
  onError?: (message: string) => void;
  onConflict?: () => void;
  inline?: boolean;
}) {
  const [open, setOpen] = React.useState(Boolean(props.inline));
  const [reason, setReason] = React.useState("");
  const [endDate, setEndDate] = React.useState(new Date().toISOString().slice(0, 10));
  const [permanent, setPermanent] = React.useState(false);
  const [loading, setLoading] = React.useState(false);

  React.useEffect(() => {
    if (props.inline) setOpen(true);
  }, [props.inline]);

  async function submit() {
    setLoading(true);
    const nextEndDate = permanent ? null : endDate || new Date().toISOString().slice(0, 10);
    props.onSubmitStart?.(props.alerts, reason, nextEndDate);
    try {
      await apiFetch(props.endpoint, {
        method: "POST",
        body: JSON.stringify({ alerts: props.alerts, reason, end_date: nextEndDate })
      });
      setOpen(Boolean(props.inline));
      setReason("");
      setEndDate(new Date().toISOString().slice(0, 10));
      setPermanent(false);
      props.onDone();
    } catch (err) {
      const message = err instanceof Error ? err.message : "Snooze failed";
      props.onError?.(message);
      if (err instanceof ApiError && err.status === 409) props.onConflict?.();
    } finally {
      setLoading(false);
    }
  }

  if (!open) {
    return <button className="dock-button" disabled={!props.alerts.length} onClick={() => setOpen(true)}><Clock3 size={15} /> Snooze</button>;
  }

  return (
    <>
      <input
        className="dock-input wide"
        name="snooze-reason"
        placeholder="Snooze reason"
        autoComplete="off"
        value={reason}
        onChange={(event) => setReason(event.target.value)}
      />
      <label className="dock-check">
        <input type="checkbox" checked={permanent} onChange={(event) => setPermanent(event.target.checked)} />
        Permanent
      </label>
      {!permanent && (
        <input
          className="dock-input"
          name="snooze-end-date"
          type="date"
          autoComplete="off"
          value={endDate}
          onChange={(event) => setEndDate(event.target.value)}
        />
      )}
      <button className="dock-button" disabled={!reason || (!permanent && !endDate) || loading} onClick={() => void submit()}>
        {loading ? <Loader2 className="spin" size={15} /> : <Clock3 size={15} />} Snooze
      </button>
    </>
  );
}

function ActionDock(props: { selectedCount: number; onClear: () => void; children: React.ReactNode }) {
  return (
    <div className={`action-dock ${props.selectedCount ? "visible" : ""}`}>
      <span>{props.selectedCount} selected</span>
      {props.children}
      <button className="icon-button" title="Clear selection" onClick={props.onClear}><X size={15} /></button>
    </div>
  );
}

function AdminPage({ isAdmin }: { isAdmin: boolean }) {
  const [defaults, setDefaults] = React.useState<AutomationDefaults | null>(null);
  const [recipients, setRecipients] = React.useState("");
  const [forceAlert, setForceAlert] = React.useState(true);
  const [forceDryRun, setForceDryRun] = React.useState(true);
  const [campaignResult, setCampaignResult] = React.useState<AutomationResult | null>(null);
  const [dailyResult, setDailyResult] = React.useState<AutomationResult | null>(null);
  const [loading, setLoading] = React.useState<"campaign" | "daily" | "defaults" | "">("defaults");
  const [error, setError] = React.useState("");

  React.useEffect(() => {
    if (!isAdmin) return;
    setLoading("defaults");
    apiFetch<AutomationDefaults>("/api/admin/automations")
      .then((data) => {
        setDefaults(data);
        setRecipients(data.campaign_alert.recipients || "");
        setForceDryRun((data.daily_trafficking.dry_run_mode || "true").toLowerCase() !== "false");
        setError("");
      })
      .catch((err) => setError(err instanceof Error ? err.message : "Failed to load admin settings."))
      .finally(() => setLoading(""));
  }, [isAdmin]);

  async function runCampaignAlert() {
    setLoading("campaign");
    setError("");
    setCampaignResult(null);
    try {
      const result = await apiFetch<AutomationResult>("/api/admin/automations/campaign-alert", {
        method: "POST",
        body: JSON.stringify({ recipients, force_run: forceAlert })
      });
      setCampaignResult(result);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Campaign alert run failed.");
    } finally {
      setLoading("");
    }
  }

  async function runDailyTrafficking() {
    setLoading("daily");
    setError("");
    setDailyResult(null);
    try {
      const result = await apiFetch<AutomationResult>("/api/admin/automations/daily-trafficking", {
        method: "POST",
        body: JSON.stringify({ force_dry_run: forceDryRun })
      });
      setDailyResult(result);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Daily trafficking run failed.");
    } finally {
      setLoading("");
    }
  }

  if (!isAdmin) {
    return <PlaceholderPage title="Admin" body="Admin access is restricted." />;
  }

  return (
    <>
      <PageHeader
        eyebrow="Ashwin only"
        title="Admin"
        subtitle="Manual controls for automation runs. These call the same scripts used by the existing scheduled workflows."
        loading={loading === "defaults"}
        onRefresh={() => apiFetch<AutomationDefaults>("/api/admin/automations").then((data) => {
          setDefaults(data);
          setRecipients(data.campaign_alert.recipients || "");
        })}
      />
      {error && <div className="state-box error compact"><AlertTriangle /> {error}</div>}
      <section className="admin-grid">
        <article className="admin-card">
          <div>
            <span className="eyebrow">Email alert</span>
            <h2>Alert Digest Email</h2>
            <p>Runs all alert checks and sends the digest email if open alerts exist.</p>
          </div>
          <label>
            Recipients
            <input
              name="admin-alert-recipients"
              autoComplete="off"
              value={recipients}
              onChange={(event) => setRecipients(event.target.value)}
              placeholder="ashwin@acquirenz.com,zane@acquirenz.com"
            />
          </label>
          <label className="admin-check">
            <input type="checkbox" checked={forceAlert} onChange={(event) => setForceAlert(event.target.checked)} />
            Force run, ignoring the NZ 6AM weekday guard
          </label>
          <button className="primary-button" disabled={loading === "campaign" || !recipients.trim()} onClick={() => void runCampaignAlert()}>
            {loading === "campaign" ? <Loader2 className="spin" size={16} /> : <Activity size={16} />}
            Send Alert Digest Now
          </button>
          <AutomationOutput result={campaignResult} />
          {defaults?.campaign_alert.script && <p className="admin-note">Script: {defaults.campaign_alert.script}</p>}
        </article>

        <article className="admin-card">
          <div>
            <span className="eyebrow">Trafficking</span>
            <h2>Daily Trafficking Dry Run</h2>
            <p>Runs the Gmail-to-Asana dry-run script and emails the parent/subtask CSV outputs.</p>
          </div>
          <label className="admin-check">
            <input type="checkbox" checked={forceDryRun} onChange={(event) => setForceDryRun(event.target.checked)} />
            Force DRY_RUN_MODE=true
          </label>
          {!forceDryRun && (
            <div className="warning-box">
              If the backend secret `DRY_RUN_MODE` is false, this can create Asana tasks.
            </div>
          )}
          <button className="primary-button" disabled={loading === "daily"} onClick={() => void runDailyTrafficking()}>
            {loading === "daily" ? <Loader2 className="spin" size={16} /> : <Table2 size={16} />}
            Run Daily Trafficking Script Now
          </button>
          <AutomationOutput result={dailyResult} />
          {defaults?.daily_trafficking.script && <p className="admin-note">Script: {defaults.daily_trafficking.script}</p>}
        </article>
      </section>
    </>
  );
}

function AutomationOutput({ result }: { result: AutomationResult | null }) {
  if (!result) return null;
  const ok = result.status === "ok" && result.exit_code === 0;
  return (
    <div className={`admin-output ${ok ? "success" : "failure"}`}>
      <strong>{ok ? "Completed" : `Finished with ${result.status}`} · exit {result.exit_code}</strong>
      <span>{new Date(result.finished_at).toLocaleString()}</span>
      {result.output && <pre>{result.output}</pre>}
    </div>
  );
}

function PlaceholderPage({ title, body }: { title: string; body: string }) {
  return (
    <section className="placeholder">
      <LayoutDashboard size={36} />
      <h1>{title}</h1>
      <p>{body}</p>
    </section>
  );
}

function avg(values: number[]) {
  const clean = values.filter((value) => Number.isFinite(value));
  if (!clean.length) return null;
  return clean.reduce((sum, value) => sum + value, 0) / clean.length;
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);

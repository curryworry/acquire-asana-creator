import React from "react";
import ReactDOM from "react-dom/client";
import {
  Activity,
  AlertTriangle,
  ArrowDownUp,
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
  ShieldCheck,
  SlidersHorizontal,
  Sparkles,
  Table2,
  X
} from "lucide-react";
import "./styles.css";

type AlertSection = "NOT_LIVE" | "STOPPED_IMPRESSIONS" | "MISSING_OUR_REF" | "ENDED_BUT_IMPRESSIONS";
type Page = "margin" | "alerts:not_live" | "alerts:stopped_impressions" | "alerts:missing_our_ref" | "alerts:ended_but_impressions" | "trafficking" | "automation";
type ApiEnvelope<T> = { rows: T[]; meta: Record<string, string> };
type AnyRow = Record<string, string | number | null>;

const TOKEN_KEY = "acquire_ops_token";
const ALERT_SECTIONS: Array<{ key: AlertSection; page: Page; label: string }> = [
  { key: "NOT_LIVE", page: "alerts:not_live", label: "Not live" },
  { key: "STOPPED_IMPRESSIONS", page: "alerts:stopped_impressions", label: "Stopped impressions" },
  { key: "MISSING_OUR_REF", page: "alerts:missing_our_ref", label: "Missing OUR_REF" },
  { key: "ENDED_BUT_IMPRESSIONS", page: "alerts:ended_but_impressions", label: "Ended but impressions" }
];
const ALERT_PAGE_SIZE = 100;
const EMPTY_ALERT_COUNTS: Record<AlertSection, number> = {
  NOT_LIVE: 0,
  STOPPED_IMPRESSIONS: 0,
  MISSING_OUR_REF: 0,
  ENDED_BUT_IMPRESSIONS: 0
};
const API_CACHE = new Map<string, ApiEnvelope<AnyRow>>();

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
    throw new Error(body.detail || `Request failed: ${response.status}`);
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
  const [error, setError] = React.useState("");
  const refresh = React.useCallback(async () => {
    if (!enabled) {
      setRows([]);
      setMeta({});
      setLoading(false);
      setError("");
      return;
    }
    setLoading(true);
    setError("");
    try {
      const data = await apiFetch<ApiEnvelope<T>>(path);
      API_CACHE.set(path, data as ApiEnvelope<AnyRow>);
      setRows(data.rows);
      setMeta(data.meta || {});
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
      setError("");
      return;
    }
    const cached = API_CACHE.get(path) as ApiEnvelope<T> | undefined;
    if (cached) {
      setRows(cached.rows);
      setMeta(cached.meta || {});
      setLoading(false);
      setError("");
      return;
    }
    void refresh();
  }, [enabled, path, refresh]);

  return { rows, meta, loading, error, refresh };
}

function App() {
  const [page, setPage] = React.useState<Page>("alerts:not_live");
  const [token, setToken] = React.useState(localStorage.getItem(TOKEN_KEY) || "");
  const [user, setUser] = React.useState("");
  const [alertsExpanded, setAlertsExpanded] = React.useState(true);
  const alertsActive = page.startsWith("alerts:");
  const activeAlertType = pageToAlertType(page);
  const [alertQuery, setAlertQuery] = React.useState("");
  const [alertStatus, setAlertStatus] = React.useState("OPEN");
  const [alertPage, setAlertPage] = React.useState(1);
  const deferredAlertQuery = React.useDeferredValue(alertQuery);
  const alertsPath = activeAlertType ? buildAlertsPath(activeAlertType, alertStatus, deferredAlertQuery, alertPage) : "";
  const alertsData = useApiRows<AnyRow>(alertsPath, Boolean(token && activeAlertType));
  const [alertCounts, setAlertCounts] = React.useState<Record<AlertSection, number>>(EMPTY_ALERT_COUNTS);

  React.useEffect(() => {
    if (!token) return;
    apiFetch<{ display_name?: string; username?: string }>("/api/auth/me")
      .then((me) => setUser(me.display_name || me.username || "User"))
      .catch(() => {
        localStorage.removeItem(TOKEN_KEY);
        setToken("");
      });
  }, [token]);

  React.useEffect(() => {
    setAlertPage(1);
  }, [activeAlertType, alertStatus, deferredAlertQuery]);

  React.useEffect(() => {
    if (!("count_not_live" in alertsData.meta)) return;
    setAlertCounts({
      NOT_LIVE: Number(alertsData.meta.count_not_live || 0),
      STOPPED_IMPRESSIONS: Number(alertsData.meta.count_stopped_impressions || 0),
      MISSING_OUR_REF: Number(alertsData.meta.count_missing_our_ref || 0),
      ENDED_BUT_IMPRESSIONS: Number(alertsData.meta.count_ended_but_impressions || 0)
    });
  }, [alertsData.meta]);

  if (!token) {
    return <Login onLogin={(nextToken, displayName) => {
      localStorage.setItem(TOKEN_KEY, nextToken);
      setToken(nextToken);
      setUser(displayName);
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
        </nav>
        <div className="user-panel">
          <ShieldCheck size={16} />
          <span>{user || "Signed in"}</span>
          <button className="icon-button" title="Sign out" onClick={() => {
            localStorage.removeItem(TOKEN_KEY);
            setToken("");
          }}>
            <LogOut size={16} />
          </button>
        </div>
      </aside>
      <main className="workspace">
        {page === "margin" && <MarginPage />}
        {page === "alerts:not_live" && activeAlertType && <AlertsPage alertType={activeAlertType} {...alertsData} query={alertQuery} setQuery={setAlertQuery} status={alertStatus} setStatus={setAlertStatus} page={alertPage} setPage={setAlertPage} />}
        {page === "alerts:stopped_impressions" && activeAlertType && <AlertsPage alertType={activeAlertType} {...alertsData} query={alertQuery} setQuery={setAlertQuery} status={alertStatus} setStatus={setAlertStatus} page={alertPage} setPage={setAlertPage} />}
        {page === "alerts:missing_our_ref" && activeAlertType && <AlertsPage alertType={activeAlertType} {...alertsData} query={alertQuery} setQuery={setAlertQuery} status={alertStatus} setStatus={setAlertStatus} page={alertPage} setPage={setAlertPage} />}
        {page === "alerts:ended_but_impressions" && activeAlertType && <AlertsPage alertType={activeAlertType} {...alertsData} query={alertQuery} setQuery={setAlertQuery} status={alertStatus} setStatus={setAlertStatus} page={alertPage} setPage={setAlertPage} />}
        {page === "trafficking" && <PlaceholderPage title="Trafficking to Asana" body="The next migration slice will bring the Gmail fetch, parse preview, Asana dedupe check, and dry-run downloads into this interface." />}
        {page === "automation" && <PlaceholderPage title="Automation Runs" body="Manual automation triggers should run as background jobs with live status and logs, instead of blocking the interface." />}
      </main>
    </div>
  );
}

function Login({ onLogin }: { onLogin: (token: string, displayName: string) => void }) {
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
      onLogin(response.token, response.display_name || response.username);
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
  const { rows, meta, loading, error, refresh } = useApiRows<AnyRow>("/api/margin");
  const [query, setQuery] = React.useState("");
  const [status, setStatus] = React.useState("OPEN");
  const [selected, setSelected] = React.useState<Set<string>>(new Set());

  const filtered = React.useMemo(() => rows.filter((row) => {
    const text = `${row.OUR_REF} ${row.JOB_NUMBER} ${row.CAMPAIGN_NAME} ${row.ADVERTISER_NAME}`.toLowerCase();
    const stateMatch = status === "ALL" || row.MARGIN_SNOOZE_STATE === status;
    return stateMatch && text.includes(query.toLowerCase());
  }), [rows, query, status]);

  const active = rows.filter((row) => row.MARGIN_SNOOZE_STATE === "OPEN");
  const selectedAlerts = rows
    .filter((row) => selected.has(String(row.OUR_REF)))
    .map((row) => ({ alert_type: "MARGIN_DASHBOARD", alert_key: String(row.OUR_REF), our_ref: String(row.OUR_REF) }));

  return (
    <>
      <PageHeader
        eyebrow={meta.view ? `${meta.project_id}.${meta.dataset}.${meta.view}` : "BigQuery margin view"}
        title="Margin Dashboard"
        subtitle="Live margin, pacing, budget, and snooze state at OUR REF level."
        loading={loading}
        onRefresh={refresh}
        onDownload={() => downloadCsv("margin-dashboard.csv", filtered)}
      />
      <MetricStrip metrics={[
        { label: "Active rows", value: num(active.length) },
        { label: "Active budget", value: currency(active.reduce((sum, row) => sum + Number(row.BUDGET || 0), 0)) },
        { label: "Actual nett spend", value: currency(active.reduce((sum, row) => sum + Number(row.ACTUAL_NETT_SPEND || 0), 0)) },
        { label: "Avg margin", value: pct(avg(active.map((row) => Number(row.MARGIN_PCT || 0)))) }
      ]} />
      <Toolbar query={query} setQuery={setQuery} status={status} setStatus={setStatus} statuses={["OPEN", "ACTIVE", "ALL"]} selectedCount={selected.size} />
      <DataState loading={loading} error={error} empty={!filtered.length}>
        <DataTable
          rows={filtered}
          selected={selected}
          setSelected={setSelected}
          idKey="OUR_REF"
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

function AlertsPage(props: {
  alertType: AlertSection;
  rows: AnyRow[];
  meta: Record<string, string>;
  loading: boolean;
  error: string;
  refresh: () => Promise<void>;
  query: string;
  setQuery: (value: string) => void;
  status: string;
  setStatus: (value: string) => void;
  page: number;
  setPage: (value: number) => void;
}) {
  const [selected, setSelected] = React.useState<Set<string>>(new Set());
  const [adminPass, setAdminPass] = React.useState("");
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
  const apiSupportsFilteredAlerts = props.meta.alert_api_version === "2";

  React.useEffect(() => {
    setSelected(new Set());
  }, [props.status, props.alertType, props.page]);

  const filtered = React.useMemo<AnyRow[]>(
    () => props.rows.map((row, index) => ({ ...row, __row_id: `${row.ALERT_TYPE}:${row.ALERT_KEY}:${props.page}:${index}` })),
    [props.page, props.rows]
  );
  const totalRows = Number(props.meta.total_rows || 0);
  const totalPages = Number(props.meta.total_pages || 1);

  const selectedAlerts = filtered
    .filter((row) => selected.has(String(row.__row_id)))
    .map((row) => ({ alert_type: String(row.ALERT_TYPE), alert_key: String(row.ALERT_KEY), our_ref: String(row.OUR_REF) }));

  async function postAction(path: string, body: Record<string, unknown>) {
    await apiFetch(path, { method: "POST", body: JSON.stringify(body) });
    invalidateApiCache("/api/alerts");
    setSelected(new Set());
    await props.refresh();
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
        onDownload={() => downloadCsv(`${sectionMeta.label.toLowerCase().replaceAll(" ", "-")}-alerts.csv`, filtered)}
      />
      <MetricStrip metrics={[
        { label: "Open", value: num(props.meta.open_count || 0) },
        { label: "Snoozed", value: num(props.meta.snoozed_count || 0) },
        { label: "Dismissed", value: num(props.meta.dismissed_count || 0) },
        { label: "Latest run", value: String(props.meta.latest_run || "N/A") }
      ]} />
      <Toolbar query={props.query} setQuery={props.setQuery} status={props.status} setStatus={(value) => { props.setStatus(value); props.setPage(1); }} statuses={["OPEN", "SNOOZED", "DISMISSED", "ALL"]} selectedCount={selected.size} />
      <DataState loading={props.loading} error={apiSupportsFilteredAlerts ? props.error : "The API server needs to be restarted to load filtered alert tables."} empty={!filtered.length}>
        <>
          <DataTable
            rows={filtered}
            selected={selected}
            setSelected={setSelected}
            idKey="__row_id"
            columns={isSnoozeDetailView ? snoozeDetailColumns : alertColumns}
            format={(key, value, row) => {
              if (["TOTAL_IMPRESSIONS", "TOTAL_CLICKS"].includes(key)) return num(value);
              if (key === "SNOOZE_END_DATE" && !value && String(row.SNOOZE_STATUS || "").toUpperCase() === "ACTIVE") return "Permanent";
              return String(value ?? "");
            }}
          />
          <TablePagination page={props.page} totalPages={totalPages} totalRows={totalRows} pageSize={Number(props.meta.page_size || ALERT_PAGE_SIZE)} onPageChange={props.setPage} />
        </>
      </DataState>
      <ActionDock selectedCount={selected.size} onClear={() => setSelected(new Set())}>
        {props.status === "OPEN" ? (
          <SnoozeButton endpoint="/api/alerts/snooze" alerts={selectedAlerts} onDone={() => { setSelected(new Set()); invalidateApiCache("/api/alerts"); void props.refresh(); }} inline />
        ) : (
          <>
            <SnoozeButton endpoint="/api/alerts/snooze" alerts={selectedAlerts} onDone={() => { invalidateApiCache("/api/alerts"); void props.refresh(); }} />
            <button className="dock-button" disabled={!selected.size} onClick={() => void postAction("/api/alerts/unsnooze", { alerts: selectedAlerts })}>
              <Check size={15} /> Unsnooze
            </button>
            <input className="dock-input" placeholder="Admin pass" type="password" value={adminPass} onChange={(event) => setAdminPass(event.target.value)} />
            <button className="dock-button danger" disabled={!selected.size || !adminPass} onClick={() => void postAction("/api/alerts/dismiss", { alerts: selectedAlerts, reason: "Dismissed in React dashboard", admin_pass: adminPass })}>
              <X size={15} /> Dismiss
            </button>
          </>
        )}
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
        <input placeholder="Search refs, campaigns, advertisers..." value={props.query} onChange={(event) => props.setQuery(event.target.value)} />
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
  columns: Array<[string, string]>;
  format: (key: string, value: unknown, row: AnyRow) => string;
}) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th className="select-col"><ArrowDownUp size={14} /></th>
            {props.columns.map(([key, label]) => <th key={key}>{label}</th>)}
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
                {props.columns.map(([key]) => <td key={key} title={String(row[key] ?? "")}>{props.format(key, row[key], row)}</td>)}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function DataState(props: { loading: boolean; error: string; empty: boolean; children: React.ReactNode }) {
  if (props.loading) return <div className="state-box"><Loader2 className="spin" /> Loading live data...</div>;
  if (props.error) return <div className="state-box error"><AlertTriangle /> {props.error}</div>;
  if (props.empty) return <div className="state-box"><Clock3 /> No rows match the current view.</div>;
  return <>{props.children}</>;
}

function SnoozeButton(props: { endpoint: string; alerts: Array<Record<string, string>>; onDone: () => void; inline?: boolean }) {
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
    try {
      await apiFetch(props.endpoint, {
        method: "POST",
        body: JSON.stringify({ alerts: props.alerts, reason, end_date: permanent ? null : endDate || new Date().toISOString().slice(0, 10) })
      });
      setOpen(Boolean(props.inline));
      setReason("");
      setEndDate(new Date().toISOString().slice(0, 10));
      setPermanent(false);
      props.onDone();
    } finally {
      setLoading(false);
    }
  }

  if (!open) {
    return <button className="dock-button" disabled={!props.alerts.length} onClick={() => setOpen(true)}><Clock3 size={15} /> Snooze</button>;
  }

  return (
    <>
      <input className="dock-input wide" placeholder="Snooze reason" value={reason} onChange={(event) => setReason(event.target.value)} />
      <label className="dock-check">
        <input type="checkbox" checked={permanent} onChange={(event) => setPermanent(event.target.checked)} />
        Permanent
      </label>
      {!permanent && <input className="dock-input" type="date" value={endDate} onChange={(event) => setEndDate(event.target.value)} />}
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

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::{extract::State, response::Html, routing::get, Json, Router};
use serde::Serialize;

use swanlake_core::config::ServerConfig;
use swanlake_core::metrics::{Metrics, MetricsSnapshot};
use swanlake_core::session::registry::{SessionRegistry, SessionRegistrySnapshot};

#[derive(Clone)]
struct StatusState {
    metrics: Arc<Metrics>,
    registry: Arc<SessionRegistry>,
}

#[derive(Serialize)]
struct StatusPayload {
    generated_at_ms: u64,
    metrics: MetricsSnapshot,
    sessions: SessionRegistrySnapshot,
}

pub async fn spawn_status_server(
    config: &ServerConfig,
    metrics: Arc<Metrics>,
    registry: Arc<SessionRegistry>,
) -> Result<()> {
    if !config.status_enabled {
        return Ok(());
    }

    let addr: SocketAddr = format!("{}:{}", config.status_host, config.status_port)
        .parse()
        .with_context(|| "invalid status server bind address")?;

    let state = StatusState { metrics, registry };
    let app = Router::new()
        .route("/", get(status_page))
        .route("/status.json", get(status_json))
        .with_state(state);

    tokio::spawn(async move {
        match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => {
                if let Err(err) = axum::serve(listener, app).await {
                    tracing::error!(%err, "status server failed");
                }
            }
            Err(err) => {
                tracing::error!(%err, "status server bind failed");
            }
        }
    });

    tracing::info!(%addr, "status server listening");
    Ok(())
}

async fn status_page() -> Html<&'static str> {
    Html(STATUS_PAGE)
}

async fn status_json(State(state): State<StatusState>) -> Json<StatusPayload> {
    let payload = StatusPayload {
        generated_at_ms: now_millis(),
        metrics: state.metrics.snapshot(),
        sessions: state.registry.snapshot(),
    };
    Json(payload)
}

fn now_millis() -> u64 {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as u64
}

const STATUS_PAGE: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>SwanLake DB Status</title>
  <style>
    :root {
      --bg: #0f1218;
      --surface: #171b22;
      --surface-strong: #1e2430;
      --border: #2a313d;
      --text: #eef1f6;
      --muted: #9aa3b2;
      --accent: #4cc38a;
      --warn: #f4c168;
      --danger: #ff7b7b;
      --shadow: 0 18px 30px rgba(9, 12, 18, 0.45);
      --radius: 14px;
    }

    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "DM Sans", "IBM Plex Sans", "Segoe UI", sans-serif;
      background: radial-gradient(circle at top left, rgba(76, 195, 138, 0.12), transparent 40%),
                  radial-gradient(circle at 80% 0%, rgba(244, 193, 104, 0.14), transparent 45%),
                  var(--bg);
      color: var(--text);
    }

    .wrap {
      max-width: 1200px;
      margin: 0 auto;
      padding: 28px 22px 40px;
      display: grid;
      gap: 22px;
    }

    header {
      display: flex;
      flex-wrap: wrap;
      gap: 18px;
      align-items: center;
      justify-content: space-between;
    }

    .eyebrow {
      font-size: 12px;
      letter-spacing: 2px;
      text-transform: uppercase;
      color: var(--muted);
      margin-bottom: 6px;
    }

    h1 {
      margin: 0;
      font-size: clamp(26px, 4vw, 36px);
    }

    .sub {
      margin: 8px 0 0;
      color: var(--muted);
      font-size: 14px;
    }

    .pill {
      display: inline-flex;
      align-items: center;
      gap: 10px;
      padding: 8px 14px;
      border-radius: 999px;
      border: 1px solid var(--border);
      background: rgba(255, 255, 255, 0.03);
      box-shadow: var(--shadow);
      font-size: 13px;
    }

    .dot {
      width: 9px;
      height: 9px;
      border-radius: 50%;
      background: var(--accent);
      box-shadow: 0 0 10px rgba(76, 195, 138, 0.7);
      animation: pulse 2s infinite;
    }

    main {
      display: grid;
      gap: 20px;
    }

    .cards {
      display: grid;
      gap: 14px;
      grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
    }

    .card {
      background: var(--surface);
      border-radius: var(--radius);
      border: 1px solid var(--border);
      padding: 16px;
      box-shadow: var(--shadow);
    }

    .card h2 {
      margin: 0 0 12px;
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 1.5px;
      color: var(--muted);
    }

    .stat {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 8px 0;
      border-bottom: 1px solid rgba(255, 255, 255, 0.05);
    }

    .stat:last-child { border-bottom: none; }

    .label {
      font-size: 13px;
      color: var(--muted);
    }

    .value {
      font-size: 17px;
      font-weight: 600;
    }

    .value.error { color: var(--danger); }

    .lists {
      display: grid;
      gap: 14px;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
    }

    .list {
      list-style: none;
      margin: 0;
      padding: 0;
      display: grid;
      gap: 10px;
    }

    .list li {
      background: var(--surface-strong);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 12px 14px;
      display: grid;
      gap: 6px;
    }

    .sql {
      font-family: "JetBrains Mono", "Fira Code", monospace;
      font-size: 12px;
      color: #e5e9f2;
      word-break: break-word;
    }

    .meta {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      color: var(--muted);
      font-size: 12px;
    }

    .meta span {
      padding: 2px 8px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.04);
    }

    .reason {
      color: var(--warn);
      font-weight: 600;
      font-size: 12px;
    }

    .error {
      color: var(--danger);
      font-weight: 600;
      font-size: 13px;
    }

    .footer {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      justify-content: space-between;
      color: var(--muted);
      font-size: 12px;
    }

    @keyframes pulse {
      0% { transform: scale(0.9); opacity: 0.6; }
      50% { transform: scale(1.1); opacity: 1; }
      100% { transform: scale(0.9); opacity: 0.6; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <header>
      <div>
        <div class="eyebrow">SwanLake Status</div>
        <h1>DB Performance</h1>
        <div class="sub">Live view of sessions, latency, and recent issues.</div>
      </div>
      <div class="pill">
        <span class="dot"></span>
        <span id="last-refresh">Fetching live metrics...</span>
      </div>
    </header>

    <main>
      <section class="cards">
        <article class="card">
          <h2>Sessions</h2>
          <div class="stat"><span class="label">Active</span><span id="sessions-active" class="value">-</span></div>
          <div class="stat"><span class="label">Max Allowed</span><span id="sessions-max" class="value">-</span></div>
          <div class="stat"><span class="label">Idle Timeout</span><span id="sessions-timeout" class="value">-</span></div>
          <div class="stat"><span class="label">Oldest Idle</span><span id="sessions-oldest" class="value">-</span></div>
          <div class="stat"><span class="label">Avg Idle</span><span id="sessions-avg" class="value">-</span></div>
        </article>

        <article class="card">
          <h2>Throughput</h2>
          <div class="stat"><span class="label">Total Queries</span><span id="total-queries" class="value">-</span></div>
          <div class="stat"><span class="label">Total Updates</span><span id="total-updates" class="value">-</span></div>
          <div class="stat"><span class="label">Errors</span><span id="total-errors" class="value error">-</span></div>
          <div class="stat"><span class="label">Slow Queries</span><span id="total-slow" class="value">-</span></div>
        </article>

        <article class="card">
          <h2>Query Latency</h2>
          <div class="stat"><span class="label">Avg</span><span id="query-avg" class="value">-</span></div>
          <div class="stat"><span class="label">p50 / p95</span><span id="query-p50" class="value">-</span></div>
          <div class="stat"><span class="label">p99 / Max</span><span id="query-p99" class="value">-</span></div>
          <div class="stat"><span class="label">In Flight</span><span id="query-inflight" class="value">-</span></div>
        </article>

        <article class="card">
          <h2>Update Latency</h2>
          <div class="stat"><span class="label">Avg</span><span id="update-avg" class="value">-</span></div>
          <div class="stat"><span class="label">p50 / p95</span><span id="update-p50" class="value">-</span></div>
          <div class="stat"><span class="label">p99 / Max</span><span id="update-p99" class="value">-</span></div>
          <div class="stat"><span class="label">In Flight</span><span id="update-inflight" class="value">-</span></div>
        </article>
      </section>

      <section class="lists">
        <article class="card">
          <h2>Slow Queries</h2>
          <div class="meta"><span id="slow-threshold">Threshold: -</span></div>
          <ul class="list" id="slow-list"></ul>
        </article>

        <article class="card">
          <h2>Recent Errors</h2>
          <ul class="list" id="error-list"></ul>
        </article>
      </section>
    </main>

    <footer class="footer">
      <span id="uptime">Uptime: -</span>
      <span id="history">History window: -</span>
    </footer>
  </div>

  <script>
    const $ = (id) => document.getElementById(id);

    function formatNumber(value) {
      if (value === null || value === undefined) return "-";
      return value.toLocaleString();
    }

    function formatDuration(ms) {
      if (ms === null || ms === undefined) return "-";
      if (ms < 1000) return `${ms} ms`;
      const seconds = Math.round(ms / 1000);
      if (seconds < 60) return `${seconds}s`;
      const minutes = Math.floor(seconds / 60);
      const rem = seconds % 60;
      return `${minutes}m ${rem}s`;
    }

    function formatBytes(bytes) {
      if (bytes === null || bytes === undefined) return "-";
      const units = ["B", "KB", "MB", "GB"];
      let idx = 0;
      let value = bytes;
      while (value >= 1024 && idx < units.length - 1) {
        value /= 1024;
        idx += 1;
      }
      return `${value.toFixed(1)} ${units[idx]}`;
    }

    function escapeHtml(value) {
      if (value === null || value === undefined) return "";
      return String(value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    function renderLatency(prefix, summary) {
      $(`${prefix}-avg`).textContent = formatDuration(summary.avg_ms);
      $(`${prefix}-p50`).textContent = `${formatDuration(summary.p50_ms)} / ${formatDuration(summary.p95_ms)}`;
      $(`${prefix}-p99`).textContent = `${formatDuration(summary.p99_ms)} / ${formatDuration(summary.max_ms)}`;
    }

    function renderSlowQueries(list) {
      const container = $("slow-list");
      container.innerHTML = "";
      if (!list.length) {
        const empty = document.createElement("li");
        empty.innerHTML = '<div class="label">No slow queries recorded.</div>';
        container.appendChild(empty);
        return;
      }
      list.slice(0, 8).forEach((item) => {
        const li = document.createElement("li");
        li.innerHTML = `
          <div class="sql">${escapeHtml(item.sql)}</div>
          <div class="meta">
            <span>${formatDuration(item.duration_ms)}</span>
            <span>${item.is_query ? "query" : "update"}</span>
            <span>${formatNumber(item.rows)} rows</span>
            <span>${formatBytes(item.bytes)}</span>
          </div>
          <div class="reason">${(item.reasons || []).map(escapeHtml).join(" / ") || "Slow execution"}</div>
        `;
        container.appendChild(li);
      });
    }

    function renderErrors(list) {
      const container = $("error-list");
      container.innerHTML = "";
      if (!list.length) {
        const empty = document.createElement("li");
        empty.innerHTML = '<div class="label">No recent errors.</div>';
        container.appendChild(empty);
        return;
      }
      list.slice(0, 8).forEach((item) => {
        const li = document.createElement("li");
        li.innerHTML = `
          <div class="error">${escapeHtml(item.message)}</div>
          ${item.sql ? `<div class="sql">${escapeHtml(item.sql)}</div>` : ""}
          <div class="meta">
            <span>${escapeHtml(item.context)}</span>
            <span>${new Date(item.timestamp_ms).toLocaleTimeString()}</span>
          </div>
        `;
        container.appendChild(li);
      });
    }

    async function refresh() {
      try {
        const res = await fetch("/status.json", { cache: "no-store" });
        if (!res.ok) throw new Error("status fetch failed");
        const data = await res.json();
        const metrics = data.metrics;
        const sessions = data.sessions;

        $("sessions-active").textContent = formatNumber(sessions.total_sessions);
        $("sessions-max").textContent = formatNumber(sessions.max_sessions);
        $("sessions-timeout").textContent = formatDuration(sessions.session_timeout_seconds * 1000);
        $("sessions-oldest").textContent = formatDuration(sessions.oldest_idle_ms);
        $("sessions-avg").textContent = formatDuration(sessions.average_idle_ms);

        $("total-queries").textContent = formatNumber(metrics.totals.queries);
        $("total-updates").textContent = formatNumber(metrics.totals.updates);
        $("total-errors").textContent = formatNumber(metrics.totals.errors);
        $("total-slow").textContent = formatNumber(metrics.totals.slow_queries);

        renderLatency("query", metrics.latency.queries);
        renderLatency("update", metrics.latency.updates);

        $("query-inflight").textContent = formatNumber(metrics.in_flight.queries);
        $("update-inflight").textContent = formatNumber(metrics.in_flight.updates);

        $("slow-threshold").textContent = `Threshold: ${formatDuration(metrics.slow_query_threshold_ms)}`;
        renderSlowQueries(metrics.slow_queries || []);
        renderErrors(metrics.recent_errors || []);

        $("uptime").textContent = `Uptime: ${formatDuration(metrics.uptime_ms)}`;
        $("history").textContent = `History window: ${metrics.history_size}`;
        const lastRefresh = $("last-refresh");
        if (lastRefresh) {
          lastRefresh.textContent = `Last refresh: ${new Date(data.generated_at_ms).toLocaleTimeString()}`;
        }
      } catch (err) {
        const lastRefresh = $("last-refresh");
        if (lastRefresh) {
          lastRefresh.textContent = "Failed to fetch metrics";
        }
      }
    }

    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>"#;

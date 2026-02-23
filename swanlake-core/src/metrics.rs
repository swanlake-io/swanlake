use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tracing::warn;

const DEFAULT_HISTORY_SIZE: usize = 200;
const DEFAULT_SLOW_QUERY_THRESHOLD_MS: u64 = 1_000;
const MAX_SQL_LEN: usize = 2048;

#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    started_at: Instant,
    started_at_ms: u64,
    slow_query_threshold: Duration,
    history_size: usize,
    total_queries: AtomicU64,
    total_updates: AtomicU64,
    total_errors: AtomicU64,
    total_slow_queries: AtomicU64,
    in_flight_queries: AtomicU64,
    in_flight_updates: AtomicU64,
    query_latencies: RwLock<VecDeque<u64>>,
    update_latencies: RwLock<VecDeque<u64>>,
    slow_queries: RwLock<VecDeque<SlowQuery>>,
    recent_errors: RwLock<VecDeque<ErrorEvent>>,
}

#[derive(Clone, Serialize)]
pub struct SlowQuery {
    pub timestamp_ms: u64,
    pub duration_ms: u64,
    pub sql: String,
    pub rows: Option<u64>,
    pub bytes: Option<u64>,
    pub is_query: bool,
    pub reasons: Vec<String>,
}

#[derive(Clone, Serialize)]
pub struct ErrorEvent {
    pub timestamp_ms: u64,
    pub message: String,
    pub sql: Option<String>,
    pub context: String,
}

#[derive(Clone, Serialize)]
pub struct SlowQueryGroup {
    pub sql: String,
    pub is_query: bool,
    pub count: u64,
    pub total_ms: u64,
    pub avg_ms: u64,
    pub max_ms: u64,
    pub latest_timestamp_ms: u64,
}

#[derive(Clone, Serialize)]
pub struct MetricsSnapshot {
    pub started_at_ms: u64,
    pub uptime_ms: u64,
    pub slow_query_threshold_ms: u64,
    pub totals: TotalsSnapshot,
    pub in_flight: InFlightSnapshot,
    pub latency: LatencyStatsSnapshot,
    pub slow_queries: Vec<SlowQuery>,
    pub slow_query_groups: Vec<SlowQueryGroup>,
    pub recent_errors: Vec<ErrorEvent>,
    pub history_size: usize,
}

#[derive(Clone, Serialize)]
pub struct TotalsSnapshot {
    pub queries: u64,
    pub updates: u64,
    pub errors: u64,
    pub slow_queries: u64,
}

#[derive(Clone, Serialize)]
pub struct InFlightSnapshot {
    pub queries: u64,
    pub updates: u64,
}

#[derive(Clone, Serialize)]
pub struct LatencyStatsSnapshot {
    pub queries: LatencySummarySnapshot,
    pub updates: LatencySummarySnapshot,
}

#[derive(Clone, Serialize)]
pub struct LatencySummarySnapshot {
    pub count: usize,
    pub avg_ms: u64,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub p99_ms: u64,
    pub max_ms: u64,
}

#[derive(Clone, Copy)]
enum InFlightKind {
    Query,
    Update,
}

pub struct InFlightGuard {
    inner: Arc<MetricsInner>,
    kind: InFlightKind,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        match self.kind {
            InFlightKind::Query => {
                self.inner.in_flight_queries.fetch_sub(1, Ordering::Relaxed);
            }
            InFlightKind::Update => {
                self.inner.in_flight_updates.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

impl Metrics {
    pub fn new(slow_query_threshold_ms: u64, history_size: usize) -> Self {
        let history_size = if history_size == 0 {
            DEFAULT_HISTORY_SIZE
        } else {
            history_size
        };
        let threshold_ms = if slow_query_threshold_ms == 0 {
            DEFAULT_SLOW_QUERY_THRESHOLD_MS
        } else {
            slow_query_threshold_ms
        };
        let started_at_ms = now_millis();

        Self {
            inner: Arc::new(MetricsInner {
                started_at: Instant::now(),
                started_at_ms,
                slow_query_threshold: Duration::from_millis(threshold_ms),
                history_size,
                total_queries: AtomicU64::new(0),
                total_updates: AtomicU64::new(0),
                total_errors: AtomicU64::new(0),
                total_slow_queries: AtomicU64::new(0),
                in_flight_queries: AtomicU64::new(0),
                in_flight_updates: AtomicU64::new(0),
                query_latencies: RwLock::new(VecDeque::with_capacity(history_size)),
                update_latencies: RwLock::new(VecDeque::with_capacity(history_size)),
                slow_queries: RwLock::new(VecDeque::with_capacity(history_size)),
                recent_errors: RwLock::new(VecDeque::with_capacity(history_size)),
            }),
        }
    }

    pub fn start_query(&self) -> InFlightGuard {
        self.inner.in_flight_queries.fetch_add(1, Ordering::Relaxed);
        InFlightGuard {
            inner: self.inner.clone(),
            kind: InFlightKind::Query,
        }
    }

    pub fn start_update(&self) -> InFlightGuard {
        self.inner.in_flight_updates.fetch_add(1, Ordering::Relaxed);
        InFlightGuard {
            inner: self.inner.clone(),
            kind: InFlightKind::Update,
        }
    }

    pub fn record_query_success(&self, sql: &str, duration: Duration, rows: usize, bytes: usize) {
        self.record_query_result(sql, duration, Some(rows as u64), Some(bytes as u64), None);
    }

    pub fn record_query_error(&self, sql: &str, duration: Duration, message: String) {
        self.record_query_result(sql, duration, None, None, Some(message));
    }

    pub fn record_update_success(&self, sql: &str, duration: Duration, affected_rows: Option<i64>) {
        let rows =
            affected_rows.and_then(|value| if value >= 0 { Some(value as u64) } else { None });
        self.record_update_result(sql, duration, rows, None);
    }

    pub fn record_update_error(&self, sql: &str, duration: Duration, message: String) {
        self.record_update_result(sql, duration, None, Some(message));
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let uptime_ms = self.inner.started_at.elapsed().as_millis() as u64;
        let totals = TotalsSnapshot {
            queries: self.inner.total_queries.load(Ordering::Relaxed),
            updates: self.inner.total_updates.load(Ordering::Relaxed),
            errors: self.inner.total_errors.load(Ordering::Relaxed),
            slow_queries: self.inner.total_slow_queries.load(Ordering::Relaxed),
        };
        let in_flight = InFlightSnapshot {
            queries: self.inner.in_flight_queries.load(Ordering::Relaxed),
            updates: self.inner.in_flight_updates.load(Ordering::Relaxed),
        };

        let query_latencies = self
            .inner
            .query_latencies
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let update_latencies = self
            .inner
            .update_latencies
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .copied()
            .collect::<Vec<_>>();

        let slow_queries = self
            .inner
            .slow_queries
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .rev()
            .cloned()
            .collect::<Vec<_>>();
        let recent_errors = self
            .inner
            .recent_errors
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .rev()
            .cloned()
            .collect::<Vec<_>>();
        let slow_query_groups = summarize_slow_query_groups(&slow_queries, 10);

        MetricsSnapshot {
            started_at_ms: self.inner.started_at_ms,
            uptime_ms,
            slow_query_threshold_ms: self.inner.slow_query_threshold.as_millis() as u64,
            totals,
            in_flight,
            latency: LatencyStatsSnapshot {
                queries: summarize_latencies(query_latencies),
                updates: summarize_latencies(update_latencies),
            },
            slow_queries,
            slow_query_groups,
            recent_errors,
            history_size: self.inner.history_size,
        }
    }

    fn record_query_result(
        &self,
        sql: &str,
        duration: Duration,
        rows: Option<u64>,
        bytes: Option<u64>,
        error: Option<String>,
    ) {
        self.inner.total_queries.fetch_add(1, Ordering::Relaxed);
        self.push_latency(&self.inner.query_latencies, duration);
        self.record_common(sql, duration, rows, bytes, true, error);
    }

    fn record_update_result(
        &self,
        sql: &str,
        duration: Duration,
        rows: Option<u64>,
        error: Option<String>,
    ) {
        self.inner.total_updates.fetch_add(1, Ordering::Relaxed);
        self.push_latency(&self.inner.update_latencies, duration);
        self.record_common(sql, duration, rows, None, false, error);
    }

    fn record_common(
        &self,
        sql: &str,
        duration: Duration,
        rows: Option<u64>,
        bytes: Option<u64>,
        is_query: bool,
        error: Option<String>,
    ) {
        let duration_ms = duration.as_millis() as u64;
        let truncated_sql = compact_sql(sql);
        let had_error = error.is_some();

        if let Some(message) = error {
            self.record_error("execution", Some(&truncated_sql), message);
        }

        if duration >= self.inner.slow_query_threshold {
            self.inner
                .total_slow_queries
                .fetch_add(1, Ordering::Relaxed);
            let threshold_ms = self.inner.slow_query_threshold.as_millis() as u64;
            let reasons = infer_reasons(
                &truncated_sql,
                is_query,
                rows,
                bytes,
                duration_ms,
                threshold_ms,
                had_error,
            );
            warn!(
                duration_ms,
                threshold_ms,
                is_query,
                rows = ?rows,
                bytes = ?bytes,
                sql = %truncated_sql,
                reasons = ?reasons,
                "slow statement recorded"
            );
            let slow_query = SlowQuery {
                timestamp_ms: now_millis(),
                duration_ms,
                sql: truncated_sql,
                rows,
                bytes,
                is_query,
                reasons,
            };
            let mut slow_queries = self
                .inner
                .slow_queries
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            push_ring(&mut slow_queries, slow_query, self.inner.history_size);
        }
    }

    fn record_error(&self, context: &str, sql: Option<&str>, message: String) {
        self.inner.total_errors.fetch_add(1, Ordering::Relaxed);
        let event = ErrorEvent {
            timestamp_ms: now_millis(),
            message,
            sql: sql.map(str::to_string),
            context: context.to_string(),
        };
        let mut errors = self
            .inner
            .recent_errors
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        push_ring(&mut errors, event, self.inner.history_size);
    }

    fn push_latency(&self, target: &RwLock<VecDeque<u64>>, duration: Duration) {
        let mut latencies = target
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        push_ring(
            &mut latencies,
            duration.as_millis() as u64,
            self.inner.history_size,
        );
    }
}

fn push_ring<T>(target: &mut VecDeque<T>, value: T, max: usize) {
    if target.len() >= max {
        target.pop_front();
    }
    target.push_back(value);
}

fn summarize_latencies(values: Vec<u64>) -> LatencySummarySnapshot {
    if values.is_empty() {
        return LatencySummarySnapshot {
            count: 0,
            avg_ms: 0,
            p50_ms: 0,
            p95_ms: 0,
            p99_ms: 0,
            max_ms: 0,
        };
    }

    let count = values.len();
    let sum: u64 = values.iter().sum();
    let avg_ms = sum / count as u64;

    let mut sorted = values;
    sorted.sort_unstable();
    let max_ms = *sorted.last().unwrap_or(&0);

    let p50_ms = percentile(&sorted, 0.50);
    let p95_ms = percentile(&sorted, 0.95);
    let p99_ms = percentile(&sorted, 0.99);

    LatencySummarySnapshot {
        count,
        avg_ms,
        p50_ms,
        p95_ms,
        p99_ms,
        max_ms,
    }
}

fn summarize_slow_query_groups(slow_queries: &[SlowQuery], limit: usize) -> Vec<SlowQueryGroup> {
    let mut grouped: HashMap<(bool, String), SlowQueryGroup> = HashMap::new();
    for query in slow_queries {
        let key = (query.is_query, query.sql.clone());
        let entry = grouped.entry(key).or_insert_with(|| SlowQueryGroup {
            sql: query.sql.clone(),
            is_query: query.is_query,
            count: 0,
            total_ms: 0,
            avg_ms: 0,
            max_ms: 0,
            latest_timestamp_ms: query.timestamp_ms,
        });
        entry.count = entry.count.saturating_add(1);
        entry.total_ms = entry.total_ms.saturating_add(query.duration_ms);
        entry.max_ms = entry.max_ms.max(query.duration_ms);
        entry.latest_timestamp_ms = entry.latest_timestamp_ms.max(query.timestamp_ms);
    }

    let mut out = grouped.into_values().collect::<Vec<_>>();
    for group in &mut out {
        if group.count > 0 {
            group.avg_ms = group.total_ms / group.count;
        }
    }

    out.sort_unstable_by(|a, b| {
        b.total_ms
            .cmp(&a.total_ms)
            .then_with(|| b.max_ms.cmp(&a.max_ms))
            .then_with(|| b.count.cmp(&a.count))
    });
    if out.len() > limit {
        out.truncate(limit);
    }
    out
}

fn percentile(sorted: &[u64], quantile: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (quantile * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

fn compact_sql(sql: &str) -> String {
    let collapsed = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    truncate_ascii_like(collapsed, MAX_SQL_LEN)
}

fn truncate_ascii_like(value: String, max_len: usize) -> String {
    let mut out = String::new();
    for ch in value.chars().take(max_len) {
        out.push(ch);
    }
    if value.chars().count() > max_len {
        out.push_str("...");
    }
    out
}

fn infer_reasons(
    sql: &str,
    is_query: bool,
    rows: Option<u64>,
    bytes: Option<u64>,
    duration_ms: u64,
    slow_threshold_ms: u64,
    had_error: bool,
) -> Vec<String> {
    let mut reasons = Vec::new();
    let lower = sql.to_ascii_lowercase();

    if let Some(rows) = rows {
        if rows >= 100_000 {
            reasons.push("Large result set".to_string());
        }
    }

    if let Some(bytes) = bytes {
        if bytes >= 50 * 1024 * 1024 {
            reasons.push("Large payload".to_string());
        }
    }

    if lower.contains(" join ")
        || lower.contains(" group by ")
        || lower.contains(" order by ")
        || lower.contains(" distinct ")
        || lower.contains(" union ")
        || lower.contains(" window ")
    {
        reasons.push("Join/aggregation/sort".to_string());
    }

    if lower.contains("select *") {
        reasons.push("Wide select".to_string());
    }

    if lower.contains(" like '%") || lower.contains(" ilike '%") {
        reasons.push("Leading wildcard match".to_string());
    }

    if !is_query {
        reasons.push("Write-heavy statement".to_string());
    }

    if duration_ms >= slow_threshold_ms.saturating_mul(3) {
        reasons.push("Very long-running".to_string());
    }

    if had_error {
        reasons.push("Errored before completion".to_string());
    }

    reasons
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::time::Duration;

    use super::*;

    #[test]
    fn metrics_defaults_apply_for_zero_values() {
        let metrics = Metrics::new(0, 0);
        let snapshot = metrics.snapshot();

        assert_eq!(
            snapshot.slow_query_threshold_ms,
            DEFAULT_SLOW_QUERY_THRESHOLD_MS
        );
        assert_eq!(snapshot.history_size, DEFAULT_HISTORY_SIZE);
    }

    #[test]
    fn in_flight_guards_increment_and_decrement_counts() {
        let metrics = Metrics::new(100, 8);
        assert_eq!(metrics.snapshot().in_flight.queries, 0);
        assert_eq!(metrics.snapshot().in_flight.updates, 0);

        {
            let _query_guard = metrics.start_query();
            let _update_guard = metrics.start_update();
            let snapshot = metrics.snapshot();
            assert_eq!(snapshot.in_flight.queries, 1);
            assert_eq!(snapshot.in_flight.updates, 1);
        }

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.in_flight.queries, 0);
        assert_eq!(snapshot.in_flight.updates, 0);
    }

    #[test]
    fn record_paths_update_totals_and_ring_histories() {
        let metrics = Metrics::new(10, 2);

        metrics.record_query_success("SELECT 1", Duration::from_millis(5), 1, 16);
        metrics.record_query_error(
            "SELECT * FROM t",
            Duration::from_millis(15),
            "query failed".to_string(),
        );
        metrics.record_query_success(
            "SELECT * FROM big_table JOIN dim USING (id) ORDER BY id",
            Duration::from_millis(35),
            120_000,
            60 * 1024 * 1024,
        );
        metrics.record_update_success(
            "UPDATE t SET value = 1 WHERE name LIKE '%abc'",
            Duration::from_millis(20),
            Some(-1),
        );
        metrics.record_update_error(
            "DELETE FROM t WHERE id = 10",
            Duration::from_millis(25),
            "delete failed".to_string(),
        );

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.totals.queries, 3);
        assert_eq!(snapshot.totals.updates, 2);
        assert_eq!(snapshot.totals.errors, 2);
        assert_eq!(snapshot.totals.slow_queries, 4);

        // history_size=2 caps latency histories and event rings.
        assert_eq!(snapshot.latency.queries.count, 2);
        assert_eq!(snapshot.latency.updates.count, 2);
        assert_eq!(snapshot.recent_errors.len(), 2);
        assert_eq!(snapshot.slow_queries.len(), 2);
        assert!(snapshot.slow_query_groups.len() <= 2);
    }

    #[test]
    fn summarize_latencies_handles_empty_and_percentiles() {
        let empty = summarize_latencies(vec![]);
        assert_eq!(empty.count, 0);
        assert_eq!(empty.avg_ms, 0);
        assert_eq!(empty.p50_ms, 0);
        assert_eq!(empty.max_ms, 0);

        let summary = summarize_latencies(vec![100, 10, 30, 20]);
        assert_eq!(summary.count, 4);
        assert_eq!(summary.avg_ms, 40);
        assert_eq!(summary.p50_ms, 30);
        assert_eq!(summary.p95_ms, 100);
        assert_eq!(summary.p99_ms, 100);
        assert_eq!(summary.max_ms, 100);
    }

    #[test]
    fn slow_query_group_summary_aggregates_and_truncates() {
        let queries = vec![
            SlowQuery {
                timestamp_ms: 1,
                duration_ms: 100,
                sql: "SELECT 1".to_string(),
                rows: Some(1),
                bytes: Some(8),
                is_query: true,
                reasons: vec!["A".to_string()],
            },
            SlowQuery {
                timestamp_ms: 3,
                duration_ms: 50,
                sql: "SELECT 1".to_string(),
                rows: Some(1),
                bytes: Some(8),
                is_query: true,
                reasons: vec!["B".to_string()],
            },
            SlowQuery {
                timestamp_ms: 2,
                duration_ms: 200,
                sql: "UPDATE t SET v = 1".to_string(),
                rows: Some(1),
                bytes: None,
                is_query: false,
                reasons: vec!["C".to_string()],
            },
        ];

        let grouped = summarize_slow_query_groups(&queries, 10);
        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped[0].sql, "UPDATE t SET v = 1");
        assert_eq!(grouped[0].total_ms, 200);
        assert_eq!(grouped[1].sql, "SELECT 1");
        assert_eq!(grouped[1].count, 2);
        assert_eq!(grouped[1].avg_ms, 75);
        assert_eq!(grouped[1].latest_timestamp_ms, 3);

        let limited = summarize_slow_query_groups(&queries, 1);
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].sql, "UPDATE t SET v = 1");
    }

    #[test]
    fn sql_compaction_and_truncation_are_stable() {
        let compacted = compact_sql("SELECT   *\nFROM   test_table\tWHERE  id = 1");
        assert_eq!(compacted, "SELECT * FROM test_table WHERE id = 1");

        let exact = truncate_ascii_like("abc".to_string(), 3);
        assert_eq!(exact, "abc");

        let truncated = truncate_ascii_like("abcdef".to_string(), 3);
        assert_eq!(truncated, "abc...");

        let long_sql = format!("SELECT {}", "x".repeat(MAX_SQL_LEN + 32));
        let compacted_long = compact_sql(&long_sql);
        assert!(compacted_long.ends_with("..."));
        assert!(compacted_long.chars().count() <= MAX_SQL_LEN + 3);
    }

    #[test]
    fn infer_reasons_reports_expected_signals() {
        let reasons = infer_reasons(
            "SELECT * FROM t JOIN u ON t.id = u.id WHERE name LIKE '%abc' GROUP BY t.id ORDER BY t.id UNION SELECT * FROM v WINDOW w AS (PARTITION BY id)",
            false,
            Some(100_000),
            Some(50 * 1024 * 1024),
            4_000,
            1_000,
            true,
        );

        assert!(reasons.contains(&"Large result set".to_string()));
        assert!(reasons.contains(&"Large payload".to_string()));
        assert!(reasons.contains(&"Join/aggregation/sort".to_string()));
        assert!(reasons.contains(&"Wide select".to_string()));
        assert!(reasons.contains(&"Leading wildcard match".to_string()));
        assert!(reasons.contains(&"Write-heavy statement".to_string()));
        assert!(reasons.contains(&"Very long-running".to_string()));
        assert!(reasons.contains(&"Errored before completion".to_string()));

        let no_reasons = infer_reasons(
            "SELECT id FROM t WHERE id = 1",
            true,
            Some(1),
            Some(8),
            100,
            1_000,
            false,
        );
        assert!(no_reasons.is_empty());
    }

    #[test]
    fn push_ring_obeys_capacity_and_now_millis_is_positive() {
        let mut ring = VecDeque::new();
        push_ring(&mut ring, 1, 2);
        push_ring(&mut ring, 2, 2);
        push_ring(&mut ring, 3, 2);
        assert_eq!(ring.into_iter().collect::<Vec<_>>(), vec![2, 3]);

        assert!(now_millis() > 0);
    }
}

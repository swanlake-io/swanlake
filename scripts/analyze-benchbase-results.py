#!/usr/bin/env python3
"""Analyze BenchBase outputs and SwanLake server logs to highlight bottlenecks."""

from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SLOW_ENGINE_RE = re.compile(
    r"engine/connection\.rs:(?P<site>83|118): close time\.busy="
    r"(?P<value>[0-9.]+)(?P<unit>[a-zA-Zµ]+).*? sql=(?P<sql>.+)"
)
TIMESTAMP_LINE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T")
TAG_RE_CACHE: dict[str, re.Pattern[str]] = {}


@dataclass
class WindowPoint:
    time_s: float
    throughput: float
    avg_latency_ms: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze BenchBase result files and SwanLake server logs."
    )
    parser.add_argument(
        "--summary-file",
        type=Path,
        help="Path to a BenchBase *.summary.json file. If omitted, --results-dir is required.",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        help="BenchBase results directory used to auto-discover the latest summary file.",
    )
    parser.add_argument(
        "--benchmark",
        default="tpch",
        help="Benchmark prefix used when auto-discovering summary files (default: tpch).",
    )
    parser.add_argument(
        "--server-log",
        type=Path,
        help="Optional SwanLake server log path to include engine-side SQL bottlenecks.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        help="Optional path to write structured JSON analysis.",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def find_latest_summary(results_dir: Path, benchmark: str) -> Path:
    candidates = sorted(results_dir.glob(f"{benchmark}_*.summary.json"))
    if not candidates:
        raise FileNotFoundError(
            f"no summary files matching '{benchmark}_*.summary.json' under {results_dir}"
        )
    return max(candidates, key=lambda p: p.stat().st_mtime)


def parse_results_csv(path: Path) -> list[WindowPoint]:
    points: list[WindowPoint] = []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            lat = safe_float(row.get("Average Latency (millisecond)", ""))
            thr = safe_float(row.get("Throughput (requests/second)", ""))
            t = safe_float(row.get("Time (seconds)", ""))
            if lat is None or thr is None or t is None:
                continue
            if lat < 0:
                continue
            points.append(WindowPoint(time_s=t, throughput=thr, avg_latency_ms=lat))
    return points


def infer_window_seconds(points: list[WindowPoint]) -> float:
    if len(points) < 2:
        return 5.0
    times = sorted({p.time_s for p in points})
    deltas = [b - a for a, b in zip(times, times[1:]) if (b - a) > 0]
    if not deltas:
        return 5.0
    return min(deltas)


def parse_query_breakdown(prefix: str) -> list[dict[str, Any]]:
    q_files = sorted(Path(f"{prefix}.results.csv").parent.glob(Path(prefix).name + ".results.Q*.csv"))
    out: list[dict[str, Any]] = []
    for q_file in q_files:
        query_name = q_file.stem.split(".")[-1]
        points = parse_results_csv(q_file)
        if not points:
            continue
        window_s = infer_window_seconds(points)

        req_sum = 0.0
        weighted_ms = 0.0
        hot_req_sum = 0.0
        hot_weighted_ms = 0.0
        max_ms = 0.0
        for p in points:
            req = p.throughput * window_s
            if req <= 0:
                continue
            req_sum += req
            weighted_ms += req * p.avg_latency_ms
            if p.avg_latency_ms <= 5000:
                hot_req_sum += req
                hot_weighted_ms += req * p.avg_latency_ms
            max_ms = max(max_ms, p.avg_latency_ms)

        if req_sum <= 0:
            continue
        avg_ms = weighted_ms / req_sum
        hot_avg_ms = (hot_weighted_ms / hot_req_sum) if hot_req_sum > 0 else None
        contribution_s = weighted_ms / 1000.0
        out.append(
            {
                "query": query_name,
                "requests_estimate": req_sum,
                "weighted_avg_ms": avg_ms,
                "hot_avg_ms": hot_avg_ms,
                "max_window_avg_ms": max_ms,
                "contribution_seconds": contribution_s,
            }
        )

    total_s = sum(item["contribution_seconds"] for item in out)
    for item in out:
        item["contribution_pct"] = (
            (item["contribution_seconds"] / total_s) * 100.0 if total_s > 0 else 0.0
        )
    out.sort(key=lambda x: x["contribution_seconds"], reverse=True)
    return out


def parse_tag(xml_text: str, tag: str) -> str | None:
    pat = TAG_RE_CACHE.get(tag)
    if pat is None:
        pat = re.compile(rf"<{tag}>(.*?)</{tag}>", re.IGNORECASE | re.DOTALL)
        TAG_RE_CACHE[tag] = pat
    match = pat.search(xml_text)
    if not match:
        return None
    return match.group(1).strip()


def parse_config(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    text = path.read_text(encoding="utf-8")
    out: dict[str, Any] = {}
    for key in ("warmup", "time", "terminals", "scalefactor"):
        value = parse_tag(text, key)
        if value is not None:
            out[key] = value
    return out


def parse_duration_to_ms(value: float, unit: str) -> float | None:
    unit = unit.lower()
    if unit == "s":
        return value * 1000.0
    if unit == "ms":
        return value
    if unit in ("us", "µs"):
        return value / 1000.0
    if unit == "ns":
        return value / 1_000_000.0
    return None


def normalize_sql(sql: str) -> str:
    cleaned = sql.split(" sql=", 1)[0]
    cleaned = cleaned.split(" param_count=", 1)[0]
    cleaned = " ".join(cleaned.split())
    if len(cleaned) > 240:
        cleaned = cleaned[:240] + "..."
    return cleaned or "<unknown>"


def parse_server_log(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()

    by_sql: dict[str, dict[str, Any]] = {}
    i = 0
    while i < len(lines):
        line = lines[i]
        match = SLOW_ENGINE_RE.search(line)
        if not match:
            i += 1
            continue

        value = float(match.group("value"))
        duration_ms = parse_duration_to_ms(value, match.group("unit"))
        if duration_ms is None:
            i += 1
            continue

        sql = match.group("sql")
        if " param_count=" not in sql:
            j = i + 1
            extra_parts: list[str] = []
            while j < len(lines) and not TIMESTAMP_LINE_RE.match(lines[j]):
                extra_parts.append(lines[j].strip())
                j += 1
            if extra_parts:
                sql = f"{sql} {' '.join(extra_parts)}"
            i = j
        else:
            i += 1

        sql_norm = normalize_sql(sql)
        record = by_sql.setdefault(
            sql_norm,
            {"sql": sql_norm, "count": 0, "total_ms": 0.0, "max_ms": 0.0},
        )
        record["count"] += 1
        record["total_ms"] += duration_ms
        record["max_ms"] = max(record["max_ms"], duration_ms)

    rows = list(by_sql.values())
    rows.sort(key=lambda r: r["total_ms"], reverse=True)
    return rows


def phase_stats(points: list[WindowPoint]) -> dict[str, Any] | None:
    if len(points) < 6:
        return None
    chunk = max(1, len(points) // 3)
    early = points[:chunk]
    late = points[-chunk:]

    def stats(items: list[WindowPoint]) -> dict[str, float]:
        return {
            "avg_throughput": statistics.fmean(p.throughput for p in items),
            "avg_latency_ms": statistics.fmean(p.avg_latency_ms for p in items),
        }

    return {"early": stats(early), "late": stats(late), "windows_per_phase": chunk}


def safe_float(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def fmt_ms(ms: float | None) -> str:
    if ms is None:
        return "-"
    if ms < 1000:
        return f"{ms:.1f}ms"
    return f"{ms / 1000.0:.2f}s"


def print_report(data: dict[str, Any]) -> None:
    summary = data["summary"]
    cfg = data.get("config", {})

    print("SwanLake BenchBase Bottleneck Report")
    print(f"Run: {data['run_prefix']}")
    print(f"Summary: {data['summary_file']}")
    if data.get("server_log"):
        print(f"Server log: {data['server_log']}")
    print()

    print("Overall")
    print(
        f"- Throughput: {summary.get('Throughput (requests/second)', 0):.3f} req/s"
        f" | Goodput: {summary.get('Goodput (requests/second)', 0):.3f} req/s"
    )
    print(
        f"- Latency avg/median/p95/p99/max:"
        f" {fmt_ms(summary.get('Latency Distribution', {}).get('Average Latency (microseconds)', 0) / 1000.0)}"
        f" / {fmt_ms(summary.get('Latency Distribution', {}).get('Median Latency (microseconds)', 0) / 1000.0)}"
        f" / {fmt_ms(summary.get('Latency Distribution', {}).get('95th Percentile Latency (microseconds)', 0) / 1000.0)}"
        f" / {fmt_ms(summary.get('Latency Distribution', {}).get('99th Percentile Latency (microseconds)', 0) / 1000.0)}"
        f" / {fmt_ms(summary.get('Latency Distribution', {}).get('Maximum Latency (microseconds)', 0) / 1000.0)}"
    )
    print(f"- Measured requests: {summary.get('Measured Requests', 0)}")
    if cfg:
        print(
            f"- Config: warmup={cfg.get('warmup', '?')}s,"
            f" run={cfg.get('time', '?')}s,"
            f" terminals={cfg.get('terminals', '?')},"
            f" scalefactor={cfg.get('scalefactor', '?')}"
        )
    print()

    phase = data.get("phase")
    if phase:
        print("Phase Split")
        print(
            f"- Early phase (first {phase['windows_per_phase']} windows):"
            f" throughput={phase['early']['avg_throughput']:.2f} req/s,"
            f" avg-lat={fmt_ms(phase['early']['avg_latency_ms'])}"
        )
        print(
            f"- Late phase (last {phase['windows_per_phase']} windows):"
            f" throughput={phase['late']['avg_throughput']:.2f} req/s,"
            f" avg-lat={fmt_ms(phase['late']['avg_latency_ms'])}"
        )
        print()

    print("Top Query Contributors (estimated by windowed req/s x avg-lat)")
    top_queries = data.get("query_breakdown", [])[:8]
    if not top_queries:
        print("- No per-query files found")
    else:
        for idx, item in enumerate(top_queries, start=1):
            hot_avg = item["hot_avg_ms"]
            hot_fragment = f", hot-avg={fmt_ms(hot_avg)}" if hot_avg is not None else ""
            print(
                f"{idx}. {item['query']}: share={item['contribution_pct']:.1f}%,"
                f" total={item['contribution_seconds']:.2f}s,"
                f" avg={fmt_ms(item['weighted_avg_ms'])}"
                f"{hot_fragment}, max-window-avg={fmt_ms(item['max_window_avg_ms'])}"
            )
    print()

    print("Top Slow SQL From Server Log (engine close events)")
    slow_sql = [row for row in data.get("server_sql", []) if row["max_ms"] >= 1000][:8]
    if not slow_sql:
        print("- No >=1s engine close events found")
    else:
        for idx, row in enumerate(slow_sql, start=1):
            print(
                f"{idx}. total={fmt_ms(row['total_ms'])}, count={row['count']},"
                f" max={fmt_ms(row['max_ms'])}, sql={row['sql']}"
            )
    print()

    print("Follow-up")
    for idx, tip in enumerate(build_follow_up(data), start=1):
        print(f"{idx}. {tip}")


def build_follow_up(data: dict[str, Any]) -> list[str]:
    tips: list[str] = []
    phase = data.get("phase")
    cfg = data.get("config", {})
    warmup = safe_float(cfg.get("warmup"))
    if phase:
        early_lat = phase["early"]["avg_latency_ms"]
        late_lat = phase["late"]["avg_latency_ms"]
        if late_lat > 0 and early_lat / late_lat >= 2.0:
            if warmup is not None and warmup < 120:
                tips.append(
                    "Increase TPCH warmup to at least 180-300s; current run still includes cold-start cost."
                )
            else:
                tips.append(
                    "Keep reporting cold and warm phases separately; cold-start dominates early windows."
                )

    top_queries = data.get("query_breakdown", [])
    if top_queries:
        top = ", ".join(item["query"] for item in top_queries[:5])
        tips.append(f"Prioritize EXPLAIN ANALYZE on top queries first: {top}.")

    slow_sql = [row for row in data.get("server_sql", []) if row["max_ms"] >= 5000]
    if slow_sql:
        tips.append(
            "Investigate long engine-side statements (>5s) from server log and compare S3 vs local/cached runs."
        )

    tips.append(
        "For S3 workloads, test with BENCHBASE_ENABLE_CACHE_HTTPFS=true and compare throughput + p95/p99 across at least 3 runs."
    )
    return tips


def main() -> int:
    args = parse_args()

    summary_path: Path
    if args.summary_file:
        summary_path = args.summary_file
    elif args.results_dir:
        summary_path = find_latest_summary(args.results_dir, args.benchmark)
    else:
        raise SystemExit("either --summary-file or --results-dir must be provided")

    if not summary_path.exists():
        raise SystemExit(f"summary file not found: {summary_path}")

    run_prefix = str(summary_path)
    if run_prefix.endswith(".summary.json"):
        run_prefix = run_prefix[: -len(".summary.json")]
    summary = load_json(summary_path)
    results_csv = Path(f"{run_prefix}.results.csv")
    if not results_csv.exists():
        raise SystemExit(f"results file not found: {results_csv}")

    config_path = Path(f"{run_prefix}.config.xml")
    points = parse_results_csv(results_csv)
    query_breakdown = parse_query_breakdown(run_prefix)

    server_log = args.server_log
    server_sql: list[dict[str, Any]] = []
    if server_log:
        server_sql = parse_server_log(server_log)

    report = {
        "run_prefix": run_prefix,
        "summary_file": str(summary_path),
        "server_log": str(server_log) if server_log else None,
        "summary": summary,
        "config": parse_config(config_path),
        "phase": phase_stats(points),
        "query_breakdown": query_breakdown,
        "server_sql": server_sql,
    }

    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print_report(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

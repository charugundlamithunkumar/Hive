#!/usr/bin/env python3
"""Reporting module: reads ONLY from MySQL and prints results + execution metadata.

    python reporting/cli_report.py --run-id 5
    python reporting/cli_report.py                 (latest successful run)
    python reporting/cli_report.py --pipeline mongodb
    python reporting/cli_report.py --compare       (all runs side by side)
"""
import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from tabulate import tabulate  # noqa: E402

from orchestrator.config import load_config  # noqa: E402
from orchestrator.db_loader import ResultStore  # noqa: E402

FMT = "psql"


def fetch(cur, sql, params=()):
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return cols, [list(r) for r in cur.fetchall()]


def one(cur, sql, params=()):
    cols, rows = fetch(cur, sql, params)
    return dict(zip(cols, rows[0])) if rows else None


def show_table(title, cols, rows, limit):
    print("\n=== %s ===" % title)
    shown = rows if limit is None else rows[:limit]
    print(tabulate(shown, headers=cols, tablefmt=FMT, floatfmt=".6f"))
    if limit is not None and len(rows) > limit:
        print("... %d of %d rows shown (use --all for every row)" % (limit, len(rows)))


def results_hash(cur, run_id):
    """Short hash of the three result sets (identical results => identical hash)."""
    h = hashlib.sha256()
    for sql in (
        "SELECT log_date, status_code, request_count, total_bytes FROM q1_daily_traffic"
        " WHERE run_id=%s ORDER BY log_date, status_code",
        "SELECT resource_path, request_count, total_bytes, distinct_host_count FROM q2_top_resources"
        " WHERE run_id=%s ORDER BY rank_no",
        "SELECT log_date, log_hour, error_request_count, total_request_count, ROUND(error_rate, 9),"
        " distinct_error_hosts FROM q3_hourly_errors WHERE run_id=%s ORDER BY log_date, log_hour"):
        _, rows = fetch(cur, sql, (run_id,))
        h.update(json.dumps(rows, default=str).encode("utf-8"))
    return h.hexdigest()[:10]


def report_run(cur, run_id, limit, show_batches):
    run = one(cur, "SELECT * FROM pipeline_runs WHERE run_id=%s", (run_id,))
    if not run:
        sys.exit("run_id %s not found" % run_id)
    last_batch = one(cur, "SELECT MAX(batch_id) AS k FROM run_batches WHERE run_id=%s", (run_id,))["k"]
    print("=== Execution Metadata ===")
    print(tabulate([
        ["Run ID", run["run_id"]], ["Pipeline", run["pipeline_name"]], ["Status", run["status"]],
        ["Batch size (B)", run["batch_size"]], ["Total records", run["total_records"]],
        ["Malformed records", run["malformed_records"]],
        ["Total batches (non-empty)", run["total_batches"]],
        ["Average batch size", "%.1f" % run["avg_batch_size"]],
        ["Batch ID on result rows (K)", last_batch],
        ["Started", run["started_at"]], ["Finished", run["finished_at"]],
        ["Total runtime (s)", "%.3f" % run["runtime_seconds"]],
    ], tablefmt=FMT))

    cols, rows = fetch(cur, "SELECT stage_name AS query_or_stage, runtime_seconds, output_rows"
                            " FROM stage_metrics WHERE run_id=%s ORDER BY FIELD(stage_name,'load','parse',"
                            "'q1_daily_traffic','q2_top_resources','q3_hourly_errors','result_load')", (run_id,))
    show_table("Runtime per query / stage (seconds)", cols, rows, None)

    if show_batches:
        cols, rows = fetch(cur, "SELECT batch_id, record_count, malformed_count FROM run_batches"
                                " WHERE run_id=%s ORDER BY batch_id", (run_id,))
        shown = rows if limit is None or len(rows) <= 8 else rows[:4] + [["...", "...", "..."]] + rows[-4:]
        print("\n=== Per-batch statistics (%d batches) ===" % len(rows))
        print(tabulate(shown, headers=cols, tablefmt=FMT))

    cols, rows = fetch(cur, "SELECT log_date, status_code, request_count, total_bytes FROM q1_daily_traffic"
                            " WHERE run_id=%s ORDER BY log_date, status_code", (run_id,))
    show_table("Query 1: Daily Traffic Summary", cols, rows, limit)
    cols, rows = fetch(cur, "SELECT resource_path, request_count, total_bytes, distinct_host_count"
                            " FROM q2_top_resources WHERE run_id=%s ORDER BY rank_no", (run_id,))
    show_table("Query 2: Top 20 Requested Resources", cols, rows, None)
    cols, rows = fetch(cur, "SELECT log_date, log_hour, error_request_count, total_request_count, error_rate,"
                            " distinct_error_hosts FROM q3_hourly_errors WHERE run_id=%s"
                            " ORDER BY log_date, log_hour", (run_id,))
    show_table("Query 3: Hourly Error Analysis", cols, rows, limit)


def report_compare(cur):
    cols, rows = fetch(cur, "SELECT run_id, pipeline_name, batch_size, total_records, malformed_records,"
                            " total_batches, ROUND(avg_batch_size,1) AS avg_batch_size,"
                            " ROUND(runtime_seconds,3) AS runtime_s, status FROM pipeline_runs"
                            " ORDER BY run_id")
    for r in rows:
        r.append(results_hash(cur, r[0]) if r[-1] == "success" else "-")
    print("=== All runs (same results hash => identical Q1-Q3 output) ===")
    print(tabulate(rows, headers=cols + ["results_hash"], tablefmt=FMT))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run-id", type=int)
    ap.add_argument("--pipeline")
    ap.add_argument("--compare", action="store_true")
    ap.add_argument("--rows", type=int, default=15, help="rows shown for Q1/Q3 (default 15)")
    ap.add_argument("--all", action="store_true", help="show every query row and every batch with --batches")
    ap.add_argument("--batches", action="store_true", help="also show per-batch statistics")
    args = ap.parse_args()

    con = ResultStore(load_config()).connect()
    cur = con.cursor()
    if args.compare:
        report_compare(cur)
    else:
        run_id = args.run_id
        if run_id is None:
            if args.pipeline:
                cur.execute("SELECT MAX(run_id) FROM pipeline_runs WHERE status='success' AND pipeline_name=%s",
                            (args.pipeline,))
            else:
                cur.execute("SELECT MAX(run_id) FROM pipeline_runs WHERE status='success'")
            run_id = cur.fetchone()[0]
            if run_id is None:
                sys.exit("no successful runs found")
        report_run(cur, run_id, None if args.all else args.rows, args.batches)
    con.close()


if __name__ == "__main__":
    main()

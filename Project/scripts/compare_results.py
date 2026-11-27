#!/usr/bin/env python3
"""Compare one run stored in MySQL against the independent reference oracle.

    python scripts/compare_results.py --run-id 3 [--files FILE ... | --data-dir DIR]

Checks: totals, malformed count, per-batch record counts (and malformed counts),
and Q1/Q2/Q3 row by row (integers exact, error_rate within 1e-9).
Exit code 0 = everything identical, 1 = differences (listed).
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from orchestrator.config import load_config, official_files  # noqa: E402
from orchestrator.db_loader import ResultStore  # noqa: E402
from reference_oracle import compute  # noqa: E402


def fetch(cur, sql, params):
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def norm(rows):
    out = []
    for r in rows:
        r = dict(r)
        if "log_date" in r:
            r["log_date"] = str(r["log_date"])
        out.append(r)
    return out


def diff(name, expected, actual, tol=1e-9):
    problems = []
    if len(expected) != len(actual):
        problems.append("%s: row count expected %d, got %d" % (name, len(expected), len(actual)))
    for i, (e, a) in enumerate(zip(expected, actual)):
        for k, ev in e.items():
            av = a.get(k)
            same = (av is not None and abs(ev - av) <= tol) if isinstance(ev, float) else ev == av
            if not same:
                problems.append("%s row %d column %s: expected %r, got %r" % (name, i + 1, k, ev, av))
                break
        if len(problems) >= 10:
            break
    return problems


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run-id", type=int)
    inputs = ap.add_mutually_exclusive_group()
    inputs.add_argument("--files", nargs="+")
    inputs.add_argument("--data-dir", help="folder holding the two official archives")
    args = ap.parse_args()

    con = ResultStore(load_config()).connect()
    cur = con.cursor()
    if args.run_id is None:
        cur.execute("SELECT MAX(run_id) FROM pipeline_runs WHERE status='success'")
        args.run_id = cur.fetchone()[0]
    runs = fetch(cur, "SELECT * FROM pipeline_runs WHERE run_id=%s", (args.run_id,))
    if not runs or runs[0]["status"] != "success":
        con.close()
        sys.exit("No successful run found for run_id=%s." % args.run_id)
    run = runs[0]
    files = [Path(f) for f in args.files] if args.files else official_files(args.data_dir)
    print("Run %d (%s, batch size %d) vs reference oracle on %d file(s)..."
          % (run["run_id"], run["pipeline_name"], run["batch_size"], len(files)))
    ref = compute(files, run["batch_size"])

    problems = []
    for key in ("total_records", "malformed_records"):
        if ref[key] != run[key]:
            problems.append("%s: expected %s, got %s" % (key, ref[key], run[key]))
    total_batches = len(ref["batches"])
    expected_meta = {"total_batches": total_batches,
                     "avg_batch_size": ref["total_records"] / total_batches if total_batches else 0}
    problems += diff("run metadata", [expected_meta], [run])
    batches = fetch(cur, "SELECT batch_id, record_count, malformed_count FROM run_batches"
                         " WHERE run_id=%s ORDER BY batch_id", (args.run_id,))
    problems += diff("run_batches", ref["batches"], batches)
    q1 = norm(fetch(cur, "SELECT log_date, status_code, request_count, total_bytes FROM q1_daily_traffic"
                         " WHERE run_id=%s ORDER BY log_date, status_code", (args.run_id,)))
    q2 = norm(fetch(cur, "SELECT resource_path, request_count, total_bytes, distinct_host_count"
                         " FROM q2_top_resources WHERE run_id=%s ORDER BY rank_no", (args.run_id,)))
    q3 = norm(fetch(cur, "SELECT log_date, log_hour, error_request_count, total_request_count, error_rate,"
                         " distinct_error_hosts FROM q3_hourly_errors WHERE run_id=%s"
                         " ORDER BY log_date, log_hour", (args.run_id,)))
    problems += diff("Q1", ref["q1"], q1) + diff("Q2", ref["q2"], q2) + diff("Q3", ref["q3"], q3)
    for table, ref_name in (("q1_daily_traffic", "q1"), ("q2_top_resources", "q2"),
                            ("q3_hourly_errors", "q3")):
        metadata = fetch(cur, f"SELECT DISTINCT pipeline_name, batch_id FROM {table} WHERE run_id=%s",
                         (args.run_id,))
        expected = [{"pipeline_name": run["pipeline_name"], "batch_id": total_batches}]
        problems += diff(table + " metadata", expected if ref[ref_name] else [], metadata)
    con.close()

    print("records %d | malformed %d | batches %d | Q1 %d rows | Q2 %d rows | Q3 %d rows"
          % (ref["total_records"], ref["malformed_records"], len(ref["batches"]),
             len(ref["q1"]), len(ref["q2"]), len(ref["q3"])))
    if problems:
        print("FAIL - differences found:")
        for p in problems:
            print("  -", p)
        sys.exit(1)
    print("PASS - totals, per-batch counts, Q1, Q2 and Q3 are identical to the reference.")


if __name__ == "__main__":
    main()

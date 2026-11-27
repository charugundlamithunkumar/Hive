#!/usr/bin/env python3
"""Run NASA HTTP log ETL and three analytics queries using Hive.

    python main.py --pipeline hive --batch-size 50000
    python main.py                       (interactive pipeline menu)
"""
import argparse
import sys
from pathlib import Path

from orchestrator.config import load_config, official_files
from orchestrator.db_loader import ResultStore, now
from orchestrator.runner import PIPELINES, QUERY_NAMES, PipelineFactory
from orchestrator.timer import RunTimer


def choose_pipeline():
    print("Select exactly one execution pipeline:")
    for i, name in enumerate(PIPELINES, start=1):
        print("  %d) %s" % (i, name))
    while True:
        answer = input("Enter 1-%d: " % len(PIPELINES)).strip()
        if answer.isdigit() and 1 <= int(answer) <= len(PIPELINES):
            return PIPELINES[int(answer) - 1]


def close_runner(runner):
    try:
        runner.close()
    except Exception as exc:
        print("[WARNING] %s cleanup failed: %s" % (runner.name, exc), file=sys.stderr)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pipeline", choices=PIPELINES, help="execution backend (exactly one per run)")
    ap.add_argument("--batch-size", type=int, default=50000, help="records per batch (default 50000)")
    ap.add_argument("--files", nargs="+", metavar="FILE",
                    help="input files in logical order (default: official Jul95 then Aug95)")
    ap.add_argument("--data-dir", help="folder holding the official .gz files (default: project folder)")
    args = ap.parse_args(argv)

    if args.batch_size < 1:
        ap.error("--batch-size must be >= 1")
    pipeline = args.pipeline or choose_pipeline()
    cfg = load_config()
    files = [Path(f) for f in args.files] if args.files else official_files(args.data_dir)
    for f in files:
        if not f.is_file():
            sys.exit("[ERROR] input file not found: %s" % f)

    print("[INFO] Pipeline: %s | Batch size: %d" % (pipeline, args.batch_size))
    try:
        runner = PipelineFactory.create(pipeline, cfg, args.batch_size)
    except RuntimeError as exc:
        sys.exit("[ERROR] %s" % exc)

    store = ResultStore(cfg)
    try:
        store.ensure_schema()      # not timed
        runner.prepare()           # not timed (connect, reset working collections)
    except Exception as exc:
        close_runner(runner)
        sys.exit("[ERROR] %s / MySQL setup failed: %s" % (pipeline, exc))

    timer = RunTimer()
    started_at = now()
    timer.start()                  # runtime starts when input reading starts
    run_id = None
    try:
        with timer.stage("load") as s:
            total_records = runner.load_input(files)
            s["rows"] = total_records
            if total_records == 0:
                raise ValueError("No records found in the input files")
        with timer.stage("parse") as s:
            batches = runner.parse_and_batch()
            s["rows"] = len(batches)
            expected_batches = (total_records - 1) // args.batch_size + 1
            if len(batches) != expected_batches:
                raise ValueError("Expected %d batches, got %d" % (expected_batches, len(batches)))
            for batch_id, batch in enumerate(batches, start=1):
                expected_size = min(args.batch_size, total_records - (batch_id - 1) * args.batch_size)
                if (batch["batch_id"] != batch_id or batch["record_count"] != expected_size
                        or not 0 <= batch["malformed_count"] <= expected_size):
                    raise ValueError("Invalid statistics for batch %d: %s" % (batch_id, batch))
        results = {}
        for name in QUERY_NAMES:
            print("[INFO] Running %s ..." % name, flush=True)
            with timer.stage(name) as s:
                results[name] = runner.run_query(name)
                s["rows"] = len(results[name])

        malformed = sum(b["malformed_count"] for b in batches)
        meta = {
            "pipeline_name": pipeline, "batch_size": args.batch_size,
            "total_records": total_records, "malformed_records": malformed,
            "total_batches": len(batches),
            "avg_batch_size": total_records / len(batches), "started_at": started_at,
        }
        print("[INFO] Loading results to MySQL...", flush=True)
        with timer.stage("result_load") as s:
            run_id = store.write_results(meta, batches, results["q1_daily_traffic"],
                                         results["q2_top_resources"], results["q3_hourly_errors"])
            s["rows"] = sum(len(v) for v in results.values())
        runtime = timer.stop()     # runtime ends after the results are committed
        store.finalize_run(run_id, now(), runtime, timer.stages)
    except Exception as exc:
        elapsed = timer.stop() if timer.total is None else timer.total
        try:
            store.record_failure(pipeline, args.batch_size, started_at, elapsed, run_id=run_id)
        except Exception as failure:
            print("[WARNING] Failed to update MySQL run status for run_id=%s: %s"
                  % (run_id, failure), file=sys.stderr)
        sys.exit("[ERROR] run failed: %s" % exc)
    finally:
        close_runner(runner)

    print("[INFO] Done. run_id=%d, runtime=%.1fs, records=%d, malformed=%d, batches=%d, avg batch size=%.1f"
          % (run_id, runtime, total_records, malformed, len(batches), meta["avg_batch_size"]))
    print("[INFO] Show the report with:  python reporting/cli_report.py --run-id %d" % run_id)


if __name__ == "__main__":
    main()

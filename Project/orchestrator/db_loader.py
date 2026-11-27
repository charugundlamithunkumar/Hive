"""MySQL result store: schema creation and loading of results + metadata.

Loading is a single transaction (all result rows or none). The run-level
runtime is written right after that commit, because the runtime is defined as
"start of reading input -> final results successfully written".
"""
import datetime as dt
from pathlib import Path

import pymysql

SCHEMA_FILE = Path(__file__).resolve().parent.parent / "schema" / "create_schema.sql"


def now():
    return dt.datetime.now()


class ResultStore:
    def __init__(self, cfg):
        self.cfg = cfg

    def connect(self):
        return pymysql.connect(
            host=self.cfg.mysql_host, port=self.cfg.mysql_port,
            user=self.cfg.mysql_user, password=self.cfg.mysql_password,
            database=self.cfg.mysql_db, charset="utf8mb4", autocommit=False)

    def ensure_schema(self):
        statements = [s.strip() for s in SCHEMA_FILE.read_text(encoding="utf-8").split(";")]
        con = self.connect()
        try:
            with con.cursor() as cur:
                for stmt in statements:
                    body = "\n".join(l for l in stmt.splitlines() if not l.strip().startswith("--")).strip()
                    if body:
                        cur.execute(body)
            con.commit()
        finally:
            con.close()

    def write_results(self, meta, batches, q1, q2, q3):
        """One transaction: run row + batches + the three result tables.
        Returns run_id. meta keys: pipeline_name, batch_size, total_records,
        malformed_records, total_batches, avg_batch_size, started_at."""
        con = self.connect()
        try:
            with con.cursor() as cur:
                cur.execute(
                    "INSERT INTO pipeline_runs (pipeline_name, batch_size, total_records,"
                    " malformed_records, total_batches, avg_batch_size, started_at,"
                    " finished_at, runtime_seconds, status)"
                    " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,0,'running')",
                    (meta["pipeline_name"], meta["batch_size"], meta["total_records"],
                     meta["malformed_records"], meta["total_batches"], meta["avg_batch_size"],
                     meta["started_at"], meta["started_at"]))
                run_id = cur.lastrowid
                last_batch = max(b["batch_id"] for b in batches)
                executed_at = now()
                cur.executemany(
                    "INSERT INTO run_batches (run_id, batch_id, record_count, malformed_count)"
                    " VALUES (%s,%s,%s,%s)",
                    [(run_id, b["batch_id"], b["record_count"], b["malformed_count"]) for b in batches])
                common = (run_id, meta["pipeline_name"], last_batch, executed_at)
                cur.executemany(
                    "INSERT INTO q1_daily_traffic (run_id, pipeline_name, batch_id, executed_at,"
                    " log_date, status_code, request_count, total_bytes) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    [common + (r["log_date"], r["status_code"], r["request_count"], r["total_bytes"]) for r in q1])
                cur.executemany(
                    "INSERT INTO q2_top_resources (run_id, pipeline_name, batch_id, executed_at,"
                    " rank_no, resource_path, request_count, total_bytes, distinct_host_count)"
                    " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    [common + (i, r["resource_path"], r["request_count"], r["total_bytes"],
                               r["distinct_host_count"]) for i, r in enumerate(q2, start=1)])
                cur.executemany(
                    "INSERT INTO q3_hourly_errors (run_id, pipeline_name, batch_id, executed_at,"
                    " log_date, log_hour, error_request_count, total_request_count, error_rate,"
                    " distinct_error_hosts) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    [common + (r["log_date"], r["log_hour"], r["error_request_count"],
                               r["total_request_count"], r["error_rate"], r["distinct_error_hosts"])
                     for r in q3])
            con.commit()
            return run_id
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    def finalize_run(self, run_id, finished_at, runtime_seconds, stages):
        """stages: [(stage_name, seconds, output_rows)]."""
        con = self.connect()
        try:
            with con.cursor() as cur:
                cur.execute(
                    "UPDATE pipeline_runs SET finished_at=%s, runtime_seconds=%s, status='success'"
                    " WHERE run_id=%s", (finished_at, runtime_seconds, run_id))
                cur.executemany(
                    "INSERT INTO stage_metrics (run_id, stage_name, runtime_seconds, output_rows)"
                    " VALUES (%s,%s,%s,%s)", [(run_id, n, s, r) for n, s, r in stages])
            con.commit()
        finally:
            con.close()

    def record_failure(self, pipeline_name, batch_size, started_at, runtime_seconds, run_id=None):
        """A failed run keeps a metadata row but no result rows."""
        con = self.connect()
        try:
            with con.cursor() as cur:
                if run_id is None:
                    cur.execute(
                        "INSERT INTO pipeline_runs (pipeline_name, batch_size, total_records,"
                        " malformed_records, total_batches, avg_batch_size, started_at, finished_at,"
                        " runtime_seconds, status) VALUES (%s,%s,0,0,0,0,%s,%s,%s,'failed')",
                        (pipeline_name, batch_size, started_at, now(), runtime_seconds))
                else:
                    cur.execute(
                        "UPDATE pipeline_runs SET status='failed', finished_at=%s, runtime_seconds=%s,"
                        " total_records=0, malformed_records=0, total_batches=0, avg_batch_size=0"
                        " WHERE run_id=%s AND status='running'", (now(), runtime_seconds, run_id))
                    if cur.rowcount:
                        for table in ("q1_daily_traffic", "q2_top_resources", "q3_hourly_errors",
                                      "stage_metrics", "run_batches"):
                            cur.execute(f"DELETE FROM {table} WHERE run_id=%s", (run_id,))
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

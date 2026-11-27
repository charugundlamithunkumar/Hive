"""Run-scoped HiveServer2 session using a shared local filesystem for raw text."""
import gzip
import shutil
import tempfile
from importlib.resources import files as resources
from pathlib import Path
from urllib.parse import quote

from orchestrator.runner import PipelineRunner, QUERY_NAMES
from orchestrator.spec import LOG_RE, MONTHS, REQUEST_RE


def literal(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


class HiveRunner(PipelineRunner):
    name = "hive"
    connection = None
    staging = None

    def prepare(self):
        try:
            from pyhive import hive
        except ModuleNotFoundError as exc:
            raise RuntimeError("Install Hive dependencies with: python -m pip install -e ../Hive") from exc

        self.connection = hive.Connection(host=self.cfg.hive_host, port=self.cfg.hive_port,
                                          auth="NOSASL", database="default")
        self.execute("SELECT 1", rows=True)
        base = Path(self.cfg.hive_stage_dir).resolve()
        base.mkdir(parents=True, exist_ok=True)
        self.staging = tempfile.TemporaryDirectory(prefix="run-", dir=base)

    def execute(self, statement, rows=False):
        cursor = self.connection.cursor()
        try:
            cursor.execute(statement)
            if rows:
                names = [col[0].rsplit(".", 1)[-1].lower() for col in cursor.description]
                return [dict(zip(names, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def sql(self, name, **values):
        source = resources("nasa_hive").joinpath("sql").joinpath(name + ".sql")
        return source.read_text(encoding="utf-8").format(**values)

    def load_input(self, files):
        stage = Path(self.staging.name)
        for seq, source in enumerate(files):
            opener = gzip.open if source.suffix == ".gz" else open
            with opener(source, "rb") as src, (stage / f"{seq}.log").open("xb") as dest:
                shutil.copyfileobj(src, dest)
        if self.cfg.hive_server_stage_dir:
            location = "file://" + quote(self.cfg.hive_server_stage_dir.rstrip("/") + "/" + stage.name)
        else:
            location = stage.as_uri()
        self.execute(self.sql("source", location=literal(location)))
        self.execute(self.sql("load"))
        counts = self.execute("""
            SELECT count(*) AS total, count(file_seq) AS mapped,
                   count(DISTINCT file_seq, line_offset) AS positions
            FROM nasa_raw""", rows=True)[0]
        if not counts["total"]:
            raise ValueError("Hive read zero lines. Check input files and the shared staging mount.")
        if counts["total"] != counts["mapped"] or counts["total"] != counts["positions"]:
            raise ValueError("Hive did not expose a unique file and line offset for every raw record")
        return counts["total"]

    def parse_and_batch(self):
        # Java also treats Latin-1 NEL as a line ending by default. The shared regex uses LF.
        self.execute(self.sql("parse", log_pattern=literal("(?d)" + LOG_RE), request_pattern=literal(REQUEST_RE),
                              months=", ".join(map(literal, MONTHS)), batch_size=int(self.batch_size)))
        return self.execute("""
            SELECT batch_id, count(*) AS record_count,
                   sum(CASE WHEN valid THEN 0 ELSE 1 END) AS malformed_count
            FROM nasa_parsed GROUP BY batch_id ORDER BY batch_id""", rows=True)

    def run_query(self, query_name):
        if query_name not in QUERY_NAMES:
            raise ValueError(query_name)
        return self.execute(self.sql(query_name), rows=True)

    def close(self):
        connection, self.connection = self.connection, None
        staging, self.staging = self.staging, None
        try:
            if connection is not None:
                connection.close()
        finally:
            if staging is not None:
                staging.cleanup()

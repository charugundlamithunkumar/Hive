"""Strategy + Factory: the orchestrator only talks to this interface."""
from abc import ABC, abstractmethod

PIPELINES = ("hive",)
QUERY_NAMES = ("q1_daily_traffic", "q2_top_resources", "q3_hourly_errors")


class PipelineRunner(ABC):
    name = "abstract"

    def __init__(self, cfg, batch_size):
        self.cfg = cfg
        self.batch_size = batch_size

    @abstractmethod
    def prepare(self):
        """Connect / reset working state. NOT timed."""

    @abstractmethod
    def load_input(self, files):
        """Hand the unchanged raw lines to the backend (Jul95 then Aug95).
        Returns the number of lines read."""

    @abstractmethod
    def parse_and_batch(self):
        """Parse inside the backend and return per-batch statistics:
        [{'batch_id', 'record_count', 'malformed_count'}, ...]."""

    @abstractmethod
    def run_query(self, query_name):
        """Return the rows of one mandatory query as a list of dicts
        (exact output columns of the assignment)."""

    def close(self):
        """Release connections."""


class PipelineFactory:
    @staticmethod
    def create(name, cfg, batch_size):
        if name == "hive":
            try:
                from nasa_hive.runner import HiveRunner
            except ModuleNotFoundError as exc:
                if exc.name != "nasa_hive":
                    raise
                raise RuntimeError("Install the sibling Hive package from Project with: "
                                   "python -m pip install -e ../Hive") from exc
            return HiveRunner(cfg, batch_size)
        raise ValueError("unknown pipeline '%s'" % name)

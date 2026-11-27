"""Wall-clock timing: total run time plus per-stage times."""
import time
from contextlib import contextmanager


class RunTimer:
    def __init__(self):
        self.stages = []          # (stage_name, seconds, output_rows)
        self.total = None
        self._t0 = None

    def start(self):
        """Call when the tool starts reading the input."""
        self._t0 = time.perf_counter()

    @contextmanager
    def stage(self, name):
        info = {"rows": 0}
        t = time.perf_counter()
        try:
            yield info
        finally:
            self.stages.append((name, time.perf_counter() - t, int(info["rows"])))

    def stop(self):
        """Call right after the final result rows are committed."""
        self.total = time.perf_counter() - self._t0
        return self.total

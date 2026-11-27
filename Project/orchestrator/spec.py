"""Loads the shared parsing specification (spec/patterns.txt).

Every pipeline and the reference oracle read the SAME pattern text from this
one file, so they cannot drift apart.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PATTERN_FILE = ROOT / "spec" / "patterns.txt"

MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
MONTH_NUMBERS = ["%02d" % i for i in range(1, 13)]
HOURS = ["%02d" % i for i in range(24)]


def _load():
    patterns = {}
    for line in PATTERN_FILE.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        name, _, value = line.partition("=")
        patterns[name.strip()] = value
    return patterns


_P = _load()
LOG_RE = _P["LOG_RE"]
REQUEST_RE = _P["REQUEST_RE"]

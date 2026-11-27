#!/usr/bin/env python3
"""Independent single-pass REFERENCE implementation (a test oracle, NOT a pipeline).

It re-implements the specification in plain Python (no database, no Hadoop) so
that every pipeline's output can be checked against it:

    python tests/reference_oracle.py FILE [FILE ...] --batch-size 50000

Pattern text comes from spec/patterns.txt (the same file the pipelines use);
character classes are ASCII-only (re.ASCII) as the specification requires.
"""
import argparse
import gzip
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from orchestrator.spec import LOG_RE, MONTHS, REQUEST_RE  # noqa: E402

RECORD = re.compile(LOG_RE, re.ASCII)
REQUEST = re.compile(REQUEST_RE, re.ASCII)
MONTH_NO = {m: i for i, m in enumerate(MONTHS, start=1)}


def parse_line(line):
    """Return the 9 structured fields of a valid record, or None if malformed."""
    m = RECORD.match(line)
    if not m or m.group(3) not in MONTH_NO:
        return None
    tokens = [t or None for t in REQUEST.match(m.group(9)).groups()]
    raw_bytes = m.group(11) or ""
    return {
        "host": m.group(1),
        "timestamp": "%s-%02d-%s %s:%s:%s" % (m.group(4), MONTH_NO[m.group(3)], m.group(2),
                                              m.group(5), m.group(6), m.group(7)),
        "log_date": "%s-%02d-%s" % (m.group(4), MONTH_NO[m.group(3)], m.group(2)),
        "log_hour": int(m.group(5)),
        "http_method": tokens[0],
        "resource_path": tokens[1],
        "protocol_version": tokens[2],
        "status_code": int(m.group(10)),
        "bytes_transferred": int(raw_bytes) if (raw_bytes.isascii() and raw_bytes.isdigit()) else 0,
    }


def _lines(path):
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rb") as fh:
        for raw in fh:
            yield raw.decode("latin-1").rstrip("\r\n")


def compute(files, batch_size):
    n = 0
    batches = defaultdict(lambda: [0, 0])      # batch_id -> [records, malformed]
    q1 = defaultdict(lambda: [0, 0])           # (date, status) -> [count, bytes]
    paths = defaultdict(lambda: [0, 0, set()])  # path -> [count, bytes, hosts]
    hours = defaultdict(lambda: [0, 0, set()])  # (date, hour) -> [total, errors, error_hosts]

    for path in files:
        for line in _lines(Path(path)):
            n += 1
            b = batches[(n - 1) // batch_size + 1]
            b[0] += 1
            rec = parse_line(line)
            if rec is None:
                b[1] += 1
                continue
            size = rec["bytes_transferred"]
            g = q1[(rec["log_date"], rec["status_code"])]
            g[0] += 1
            g[1] += size
            if rec["resource_path"] is not None:
                p = paths[rec["resource_path"]]
                p[0] += 1
                p[1] += size
                p[2].add(rec["host"])
            h = hours[(rec["log_date"], rec["log_hour"])]
            h[0] += 1
            if 400 <= rec["status_code"] <= 599:
                h[1] += 1
                h[2].add(rec["host"])

    top = sorted(paths.items(), key=lambda kv: (-kv[1][0], kv[0]))[:20]
    return {
        "total_records": n,
        "malformed_records": sum(v[1] for v in batches.values()),
        "batches": [{"batch_id": k, "record_count": v[0], "malformed_count": v[1]}
                    for k, v in sorted(batches.items())],
        "q1": [{"log_date": d, "status_code": s, "request_count": v[0], "total_bytes": v[1]}
               for (d, s), v in sorted(q1.items())],
        "q2": [{"resource_path": p, "request_count": v[0], "total_bytes": v[1],
                "distinct_host_count": len(v[2])} for p, v in top],
        "q3": [{"log_date": d, "log_hour": hr, "error_request_count": v[1],
                "total_request_count": v[0], "error_rate": v[1] / v[0],
                "distinct_error_hosts": len(v[2])} for (d, hr), v in sorted(hours.items())],
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("files", nargs="+")
    ap.add_argument("--batch-size", type=int, default=50000)
    args = ap.parse_args()
    r = compute(args.files, args.batch_size)
    print("total records     :", r["total_records"])
    print("malformed records :", r["malformed_records"])
    print("batches / avg size:", len(r["batches"]), "/ %.1f" % (r["total_records"] / len(r["batches"])))
    print("Q1 rows / Q2 rows / Q3 rows:", len(r["q1"]), len(r["q2"]), len(r["q3"]))
    print("Q2 top 3:", [(x["resource_path"], x["request_count"]) for x in r["q2"][:3]])


if __name__ == "__main__":
    main()

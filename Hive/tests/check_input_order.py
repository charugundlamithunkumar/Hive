"""Live Hive check for original byte offsets, split boundaries and logical batches."""
import argparse
import gzip
import sys
import tempfile
from pathlib import Path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-dir", type=Path, default=Path.cwd(),
                        help="directory of the separate shared CLI (default: current directory)")
    args = parser.parse_args(argv)
    project = args.project_dir.resolve()
    if not (project / "orchestrator" / "runner.py").is_file():
        parser.error(f"shared CLI not found in {project}. Pass its directory with --project-dir.")
    sys.path.insert(0, str(project))
    from orchestrator.config import load_config
    from orchestrator.runner import PipelineFactory

    line = b'h - - [01/Jul/1995:00:00:00 -0400] "GET /caf\xe9\x85 HTTP/1.0" 200'
    # Include the default Hive null marker, NEL inside a path, and a final line without LF.
    contents = [[line + b"\r\n", b"\n", line + b"\n"],
                [b"bad\tline\n", b"\\N\n"] + [line + b"\n"] * 39 + [line]]
    expected = []
    with tempfile.TemporaryDirectory() as tmp:
        files = [Path(tmp, "z.log"), Path(tmp, "a.gz")]
        for seq, lines in enumerate(contents):
            opener = gzip.open if files[seq].suffix == ".gz" else open
            with opener(files[seq], "wb") as stream:
                stream.writelines(lines)
            offset = 0
            for raw in lines:
                expected.append((seq, offset, raw.rstrip(b"\r\n").decode("latin-1")))
                offset += len(raw)
        runner = PipelineFactory.create("hive", load_config(), 4)
        try:
            runner.prepare()
            runner.execute("SET mapreduce.input.fileinputformat.split.maxsize=256")
            runner.execute("SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat")
            assert runner.load_input(files) == len(expected)
            rows = runner.execute("SELECT file_seq, line_offset, raw_line FROM nasa_raw "
                                  "ORDER BY file_seq, line_offset", rows=True)
            assert [(r["file_seq"], r["line_offset"], r["raw_line"]) for r in rows] == expected
            batches = runner.parse_and_batch()
            expected_batches = [(1, 4, 2), (2, 4, 1)]
            expected_batches.extend((batch, 4, 0) for batch in range(3, 12))
            expected_batches.append((12, 1, 0))
            assert [(r["batch_id"], r["record_count"], r["malformed_count"]) for r in batches] == expected_batches
            result = runner.run_query("q2_top_resources")
            assert result == [dict(resource_path="/café\x85", request_count=42, total_bytes=0,
                                   distinct_host_count=1)], result
        finally:
            runner.close()
    print("PASS: file order, byte offsets, duplicates, blank line, Latin-1, missing bytes and batch boundary")


if __name__ == "__main__":
    main()

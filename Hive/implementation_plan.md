# Hive implementation plan for the NASA web log project

**Goal.** A user selects Hive in the team's existing CLI, supplies a batch size, and gets the same three MySQL reports as the MongoDB run. Hive performs the parsing, batch assignment, and aggregation. Python starts the work, collects the small result sets, and passes them to the existing MySQL loader.

**Current status.** The sibling `Hive/` package runs through the shared application in `../Project/`. The fixture and full NASA dataset passed the MySQL-backed reference comparison. The full runs at 10,000 and 50,000 records per batch produced the same query results. The tested runtime uses native Ubuntu WSL2 with Hadoop and Java 17. The repeated batch-size experiment and four-backend comparison remain open. [Verified runs and setup](README.md)

## 1. Bring up Hive and prove the input order

Start with the 106-line fixture. The first milestone is a Hive result showing each raw line's source file, position, ordinal, and batch ID. This resolves the most important implementation uncertainty before writing the analytics.

1. Use the validated Ubuntu WSL2 setup with Hive 4.1.0, Java 17, Hadoop 3.4.3, and an embedded Derby metastore. The startup script configures the local MapReduce execution engine and loopback HiveServer2 endpoint. Give Hive a separate writable staging directory and use MySQL for the shared report. [Local setup](README.md)
2. In the timed load stage, decompress each official archive byte-for-byte to its own run-scoped input location. Treat a plain fixture the same way without decompression. Give each input a numeric file sequence from the CLI argument order. The default order is July, then August. Keep blank, duplicate, irregular, and malformed lines. External preprocessing is limited to decompression.
3. Read uncompressed text through a Hive external table, one record per physical line. Configure the single raw-text column to preserve the complete line, including tabs and empty lines. Materialize the file sequence, original position, and raw text on the first scan. Check that every staged row has a unique source position.
4. Test whether Hive's `BLOCK__OFFSET__INSIDE__FILE` gives a unique, increasing line position within each uncompressed file on the chosen reader. Check against known line order, including duplicates, blank lines, and a multi-split test file. If offsets repeat or the ordering fails, use a Hive-compatible text input reader exposing Hadoop's line-start byte offset. Re-run the same check. A filename orders files but cannot order two lines inside one file. [Hive virtual columns](https://hive.apache.org/docs/latest/language/languagemanual-virtualcolumns/)
5. In HiveQL, assign `n = ROW_NUMBER() OVER (ORDER BY file_seq, line_offset)`, then `batch_id = 1 + ((n - 1) DIV B)` in the next query level. Assign every raw row its ordinal before filtering. For a two-file test with 3 and 2 lines and `B = 4`, expect IDs `1, 1, 1 | 1, 2`. Put malformed lines on either side of that batch boundary and verify their exact batch counts on two runs.

**Batching decision to confirm with the team and instructor.** The current shared design counts logical batches across files, runs Q1–Q3 globally, and stores the last batch ID on each result row. Continue fixture development with that convention. If physical batch execution is required, revise the shared contract before benchmarking. Global top-20 ranking and distinct-host counts still require a final aggregation across batches.

## 2. Build one reusable parsed table

Read the shared log and request patterns through the existing spec loader. Apply the log pattern in HiveQL to the staged raw line, then apply the request pattern to its quoted request. Materialize the resulting table once so each timed query reads the same parsed data. Preserve `n`, `batch_id`, and a malformed flag for every input row. [Shared patterns](../Project/spec/patterns.txt) · [Reference parser](../Project/tests/reference_oracle.py)

| Fields | Hive behavior to verify |
|---|---|
| `host`, `status_code` | Preserve host case. Require a three-digit status token and convert it to an integer. |
| `timestamp`, `log_date`, `log_hour` | Validate the shared day and time ranges and exact English month abbreviations. Use the local time printed in the record. |
| `http_method`, `resource_path`, `protocol_version` | Extract the first three whitespace-delimited request tokens. Map missing tokens to SQL NULL. An incomplete request inside an otherwise valid line stays valid. |
| `bytes_transferred` | Use BIGINT for bytes and totals. A dash or nonnumeric token gives zero. The assignment also specifies zero when the bytes token is absent. |

**Missing bytes.** The shared regex accepts a missing bytes token. The reference and MongoDB handle its empty capture, and the focused parser regression test covers lines with and without trailing whitespace. The original 106-line fixture remains the comparison baseline.

Compute batch statistics from all staged rows with `COUNT(*)` and a conditional sum of malformed rows, grouped and ordered by `batch_id`. Use only valid rows for the following queries.

## 3. Implement the three HiveQL queries

| Query | Grouping and calculations | Output order |
|---|---|---|
| Q1 daily traffic | Group by `log_date, status_code`. Return `request_count = COUNT(*)` and `total_bytes = SUM(bytes_transferred)`. | Date, then status ascending. |
| Q2 top resources | Exclude NULL paths. Group by `resource_path`. Return `request_count`, `total_bytes`, and `distinct_host_count = COUNT(DISTINCT host)`. Calculate over both files. | Request count descending, then path ascending. Apply `LIMIT 20` after sorting. |
| Q3 hourly errors | Group by `log_date, log_hour`. For status 400–599 inclusive, sum an error flag as `error_request_count`. Return `total_request_count = COUNT(*)` and `distinct_error_hosts = COUNT(DISTINCT CASE WHEN is_error THEN host END)`. Divide errors by all requests using DOUBLE for `error_rate`. | Date, then hour ascending. Keep hours with requests and zero errors. |

Return the group keys and named metrics exactly as shown. Keep Q2 rows in ranked order when passing them to the loader, which assigns `rank_no` from their sequence. Verify the case-sensitive path tie-break at the fixture's rank-20 cutoff.

## 4. Connect Hive to the team's CLI

Keep the Hive runner, HiveQL files, and setup instructions in this sibling folder. Package the runner as an importable Python module with its SQL resources. Install it into the shared Python environment for integration. The bundled `Project/` directory includes the Hive registration, connection settings, and CLI. Reuse the existing timer, result loader, and report. [Runner interface](../Project/orchestrator/runner.py) · [CLI](../Project/main.py) · [MySQL loader](../Project/orchestrator/db_loader.py)

| Runner method | Responsibility |
|---|---|
| `prepare()` | Connect to HiveServer2, check the service, and allocate a unique staging namespace. Keep setup outside the timer. |
| `load_input(files)` | Stage the ordered inputs, execute the raw scan, and return the input line count. Start input reads inside this timed stage. |
| `parse_and_batch()` | Build the numbered, parsed table and return ordered dictionaries containing `batch_id`, `record_count`, and `malformed_count`. |
| `run_query(query_name)` | Execute the corresponding HiveQL and return typed dictionaries with the exact result columns. Surface Hive errors to the orchestrator. |
| `close()` | Release the connection and clean up only the current run's staging resources. Retrying a failed run must start from fresh staging. |

Keep the existing timing boundary from first input read through MySQL result commit. Record load, parse, Q1, Q2, Q3, and result-load durations. Check that the report shows the Hive pipeline name, run ID, stage times, batch size, batch count, and average size.

## 5. Verify correctness through the normal entry point

Run these Bash commands from `Project/` after setting up Hive and MySQL. Enter the run ID printed by each execution so the comparison reads that specific Hive run. [Comparison script](../Project/scripts/compare_results.py)

```bash
python main.py --pipeline hive --batch-size 10 --files tests/fixtures/edge_cases.log
read -r -p "Hive fixture run ID: " fixture_run
python scripts/compare_results.py --run-id "$fixture_run" --files tests/fixtures/edge_cases.log

python main.py --pipeline hive --batch-size 50000
read -r -p "Hive full-data run ID: " full_run
python scripts/compare_results.py --run-id "$full_run"
python reporting/cli_report.py --run-id "$full_run" --batches
```

| Check | Expected result |
|---|---|
| Existing fixture, B = 10 | 106 records, 10 malformed, 11 batches, final batch of 6. Q1/Q2/Q3 return 21/20/15 rows. |
| July and August, B = 50,000 | 3,461,613 records, 1 malformed, 70 batches, final batch of 11,613, average 49,451.6. |
| Q1 on full data | 295 rows, 3,461,612 requests and 65,524,314,915 bytes in total. |
| Q2 on full data | 20 rows. First is `/images/NASA-logosmall.gif` with 208,798 requests. Twentieth is `/icons/blank.xbm` with 28,852. |
| Q3 on full data | 1,369 rows and 21,247 errors in total. |

Require a passing row-by-row comparison for both runs, including every batch's malformed count. Integers must match exactly and error rates must agree within `1e-9`. Separately inspect MySQL metadata, including the final batch ID on query results and all six stage timings. On a mismatch, isolate the earliest difference in raw row order, parsed fields, grouping, or result conversion.

## 6. Produce the Hive contribution for Phase 2

- Run the same batch-size experiment as the other backends. Proposed sizes are 10,000, 50,000, 100,000, and 3,461,613. Use the same files, order, machine, resource allocation, and timing boundaries. Record run IDs, stage times, overall runtime, and correctness for each repetition. Report the median and range across at least three timed runs per size as a proposed team convention.
- Deliver the Hive runner and SQL, pinned setup instructions, integration changes, and the fixture and full-data comparison outputs. Give the team a short report section explaining how source order was preserved, how malformed lines were counted, and which Hive stages dominated measured runtime.
- For the live demo, select Hive in the shared CLI, show a successful execution, then open the MySQL-backed report for its run ID. Keep the full-data comparison and timings ready for the four-backend discussion.

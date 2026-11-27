# NASA HTTP Log Analytics with Apache Hive

This backend processes the July and August 1995 NASA web server logs and writes three analytics reports to MySQL. HiveQL handles parsing, batching, and aggregation. The Python CLI stages input files and writes results and run metadata to the reporting database.

## Verified results

| Input | Batch size | Records | Malformed | Batches | Final batch | Runtime |
|---|---:|---:|---:|---:|---:|---:|
| [Edge-case fixture](Hive/results/fixture-report.txt) | 10 | 106 | 10 | 11 | 6 | 16.0 s |
| [July + August](Hive/results/full-data-report.txt) | 50,000 | 3,461,613 | 1 | 70 | 11,613 | 223.1 s |
| [July + August](Hive/results/full-data-10000-report.txt) | 10,000 | 3,461,613 | 1 | 347 | 1,613 | 219.5 s |

Both full runs matched the independent Python reference on every batch and all **1,684 query rows**. They produced the same result hash (`4894698529`), **65,524,314,915 transferred bytes**, and **21,247 error requests**. The durations are individual end-to-end measurements on Ubuntu WSL2 with Hive 4.1.0, Hadoop 3.4.3, JDK 17, and MySQL 8.0.46. [Reference comparisons](Hive/results/reference-verification.txt)

## What the pipeline does

| Report | Output |
|---|---|
| Q1 Daily traffic | Request count and transferred bytes by date and status code |
| Q2 Top resources | The 20 most-requested paths, with request count, transferred bytes, and distinct hosts |
| Q3 Hourly errors | HTTP 400–599 requests, total requests, error rate, and distinct error hosts by date and hour |

1. Stage each source file byte-for-byte, decompressing gzip archives during the timed load.
2. Read raw lines into Hive and order them by source file and original byte offset. The official input order is July, then August.
3. Give each line a logical batch ID using `1 + ((row_number - 1) DIV batch_size)`. Malformed lines count toward the batch size. Parse the Common Log Format and treat missing or nonnumeric byte counts as zero.
4. Run the three global queries on valid rows and commit the results to MySQL. Each result row carries the run ID, pipeline, execution time, and final batch ID. The timer runs from the first input read through the MySQL result commit.

The project uses logical batches with global aggregation. Instructor confirmation of that interpretation remains open in the project plan. [Implementation plan](Hive/implementation_plan.md)

## Run locally

Use Ubuntu or WSL2 with **Python 3.10+**, **JDK 17**, **Hadoop 3.4.3**, **Hive 4.1.0**, and a **MySQL 8** instance reachable from Ubuntu. This repository includes the Hive backend in `Hive/` and the shared CLI, MySQL schema, and Python reference in `Project/`. Clone it with `git clone https://github.com/charugundlamithunkumar/Hive.git` and enter the clone.

### 1. Start HiveServer2

Download and verify Hive in a Linux tools directory with enough free space. [Apache Hive 4.1.0 archive](https://archive.apache.org/dist/hive/hive-4.1.0/)

```bash
base=https://archive.apache.org/dist/hive/hive-4.1.0
archive=apache-hive-4.1.0-bin.tar.gz
curl -fLO "$base/$archive"
curl -fLO "$base/$archive.sha256"
sha256sum -c "$archive.sha256" && tar -xzf "$archive"
```

From the directory that contains `Hive/` and `Project/`, point these variables at your installations and start Hive in a dedicated terminal.

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HIVE_HOME=/absolute/path/to/apache-hive-4.1.0-bin
bash Hive/start_local.sh
```

The script starts HiveServer2 on `127.0.0.1:10000` with Derby and Hadoop's local job runner. It stores runtime state under `~/.local/state/nasa-hive`. Set `HIVE_STATE_DIR` to choose a different directory. Set `HIVE_PORT` on both the server and client to change the listening port.

### 2. Prepare MySQL and install the CLI dependencies

Create an empty `nasa_etl` database in MySQL and give a user access to it. The CLI creates the reporting tables on the first run. In a second terminal, start from the directory containing both project folders.

```bash
cd Project
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt -e ../Hive
cp -n .env.example .env
```

Set `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DB`, `MYSQL_USER`, and `MYSQL_PASSWORD` in `Project/.env`. The CLI defaults to Hive on `127.0.0.1:10000`. The Hive server and CLI need access to the same local staging directory. For a shared mount, set `HIVE_STAGE_DIR` on the client and `HIVE_SERVER_STAGE_DIR` to its corresponding absolute server path.

### 3. Run and verify the fixture

Run from `Project/`. Use the printed run ID for both reporting and comparison.

```bash
python main.py --pipeline hive --batch-size 10 --files tests/fixtures/edge_cases.log
read -r -p "Fixture run ID: " run_id
python scripts/compare_results.py --run-id "$run_id" --files tests/fixtures/edge_cases.log
python reporting/cli_report.py --run-id "$run_id" --batches --all
```

The fixture produces **106 records**, **10 malformed lines**, and **11 batches**, with **21 / 20 / 15** result rows for Q1 / Q2 / Q3.

### 4. Run both NASA logs

Download the two official archives into `Project/` and keep their original filenames. [NASA HTTP dataset](https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html) · [July archive](https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz) · [August archive](https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz)

```bash
python main.py --pipeline hive --batch-size 50000
read -r -p "Full dataset run ID: " run_id
python scripts/compare_results.py --run-id "$run_id"
python reporting/cli_report.py --run-id "$run_id" --batches
```

Add `--data-dir /path/to/archives` to both the pipeline and comparison commands when the archives live elsewhere. Run `python ../Hive/tests/check_input_order.py` from `Project/` for the live split-file regression check. It covers duplicate lines, blank lines, Latin-1, mixed line endings, missing bytes, and batches that cross file boundaries.

## Source and outputs

- [Hive runner](Hive/nasa_hive/runner.py) opens HiveServer2 sessions and stages inputs.
- [HiveQL](Hive/nasa_hive/sql/) defines source and parsed tables and the three reports.
- [Input-order regression check](Hive/tests/check_input_order.py) exercises the reader and logical batch assignment.
- [Verified reports](Hive/results/) contain MySQL-backed output and comparisons against the Python reference.

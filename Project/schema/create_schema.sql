-- MySQL 8 reporting schema (implementation_plan.md section 8).
-- Every result row carries: pipeline name, run id, batch id, execution time.
-- batch_id on result rows = K, the last batch id of the run (queries are global).

CREATE TABLE IF NOT EXISTS pipeline_runs (
  run_id            BIGINT AUTO_INCREMENT PRIMARY KEY,
  pipeline_name     VARCHAR(16)  NOT NULL,          -- mongodb | mapreduce | pig | hive
  batch_size        INT          NOT NULL,
  total_records     BIGINT       NOT NULL,          -- includes malformed lines
  malformed_records BIGINT       NOT NULL,
  total_batches     INT          NOT NULL,          -- non-empty batches
  avg_batch_size    DOUBLE       NOT NULL,          -- total_records / total_batches
  started_at        DATETIME(3)  NOT NULL,
  finished_at       DATETIME(3)  NOT NULL,
  runtime_seconds   DOUBLE       NOT NULL,
  status            VARCHAR(10)  NOT NULL           -- running | success | failed
);

CREATE TABLE IF NOT EXISTS run_batches (
  run_id          BIGINT NOT NULL,
  batch_id        INT    NOT NULL,
  record_count    INT    NOT NULL,
  malformed_count INT    NOT NULL,
  PRIMARY KEY (run_id, batch_id),
  FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
);

CREATE TABLE IF NOT EXISTS stage_metrics (
  run_id          BIGINT      NOT NULL,
  stage_name      VARCHAR(24) NOT NULL,   -- load|parse|q1_daily_traffic|q2_top_resources|q3_hourly_errors|result_load
  runtime_seconds DOUBLE      NOT NULL,
  output_rows     INT         NOT NULL,
  PRIMARY KEY (run_id, stage_name),
  FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
);

CREATE TABLE IF NOT EXISTS q1_daily_traffic (
  run_id        BIGINT      NOT NULL,
  pipeline_name VARCHAR(16) NOT NULL,
  batch_id      INT         NOT NULL,
  executed_at   DATETIME(3) NOT NULL,
  log_date      DATE        NOT NULL,
  status_code   SMALLINT    NOT NULL,
  request_count BIGINT      NOT NULL,
  total_bytes   BIGINT      NOT NULL,
  PRIMARY KEY (run_id, log_date, status_code),
  FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id),
  FOREIGN KEY (run_id, batch_id) REFERENCES run_batches(run_id, batch_id)
);

CREATE TABLE IF NOT EXISTS q2_top_resources (
  run_id              BIGINT      NOT NULL,
  pipeline_name       VARCHAR(16) NOT NULL,
  batch_id            INT         NOT NULL,
  executed_at         DATETIME(3) NOT NULL,
  rank_no             TINYINT     NOT NULL,
  resource_path       VARCHAR(2048) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  request_count       BIGINT      NOT NULL,
  total_bytes         BIGINT      NOT NULL,
  distinct_host_count BIGINT      NOT NULL,
  PRIMARY KEY (run_id, rank_no),
  FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id),
  FOREIGN KEY (run_id, batch_id) REFERENCES run_batches(run_id, batch_id)
);

CREATE TABLE IF NOT EXISTS q3_hourly_errors (
  run_id               BIGINT      NOT NULL,
  pipeline_name        VARCHAR(16) NOT NULL,
  batch_id             INT         NOT NULL,
  executed_at          DATETIME(3) NOT NULL,
  log_date             DATE        NOT NULL,
  log_hour             TINYINT     NOT NULL,
  error_request_count  BIGINT      NOT NULL,
  total_request_count  BIGINT      NOT NULL,
  error_rate           DOUBLE      NOT NULL,
  distinct_error_hosts BIGINT      NOT NULL,
  PRIMARY KEY (run_id, log_date, log_hour),
  FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id),
  FOREIGN KEY (run_id, batch_id) REFERENCES run_batches(run_id, batch_id)
);

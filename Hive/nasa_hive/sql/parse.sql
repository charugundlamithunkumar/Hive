CREATE TEMPORARY TABLE nasa_parsed STORED AS ORC AS
SELECT n, 1 + ((n - 1) DIV {batch_size}) AS batch_id,
       host <> '' AND month_no > 0 AS valid, host,
       concat(log_date, ' ', hh, ':', mi, ':', ss) AS `timestamp`,
       log_date, CAST(hh AS INT) AS log_hour,
       nullif(regexp_extract(request_text, {request_pattern}, 1), '') AS http_method,
       nullif(regexp_extract(request_text, {request_pattern}, 2), '') AS resource_path,
       nullif(regexp_extract(request_text, {request_pattern}, 3), '') AS protocol_version,
       CAST(status_text AS INT) AS status_code,
       CASE WHEN bytes_text RLIKE '^[0-9]+$' THEN CAST(bytes_text AS BIGINT)
            ELSE CAST(0 AS BIGINT) END AS bytes_transferred
FROM (
    SELECT parts.*, concat(yyyy, '-', lpad(CAST(month_no AS STRING), 2, '0'), '-', dd) AS log_date
    FROM (
        SELECT row_number() OVER (ORDER BY file_seq, line_offset) AS n,
               regexp_extract(raw_line, {log_pattern}, 1) AS host,
               regexp_extract(raw_line, {log_pattern}, 2) AS dd,
               field(regexp_extract(raw_line, {log_pattern}, 3), {months}) AS month_no,
               regexp_extract(raw_line, {log_pattern}, 4) AS yyyy,
               regexp_extract(raw_line, {log_pattern}, 5) AS hh,
               regexp_extract(raw_line, {log_pattern}, 6) AS mi,
               regexp_extract(raw_line, {log_pattern}, 7) AS ss,
               regexp_extract(raw_line, {log_pattern}, 9) AS request_text,
               regexp_extract(raw_line, {log_pattern}, 10) AS status_text,
               regexp_extract(raw_line, {log_pattern}, 11) AS bytes_text
        FROM nasa_raw
    ) parts
) dated

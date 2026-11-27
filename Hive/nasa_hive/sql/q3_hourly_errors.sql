SELECT log_date, log_hour, error_request_count, total_request_count,
       CAST(error_request_count AS DOUBLE) / total_request_count AS error_rate, distinct_error_hosts
FROM (
    SELECT log_date, log_hour,
           sum(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) AS error_request_count,
           count(*) AS total_request_count,
           count(DISTINCT CASE WHEN status_code BETWEEN 400 AND 599 THEN host END) AS distinct_error_hosts
    FROM nasa_parsed WHERE valid GROUP BY log_date, log_hour
) hourly
ORDER BY log_date, log_hour

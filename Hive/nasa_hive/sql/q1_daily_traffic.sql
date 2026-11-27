SELECT log_date, status_code, count(*) AS request_count, sum(bytes_transferred) AS total_bytes
FROM nasa_parsed WHERE valid
GROUP BY log_date, status_code ORDER BY log_date, status_code

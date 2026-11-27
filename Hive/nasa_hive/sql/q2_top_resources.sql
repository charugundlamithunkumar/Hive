SELECT resource_path, count(*) AS request_count, sum(bytes_transferred) AS total_bytes,
       count(DISTINCT host) AS distinct_host_count
FROM nasa_parsed WHERE valid AND resource_path IS NOT NULL
GROUP BY resource_path ORDER BY request_count DESC, resource_path ASC LIMIT 20

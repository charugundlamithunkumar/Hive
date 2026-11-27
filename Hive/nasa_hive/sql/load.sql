CREATE TEMPORARY TABLE nasa_raw STORED AS ORC AS
SELECT CAST(regexp_extract(INPUT__FILE__NAME, '/([0-9]+)[.]log$', 1) AS INT) AS file_seq,
       BLOCK__OFFSET__INSIDE__FILE AS line_offset, raw_line
FROM nasa_source

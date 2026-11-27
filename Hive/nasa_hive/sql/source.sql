CREATE TEMPORARY EXTERNAL TABLE nasa_source (raw_line STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    'serialization.last.column.takes.rest' = 'true',
    'serialization.encoding' = 'ISO-8859-1',
    'serialization.null.format' = '\n'
)
STORED AS TEXTFILE LOCATION {location}

CREATE DATABASE IF NOT EXISTS mailamericas_reference;

CREATE EXTERNAL TABLE IF NOT EXISTS mailamericas_reference.exchange_rates (
    year INT,
    month INT,
    exchange_rate_ars_usd DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('serialization.format' = ',', 'field.delim' = ',')
STORED AS TEXTFILE
LOCATION 's3://mailamericas-datalake/reference/exchange_rates/'
TBLPROPERTIES ('skip.header.line.count'='1');


SELECT *
FROM mailamericas_reference.exchange_rates
LIMIT 10;
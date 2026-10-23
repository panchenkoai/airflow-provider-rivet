-- One ClickHouse database per pipeline: two sources with the same table names never share one.
CREATE DATABASE IF NOT EXISTS mysql_cdc;
CREATE DATABASE IF NOT EXISTS mysql_batch;
CREATE DATABASE IF NOT EXISTS postgres_cdc;
CREATE DATABASE IF NOT EXISTS postgres_batch;

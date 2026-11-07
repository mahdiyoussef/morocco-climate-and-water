#!/bin/bash
set -euo pipefail
psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -v ON_ERROR_STOP=1 \
  -v pipeline_password="$PIPELINE_WRITER_PASSWORD" -v reader_password="$BI_READER_PASSWORD" <<'SQL'
CREATE DATABASE metastore;
CREATE DATABASE airflow;
CREATE DATABASE superset;
CREATE DATABASE observatory;
CREATE ROLE pipeline_writer LOGIN PASSWORD :'pipeline_password';
CREATE ROLE bi_reader LOGIN PASSWORD :'reader_password';
\connect observatory
CREATE SCHEMA gold AUTHORIZATION pipeline_writer;
GRANT CONNECT ON DATABASE observatory TO bi_reader;
GRANT USAGE ON SCHEMA gold TO bi_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE pipeline_writer IN SCHEMA gold GRANT SELECT ON TABLES TO bi_reader;
SQL

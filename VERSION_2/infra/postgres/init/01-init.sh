#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOF
    CREATE USER airflow WITH PASSWORD '${AIRFLOW_PASSWORD}';
    CREATE DATABASE airflow OWNER airflow;
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
EOF

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOF
    CREATE USER marquez WITH PASSWORD '${PG_PASSWORD}';
    CREATE DATABASE marquez OWNER marquez;
    GRANT ALL PRIVILEGES ON DATABASE marquez TO marquez;
EOF

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOF
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS stage;
    CREATE SCHEMA IF NOT EXISTS mart;
EOF
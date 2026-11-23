#!/bin/bash
set -e

sed -i '1i\
# Allow connections from Docker network for airflow and marquez users\
host    airflow         airflow         172.16.0.0/12            scram-sha-256\
host    marquez         marquez         172.16.0.0/12            scram-sha-256\
host    all             all             172.16.0.0/12            scram-sha-256\
' /var/lib/postgresql/data/pg_hba.conf

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "SELECT pg_reload_conf();"

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

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "marquez" <<-EOF
    GRANT ALL ON SCHEMA public TO marquez;
EOF

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOF
    CREATE TABLE IF NOT EXISTS discover_runs (
        run_id SERIAL PRIMARY KEY,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ,
        duration_sec REAL,
        count INTEGER,
        days_ago INTEGER,
        status TEXT CHECK (status IN ('running', 'success', 'failed')),
        matches_discovered INTEGER,
        players_fetched INTEGER,
        error_message TEXT
    );

    CREATE TABLE IF NOT EXISTS enrich_runs (
        run_id SERIAL PRIMARY KEY,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ,
        duration_sec REAL,
        limit_requested INTEGER,
        status TEXT CHECK (status IN ('running', 'success', 'failed')),
        matches_enriched INTEGER,
        enriched_failed INTEGER,
        error_message TEXT
    );

    CREATE TABLE IF NOT EXISTS cleanup_runs (
        run_id SERIAL PRIMARY KEY,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ,
        duration_sec REAL,
        status TEXT CHECK (status IN ('running', 'success', 'failed')),
        cutoff_dt TIMESTAMPTZ,
        matches_deleted INTEGER,
        players_deleted INTEGER,
        mongodb_docs_deleted INTEGER,
        mongodb_compacted BOOLEAN,
        error_message TEXT
    );
EOF

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOF
    CREATE DATABASE superset OWNER ${POSTGRES_USER};
    GRANT ALL PRIVILEGES ON DATABASE superset TO ${POSTGRES_USER};
EOF

echo "Database initialization completed successfully, including retention log tables."
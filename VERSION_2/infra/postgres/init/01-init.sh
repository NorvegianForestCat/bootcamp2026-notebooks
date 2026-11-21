#!/bin/bash
set -e

cat >> /var/lib/postgresql/data/pg_hba.conf <<-EOF

# Allow connections from Docker network for airflow and marquez users
host    airflow         airflow         172.16.0.0/12            md5
host    marquez         marquez         172.16.0.0/12            md5
host    all             all             172.16.0.0/12            md5
EOF

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

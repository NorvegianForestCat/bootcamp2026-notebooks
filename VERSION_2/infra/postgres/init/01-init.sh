#!/bin/bash
set -e

# Вставляем правила в НАЧАЛО pg_hba.conf (перед дефолтными правилами), чтобы они имели приоритет
sed -i '1i\
# Allow connections from Docker network for airflow and marquez users\
host    airflow         airflow         172.16.0.0/12            scram-sha-256\
host    marquez         marquez         172.16.0.0/12            scram-sha-256\
host    all             all             172.16.0.0/12            scram-sha-256\
' /var/lib/postgresql/data/pg_hba.conf

# Перезагружаем конфигурацию Postgres
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "SELECT pg_reload_conf();"

# Создание пользователя и базы для Airflow
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOF
    CREATE USER airflow WITH PASSWORD '${AIRFLOW_PASSWORD}';
    CREATE DATABASE airflow OWNER airflow;
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
EOF

# Создание пользователя и базы для Marquez
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOF
    CREATE USER marquez WITH PASSWORD '${PG_PASSWORD}';
    CREATE DATABASE marquez OWNER marquez;
    GRANT ALL PRIVILEGES ON DATABASE marquez TO marquez;
EOF

# Создание схем для основного приложения
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOF
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS stage;
    CREATE SCHEMA IF NOT EXISTS mart;
EOF

# Предоставление прав на схему public для marquez
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "marquez" <<-EOF
    GRANT ALL ON SCHEMA public TO marquez;
EOF
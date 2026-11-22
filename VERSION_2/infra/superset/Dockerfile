FROM apache/superset:6.1.0

USER root
RUN /app/.venv/bin/pip install --no-cache-dir psycopg2-binary trino

USER superset

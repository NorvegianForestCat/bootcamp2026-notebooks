#!/bin/sh
set -e

mkdir -p /app/config

cat <<EOF > /app/config/default.toml
[db]
postgres_url = "${POSTGRES_URL}"
mongo_url = "mongodb://${MONGO_USER}:${MONGO_PASSWORD}@${MONGO_HOST}:${MONGO_PORT}/${MONGO_DB}?authSource=admin"

[generation]
matches_count = ${MATCHES:-100}
pg_batch_size = 500
mongo_batch_size = 500
seed = ${SEED:-42}
EOF

exec /usr/local/bin/dota-data-generator "$@"

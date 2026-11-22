#!/bin/sh
set -e

mkdir -p /app/config

cat <<EOF > /app/config/default.toml
[db]
postgres_url = "${POSTGRES_URL}"
mongo_url = "${MONGO_URL}"

[generation]
matches_count = ${MATCHES:-100}
pg_batch_size = 500
mongo_batch_size = 500
seed = ${SEED:-42}
EOF

exec /usr/local/bin/dota-data-generator "$@"

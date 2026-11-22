#!/bin/bash
clear

time=60

run_and_log() {
    echo "*$(date)*>> $*" | tee -a res
    "$@" 2>&1 | tee -a res
    echo "" | tee -a res
}

# --------------------------------------------------------------------------------

> res

run_and_log docker compose --profile '*' down -v --remove-orphans
run_and_log docker volume rm dota-lakehouse_pg_data

run_and_log cat infra/postgres/init/01-init.sh
run_and_log docker compose --profile base --profile etl --profile compute --profile bi --profile lineage up -d --build

run_and_log sleep $time

run_and_log docker compose logs postgres --tail=50
run_and_log docker logs dota-marquez --tail=50
run_and_log docker compose ps

#!/bin/bash
clear

echo "WARNING: This script will completely remove ALL data, volumes, images, and Docker cache!"
echo "This will affect not only this project, but also other stopped projects on your machine."
read -p "Are you sure you want to continue? (y/N): " confirm
if [[ "$confirm" != [Yy]* ]]; then
    echo "Operation cancelled."
    exit 1
fi

time=60

run_and_log() {
    echo "*$(date)*>> $*" | tee -a res
    "$@" 2>&1 | tee -a res
    echo "" | tee -a res
}

# --------------------------------------------------------------------------------

> res

echo "Starting complete Docker cleanup..."

# 1. Stop and remove everything related to this project (containers, networks, volumes)
run_and_log docker compose --profile '*' down -v --remove-orphans

# 2. Nuclear cleanup of all Docker: unused images, builder cache, volumes, networks
run_and_log docker system prune -a --volumes -f

echo "Cleanup completed. Starting download and launch..."

# 3. Check initialization scripts (for logs)
run_and_log cat infra/postgres/init/01-init.sh

# 4. Download the latest images from registry
run_and_log docker compose --profile base --profile etl --profile compute --profile bi --profile lineage pull --ignore-buildable

# 5. Launch everything with rebuild (in case of local config changes)
run_and_log docker compose --profile base --profile etl --profile compute --profile bi --profile lineage up -d --build

# 6. Wait for initialization (especially DB and Airflow)
run_and_log sleep $time

# 7. Check status of key services
run_and_log docker compose logs postgres --tail=50
run_and_log docker logs dota-marquez --tail=50
run_and_log docker compose ps

echo "Done! Infrastructure launched from a completely clean slate."

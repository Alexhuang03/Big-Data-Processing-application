#!/usr/bin/env bash
# Runs the streaming pipeline: Wikipedia producer (background) + Spark job.
#
# Prerequisites: containers running (docker compose up -d --build) and the
# silver layer built (bash scripts/run_preparation.sh).
#
# Usage (from the project root): bash scripts/run_streaming.sh [SECONDS]
#   SECONDS  test window length, default 1200 (20 minutes)
set -euo pipefail
export MSYS_NO_PATHCONV=1

DURATION=${1:-1200}

echo "=== Starting Wikipedia producer in the background (${DURATION}s + margin) ==="
docker exec -d imdb_spark python -m streaming.producer --duration "$((DURATION + 120))"

echo "=== Starting Spark Structured Streaming job (${DURATION}s) ==="
docker exec imdb_spark python -m streaming.streaming_job --duration "$DURATION"

echo "Done. Summary tables and metrics are in output/streaming/"

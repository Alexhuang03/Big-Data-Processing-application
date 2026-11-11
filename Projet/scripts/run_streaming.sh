#!/usr/bin/env bash
# Runs the Kafka streaming pipeline inside the Spark container.
#
# Prerequisites:
#   - Docker containers are running (docker compose up -d)
#   - Silver layer has been built (bash scripts/run_preparation.sh)
#   - The wikipedia-edits Kafka topic will be auto-created by the producer.
#
# Usage (from the project root): bash scripts/run_streaming.sh [--duration-minutes N]
#
# The producer runs in the background (docker exec -d). The streaming job runs
# in the foreground; press Ctrl-C to stop both after your test window (15-30 min).
set -euo pipefail
export MSYS_NO_PATHCONV=1

DURATION=${1:-1800}  # default: 30 minutes

echo "=== Starting Wikipedia SSE producer (background) ==="
docker exec -d imdb_spark python -m streaming.producer

echo "=== Starting Spark Structured Streaming job ==="
echo "    Press Ctrl-C after ~${DURATION}s to stop."
docker exec imdb_spark python -m streaming.streaming_job --duration "$DURATION"

echo "Done. Streaming output is in output/streaming/"


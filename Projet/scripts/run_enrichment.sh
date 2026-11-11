#!/usr/bin/env bash
# Runs the Data Enrichment pipeline inside the Spark container.
#
# Usage (from the project root, containers started):
#   bash scripts/run_enrichment.sh [--dry-run] [--min-votes N] [--top-n N]
#
# Options are forwarded verbatim to enrichment.pipeline:
#   --dry-run        Process only the first 20 films (safe smoke-test)
#   --min-votes N    Minimum numVotes threshold (default: 100)
#   --top-n N        Rows in Markdown summary tables (default: 25)
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: do not rewrite container paths

echo "=== IMDb Data Enrichment Pipeline ==="
docker exec imdb_spark python -m enrichment.pipeline "$@"
echo "Done. Results are in output/enrichment/"

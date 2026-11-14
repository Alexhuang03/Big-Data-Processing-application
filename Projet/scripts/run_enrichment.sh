#!/usr/bin/env bash
# Runs the data enrichment pipeline inside the Spark container.
#
# Usage (from the project root, containers started, silver layer built):
#   bash scripts/run_enrichment.sh [options]
# Options are passed to enrichment.pipeline, for example:
#   --limit-films 30   quick test on 30 films
#   --stage gather     run only one stage (select, gather, build)
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: do not rewrite container paths

docker exec imdb_spark python -m enrichment.pipeline "$@"
echo "Done. Results are in output/enrichment/"

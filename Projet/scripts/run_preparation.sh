#!/usr/bin/env bash
# Runs the whole data preparation pipeline inside the Spark container.
# Usage (from the project root, containers started): bash scripts/run_preparation.sh
set -euo pipefail
export MSYS_NO_PATHCONV=1  # Git Bash on Windows: do not rewrite container paths

run() {
  echo "=== $* ==="
  docker exec imdb_spark python -m "$@"
}

run preparation.download_imdb
run preparation.ingest
run preparation.null_report
run preparation.quality_checks --stage typed
run preparation.clean
run preparation.quality_checks --stage silver
echo "Done. Results are in output/preparation/"

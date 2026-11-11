#!/usr/bin/env bash
# Runs the full open question investigation inside the Spark container.
# Usage (from the project root, containers started): bash scripts/run_open_question.sh
set -euo pipefail
export MSYS_NO_PATHCONV=1

echo "=== open_question.director_actor_synergy ==="
docker exec imdb_spark python -m open_question.director_actor_synergy

echo "Done. Results are in output/open_question/"


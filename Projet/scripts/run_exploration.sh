#!/usr/bin/env bash
# Runs all data exploration jobs inside the Spark container.
# Usage (from the project root, containers started): bash scripts/run_exploration.sh
set -euo pipefail
export MSYS_NO_PATHCONV=1  # Git Bash on Windows: do not rewrite container paths

run() {
  echo "=== $* ==="
  docker exec imdb_spark python -m "$@"
}

# Q1: Runtime outliers (z-score, then IQR)
run exploration.q1_runtime_outliers
run exploration.q1_runtime_outliers --method iqr

# Q2 & Q3: Genre popularity and comparison
run exploration.q2_q3_genre_popularity

# Q4: One-hit wonder directors
run exploration.q4_one_hit_directors

# Q5: TV series declining in final season
run exploration.q5_declining_tv_series

# Q6: Multilingual successful actors/actresses
run exploration.q6_multilingual_actors

# Q7: Actors whose popularity peaked early in their career
run exploration.q7_career_arc

# Q8: Multi-talented film professionals
run exploration.q8_multi_talented

echo "Done. Results are in output/exploration/"


#!/bin/bash
# Local test of the MapReduce logic - no Hadoop needed, only bash, sort and python3.
# `sort` plays the role of Hadoop's shuffle. The pipeline is run with and without the combiner;
# both results must equal the hand-computed expected output.
set -euo pipefail

DIR=$(cd "$(dirname "$0")" && pwd)
MR="$DIR/../mapreduce"
INPUT="$DIR/data/measurements_sample.csv"
EXPECTED="$DIR/data/expected_station_month_stats.tsv"
export LC_ALL=C

without_combiner=$(python3 "$MR/mapper.py" < "$INPUT" | sort | python3 "$MR/reducer.py")
with_combiner=$(python3 "$MR/mapper.py" < "$INPUT" | sort | python3 "$MR/combiner.py" | sort | python3 "$MR/reducer.py")

diff <(echo "$without_combiner") "$EXPECTED" && echo "PASS: without combiner"
diff <(echo "$with_combiner") "$EXPECTED" && echo "PASS: with combiner"

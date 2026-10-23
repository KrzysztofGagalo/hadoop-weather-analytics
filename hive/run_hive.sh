#!/bin/bash
# Runs the Hive script
# Usage: run_hive.sh input_dir3 input_dir4 output_dir6

# exactly 3 parameters are required
if [ $# -ne 3 ]; then
    echo "Usage: $0 input_dir3 input_dir4 output_dir6"
    exit 1
fi

INPUT_DIR3=$1       # HDFS directory with the MapReduce result, e.g. /project1/output_mr3
INPUT_DIR4=$2       # HDFS directory with the stations dictionary, e.g. /tmp/input/datasource4
OUTPUT_DIR6=$3      # HDFS directory for the final JSON result, e.g. /project1/output6

# directory of this script (hive.hql lives next to it)
SCRIPT_DIR=$(dirname "$0")

# execute hive.hql with beeline:
#   -u          HiveServer2 address and the default database
#   -n          current OS user; without it Hive connects anonymously and cannot write to HDFS
#   --hivevar   values for the ${...} placeholders in hive.hql
#   -f          file with the statements to run
beeline -u jdbc:hive2://localhost:10000/default -n "$(whoami)" \
    --hivevar input_dir3="$INPUT_DIR3" \
    --hivevar input_dir4="$INPUT_DIR4" \
    --hivevar output_dir6="$OUTPUT_DIR6" \
    -f "$SCRIPT_DIR/hive.hql"

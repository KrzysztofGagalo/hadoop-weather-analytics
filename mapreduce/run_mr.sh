#!/bin/bash
# Runs the MapReduce job (Hadoop Streaming)
# Usage: run_mr.sh input_dir1 output_dir3

# exactly 2 parameters are required ($# = number of parameters)
if [ $# -ne 2 ]; then
    echo "Usage: $0 input_dir1 output_dir3"
    exit 1
fi

INPUT_DIR1=$1       # HDFS directory with raw measurements, e.g. /tmp/input/datasource1
OUTPUT_DIR3=$2      # HDFS directory for the result, e.g. /project1/output_mr3

# directory of this script (the .py files live next to it),
# so the script works no matter which directory it is called from
SCRIPT_DIR=$(dirname "$0")

# remove the previous result - Hadoop refuses to start if the output directory exists
hadoop fs -rm -r -f "$OUTPUT_DIR3"

# Hadoop Streaming job:
#   -files     local files shipped by Hadoop to every worker
#   -input     HDFS input directory
#   -output    HDFS output directory (must not exist)
#   -mapper    command run on every input split
#   -combiner  partial aggregation on the worker, before the shuffle
#   -reducer   final aggregation for each key
mapred streaming \
    -files "$SCRIPT_DIR/mapper.py,$SCRIPT_DIR/combiner.py,$SCRIPT_DIR/reducer.py" \
    -input "$INPUT_DIR1" \
    -output "$OUTPUT_DIR3" \
    -mapper "python3 mapper.py" \
    -combiner "python3 combiner.py" \
    -reducer "python3 reducer.py"

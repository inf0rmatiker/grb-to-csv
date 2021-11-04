#!/bin/bash

function print_usage {
  echo -e "USAGE\n\t./run.sh <grb_input_dir> <csv_output_dir>\n"
}

[[ $# -ne 2 ]] && print_usage && exit 1

INPUT_DIR=$1
OUTPUT_DIR=$2

echo "Executing ./bin/python3 process.py $INPUT_DIR $OUTPUT_DIR"
./bin/python3 process.py "$INPUT_DIR" "$OUTPUT_DIR"
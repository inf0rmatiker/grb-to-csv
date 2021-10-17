#!/bin/bash

echo -e "Usage: ./import.sh <path_to_csv_files>"
for FILE in `ls $1*.csv`; do
  echo -e "\nProcessing file $FILE...\n"
  mongoimport --host=lattice-150 --port=27018 --db=sustaindb --collection=noaa_nam_sharded --type=csv --headerline --file=$FILE
done

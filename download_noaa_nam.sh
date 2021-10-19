#!/bin/bash

function print_usage {
  echo -e "NAME\n\tdownload_noaa_nam.sh\n\nDESCRIPTION\n\tDownloads original North American Mesoscale (NOAA NAM) historical data."
  echo -e "\tThe data downloaded is in the gridded binary (GRIB/.grb) format, and must be converted to .csv before"
  echo -e "\tbeing used or imported into MongoDB. Original data is pulled with wget from the following url:"
  echo -e "\thttps://www.ncei.noaa.gov/data/north-american-mesoscale-model/access/historical/analysis/\n"
  echo -e "USAGE\n\t./download_noaa_nam.sh <YYYY> <MM> <output_dir>\n"
  echo -e "\tExample: ./download_noaa_nam.sh 2010 05 ../original/\n"
}

if [[ $# -eq 3 ]]; then
  YEAR=$1
  MONTH=$2
  OUT=$3
  URL="https://www.ncei.noaa.gov/data/north-american-mesoscale-model/access/historical/analysis/${YEAR}${MONTH}/"
  wget --recursive --no-parent --level=2 --accept=grb --directory-prefix=$OUT --no-directories $URL

  #DOWNLOAD_DIR="$OUT/www.ncei.noaa.gov/data/north-american-mesoscale-model/access/historical/analysis/${YEAR}${MONTH}"
  #for YEAR_MONTH_DAY_DIR in `ls $DOWNLOAD_DIR`; do
  #  mv $DOWNLOAD_DIR/$YEAR_MONTH_DAY_DIR/*.grb $OUT
  #done
else
  print_usage
fi

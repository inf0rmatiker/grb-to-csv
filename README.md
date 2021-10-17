# grb-to-csv

Uses pygrib to process [NOAA NAM](https://www.ncei.noaa.gov/products/weather-climate-models/north-american-mesoscale) gridded binary (GRIB, .grb) data to CSV format for ingesting into MongoDB.

## Data

Download and unzip the [gridded binary datasets](https://www.ncei.noaa.gov/data/north-american-mesoscale-model/access/historical/analysis/) from NOAA NAM to a chosen directory, `<data_dir>`.

## Installation

Run `./install.sh`, which creates a virtual environment and installs the `pymongo`, `cython`, `pygrib`, `pandas` dependencies.

## Usage

Convert .grb to .csv:

- With the data downloaded to `<data_dir>`, execute `./bin/python3 process.py <year> <month> <in_prefix> <out_prefix> <latlons_to_gisjoins_csv_file>`
  - Example: `./bin/python3 process.py 2010 07 ~/NOAA/original ~/NOAA/processed gisjoin_mappings.csv`

Import to MongoDB:

- Run `./import.sh <path_to_processed>`
  - Example: `./import.sh ../processed` 

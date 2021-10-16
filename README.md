# grb-to-csv

Uses pygrib to process [NOAA NAM](https://www.ncei.noaa.gov/products/weather-climate-models/north-american-mesoscale) gridded binary (GRIB, .grb) data to CSV format for ingesting into MongoDB.

## Data

Download and unzip the [gridded binary datasets](https://www.ncei.noaa.gov/data/north-american-mesoscale-model/access/historical/analysis/) from NOAA NAM to a chosen directory, `<data_dir>`.

## Installation

Run `./install.sh`, which creates a virtual environment and installs the `pymongo`, `cython`, `pygrib`, `pandas` dependencies.

## Usage

With the data downloaded to `<data_dir>`, run `./run.sh <path_to_data_dir>`

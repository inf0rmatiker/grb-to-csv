# grb-to-csv

Uses pygrib to process [NOAA NAM](https://www.ncei.noaa.gov/products/weather-climate-models/north-american-mesoscale) gridded binary (GRIB, .grb, GRIB2, .grb2) data to CSV format for ingesting into MongoDB.

## Data

Download and unzip the [gridded binary datasets](https://www.ncei.noaa.gov/data/north-american-mesoscale-model/access/) from NOAA NAM to a chosen directory, `<data_dir>`.

## Installation

Run `./install.sh`, which creates a virtual environment and installs the `pymongo`, `cython`, `pygrib`, `pandas` dependencies.

## Usage

Inspect the files and select the fields you want included.
Determine the GRIB row indices for these fields, and choose a field name for that row, putting the tuple in `selected_fields.csv`.
See the existing file for examples.

Convert .grb/.grb2 to .csv:

- With the data downloaded to `<grb_input_dir>`, execute `./run.sh <grb_input_dir> <csv_output_dir>`
  - Example: `./run.sh ~/local-disk/b/nobackup/galileo/datasets/noaa_nam/original ~/local-disk/b/nobackup/galileo/datasets/noaa_nam/processed`
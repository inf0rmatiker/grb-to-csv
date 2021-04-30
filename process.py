import pygrib
import time
import pymongo
import sys
import os
import pandas as pd

# 428 rows x 614 cols = 262,792 entries. Some do not fall within a gisjoin.
# { key=row: value={ key=col: value=gisjoin } }
row_col_to_gisjoins = {}
is_loaded = False


# Finds the GISJoin that a lat/lon point falls into.
# If the point doesn't fall into any GISJoin, None is returned.
def lat_lon_to_gisjoin(mongo_client, lat, lon):
    sustain_db = mongo_client["sustaindb"]
    county_geo_col = sustain_db["county_geo"]

    query = {
        "geometry": {
            "$geoIntersects": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat]
                }
            }
        }
    }

    docs = county_geo_col.find(query)
    try:
        doc = next(docs)
        gisjoin = doc["properties"]["GISJOIN"]
        return gisjoin
    except Exception:
        return None


# Iterates over all latitude, longitude pairs in grb file,
# looks up which gisjoin they fall into, then saves the
# row/col->gisjoin mapping to a csv file.
def create_row_col_to_gisjoin_mappings(grb_file, csv_file):
    print("Converting lats/lons to gisjoins and caching results to file {csv_file}...")
    grbs = pygrib.open(grb_file)
    grb = grbs.message(1)
    lats, lons = grb.latlons()
    rows, cols = (lats.shape[0], lats.shape[1])
    mongo_client = pymongo.MongoClient("mongodb://lattice-100:27018/")
    with open(csv_file, "w") as f:
        f.write("gisjoin,row,col\n")
        for row in range(71, rows):  # there are no points within gisjoins until row 72
            for col in range(cols):
                lat = lats[row][col]
                lon = lons[row][col]
                gisjoin = lat_lon_to_gisjoin(mongo_client, lat, lon)
                if gisjoin:
                    f.write(f"{gisjoin},{row},{col}\n")

    grbs.close()


# Reads a previously-saved csv file containing row/col->gisjoin mappings
# into the row_col_to_gisjoins mapping dictionary.
def read_row_col_to_gisjoin_mappings(csv_file):
    global row_col_to_gisjoins
    data = pd.read_csv(csv_file, sep=',', header='infer')
    for index, df_row in data.iterrows():
        row = df_row['row']
        col = df_row['col']
        gisjoin = df_row['gisjoin']

        if row not in row_col_to_gisjoins:
            row_col_to_gisjoins[row] = {col: gisjoin}
        else:
            row_col_to_gisjoins[row][col] = gisjoin


def convert_grb_to_csv(grb_file, out_path, year, month, day, hour, timestep, start_time,
                       lat_lon_to_gisjoin_mappings_file):

    global is_loaded

    print("Using cached mappings file: ", lat_lon_to_gisjoin_mappings_file)
    if not os.path.isfile(lat_lon_to_gisjoin_mappings_file):
        create_row_col_to_gisjoin_mappings(grb_file, lat_lon_to_gisjoin_mappings_file)

    if not is_loaded:
        before = time.time()
        read_row_col_to_gisjoin_mappings(lat_lon_to_gisjoin_mappings_file)
        is_loaded = True
        after = time.time()
        print("Took %s to load gisjoin mappings" % time_elapsed(before, after))

    grbs = pygrib.open(grb_file)
    grb = grbs.message(1)
    lats, lons = grb.latlons()
    rows = list(row_col_to_gisjoins.keys())
    year_month_day_hour = f"{year}{month}{day}{hour}"
    out_file = f"{out_path}/{year}_{month}_{day}_{hour}_{timestep}.csv"

    selected_fields = [
        (2, "Mean sea level pressure", "mean_sea_level_pressure_pascal"),
        (3, "Surface pressure", "surface_pressure_surface_level_pascal"),
        (4, "Orography", "orography_surface_level_meters"),
        (5, "Temperature", "temp_surface_level_kelvin"),
        (6, "2 metre temperature", "2_metre_temp_kelvin"),
        (7, "2 metre dewpoint temperature", "2_metre_dewpoint_temp_kelvin"),
        (8, "Relative humidity", "relative_humidity_percent"),
        (9, "10 metre U wind component", "10_metre_u_wind_component_meters_per_second"),
        (10, "10 metre V wind component", "10_metre_v_wind_component_meters_per_second"),
        (11, "Total Precipitation", "total_precipitation_kg_per_squared_meter"),
        (12, "Convective precipitation (water)", "water_convection_precipitation_kg_per_squared_meter"),
        (291, "Soil Temperature", "soil_temperature_kelvin"),
        (339, "Pressure", "pressure_pascal"),
        (347, "Visibility", "visibility_meters"),
        (348, "Precipitable water", "precipitable_water_kg_per_squared_meter"),
        (358, "Total Cloud Cover", "total_cloud_cover_percent"),
        (362, "Snow depth", "snow_depth_meters"),
        (382, "Ice cover (1=ice, 0=no ice)", "ice_cover_binary")
    ]

    print(f"Processing/Writing {out_file}...")

    with open(out_file, "w") as f:
        # Create csv header row with new column names
        csv_header_row = "year_month_day_hour,timestep,gis_join,latitude,longitude,"
        csv_header_row += ",".join([field[2] for field in selected_fields])
        f.write(csv_header_row + "\n")
        next_target = 10.0

        for index, row in enumerate(rows):  # Get only the row keys which fall into gisjoins
            before = time.time()
            for col in list(row_col_to_gisjoins[row].keys()):  # Get only the col keys which fall into gisjoins
                lat = lats[row][col]
                lon = lons[row][col]
                gisjoin = row_col_to_gisjoins[row][col]

                percent_done = (index / len(rows)) * 100.0
                if percent_done >= next_target:
                    print("  %.2f percent done: row/col=[%d][%d], time elapsed: %s" % (
                        percent_done, row, col, time_elapsed(start_time, time.time())))
                    next_target += 10.0

                if gisjoin:
                    csv_row = f"{year_month_day_hour},{timestep},{gisjoin},{lat},{lon}"
                    for selected_field in selected_fields:
                        grb = grbs.message(selected_field[0])
                        value_for_field = grb.values[row][col]
                        csv_row += f",{value_for_field}"

                    f.write(csv_row + "\n")

            after = time.time()
            print("Took %s to complete row %d" % (time_elapsed(before, after), row))

    grbs.close()


def print_usage():
    print("\nUSAGE:\n\tpython3 process.py <year> <month> <in_prefix> <out_prefix> <latlons_to_gisjoins_csv_file>")
    print("\t  Note: if the lats/lons -> gisjoins csv mapping file does not exist yet, it will be created.")
    print("\nEXAMPLE:\n\tpython3 process.py 2010 01 ~/NOAA/original ~/NOAA/processed\n")


def test():
    lat = 40.585258
    lon = -105.084419
    query = {
        "geometry": {
            "$geoIntersects": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat]
                }
            }
        }
    }

    mongo_client = pymongo.MongoClient("mongodb://lattice-100:27018/")
    sustain_db = mongo_client["sustaindb"]
    county_geo_col = sustain_db["county_geo"]

    docs = county_geo_col.find(query)
    gisjoin = next(docs)["properties"]["GISJOIN"]
    print(gisjoin)


def test_grb():
    grbs = pygrib.open("/s/parsons/b/others/sustain/NOAA/original/201001/20100103/namanl_218_20100103_0600_000.grb")
    grb = grbs.message(1)
    lats, lons = grb.latlons()
    rows, cols = (lats.shape[0], lats.shape[1])
    print(rows, cols)

    count = 0
    for col in range(cols):
        for row in range(rows):
            lat = lats[row][col]
            lon = lons[row][col]

            grb = grbs.message(5)
            value_for_field = grb.values[row][col]

            print(f"{lon}, {lat}")
            count += 1
            if count == 500:
                grbs.close()
                exit(0)


def get_files(dir_name):
    list_of_files = os.listdir(dir_name)
    complete_file_list = list()
    for file in list_of_files:
        complete_path = os.path.join(dir_name, file)
        if os.path.isdir(complete_path):
            complete_file_list = complete_file_list + get_files(complete_path)
        else:
            complete_file_list.append(complete_path)

    return complete_file_list


def count_files(path):
    filenames = get_files(path)
    grb_filenames = [filename for filename in filenames if filename.endswith(".grb")]
    count = len(grb_filenames)

    return count


def print_time(start_time):
    print(f"Time elapsed: {time_elapsed(start_time, time.time())}")


def time_elapsed(t1, t2):
    diff = t2 - t1
    seconds = diff
    minutes = -1

    retval = f"{int(seconds)} seconds"
    if seconds > 60:
        minutes = seconds // 60
        seconds = seconds % 60
        retval = f"{int(minutes)} minutes, {int(seconds)} seconds"
    if minutes > 60:
        hours = minutes // 60
        minutes = minutes % 60
        retval = f"{int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds"

    return retval


def main():
    if len(sys.argv) < 6:
        print_usage()
        exit(1)

    year_str = sys.argv[1]
    month_str = sys.argv[2]
    data_dirs_prefix = sys.argv[3]
    out_file_prefix = sys.argv[4]
    lat_lon_to_gisjoin_mappings_file = sys.argv[5]
    year_month_dir_path = f"{data_dirs_prefix}/{year_str}{month_str}"

    start_time = time.time()

    file_count = 0
    total_files = count_files(data_dirs_prefix)
    print(f"Total files to process: {total_files}")
    print_time(start_time)

    for day_dir in sorted(os.listdir(year_month_dir_path)):
        day_str = day_dir[len(day_dir) - 2:]
        day_dir_path = f"{year_month_dir_path}/{day_dir}"
        filenames = os.listdir(day_dir_path)
        grb_filenames = [filename for filename in filenames if filename.endswith(".grb")]

        for grb_file in sorted(grb_filenames):
            hour_str = grb_file[20:22]
            timestep = grb_file[27:28]
            grb_file_path = f"{day_dir_path}/{grb_file}"

            convert_grb_to_csv(grb_file_path, out_file_prefix, year_str, month_str, day_str, hour_str, timestep,
                               start_time, lat_lon_to_gisjoin_mappings_file)
            file_count += 1
            print(f"Finished converting file: [{file_count}/{total_files}]")
            print_time(start_time)


if __name__ == '__main__':
    main()

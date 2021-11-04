import pygrib
import time
import pymongo
import sys
import os
import pandas as pd

# 428 rows x 614 cols = 262,792 entries. Some do not fall within a gisjoin
# { key=row: value={ key=col: value=gisjoin } }
row_col_to_gisjoins = {}

# 3192 gisjoins
# { key=gisjoin: value=[ (row,col), (row,col), ..., ] ) }
gisjoin_to_row_col = {}

is_loaded = False

# Fields we want from the .grb2 files:
# (<grb_row_index>, <assigned_field_name>)
selected_grb_2_fields = {
    (1,   "PRESSURE_REDUCED_TO_MSL_PASCAL"),
    (4,   "VISIBILITY_AT_SURFACE_METERS"),
    (5,   "VISIBILITY_AT_CLOUD_TOP_METERS"),
    (8,   "U_COMPONENT_OF_WIND_AT_PLANETARY_BOUNDARY_LAYER_METERS_PER_SEC"),
    (9,   "V_COMPONENT_OF_WIND_AT_PLANETARY_BOUNDARY_LAYER_METERS_PER_SEC"),
    (11,  "WIND_GUST_SPEED_AT_SURFACE_METERS_PER_SEC"),
    (337, "PRESSURE_AT_SURFACE_PASCAL"),
    (339, "TEMPERATURE_AT_SURFACE_KELVIN"),
    (340, "SOIL_TEMPERATURE_0_TO_0.1_M_BELOW_SURFACE_KELVIN"),
    (341, "VOLUMETRIC_SOIL_MOISTURE_CONTENT_0_TO_0.1_M_BELOW_SURFACE_FRACTION"),
    (357, "SNOW_COVER_AT_SURFACE_PERCENT"),
    (358, "SNOW_DEPTH_AT_SURFACE_METERS"),
    (361, "TEMPERATURE_2_METERS_ABOVE_SURFACE_KELVIN"),
    (363, "DEWPOINT_TEMPERATURE_2_METERS_ABOVE_SURFACE_KELVIN"),
    (364, "RELATIVE_HUMIDITY_2_METERS_ABOVE_SURFACE_PERCENT"),
    (365, "U_COMPONENT_OF_WIND_2_METERS_ABOVE_SURFACE_METERS_PER_SEC"),
    (365, "V_COMPONENT_OF_WIND_2_METERS_ABOVE_SURFACE_METERS_PER_SEC"),
    (367, "TOTAL_PRECIPITATION_SURFACE_ACCUM_KG_PER_SQ_METER"),
    (368, "CONVECTIVE_PRECIPITATION_SURFACE_ACCUM_KG_PER_SQ_METER"),
    (370, "GEOPOTENTIAL_HEIGHT_LOWEST_LEVEL_WET_BULB_ZERO_GPM"),
    (371, "CATEGORICAL_SNOW_SURFACE_BINARY"),
    (372, "CATEGORICAL_ICE_PELLETS_SURFACE_BINARY"),
    (373, "CATEGORICAL_FREEZING_RAIN_SURFACE_BINARY"),
    (374, "CATEGORICAL_RAIN_SURFACE_BINARY"),
    (378, "VEGETATION_SURFACE_PERCENT"),
    (379, "VEGETATION_TYPE_SIB_INT"),
    (380, "SOIL_TYPE_ZOBLER_INT"),
    (392, "PRESSURE_TROPOPAUSE_PASCAL"),
    (393, "TEMPERATURE_TROPOPAUSE_KELVIN"),
    (394, "U_COMPONENT_OF_WIND_TROPOPAUSE_METERS_PER_SEC"),
    (395, "U_COMPONENT_OF_WIND_TROPOPAUSE_METERS_PER_SEC")
}


def load_selected_fields():
    selected_fields_df = pd.read_csv("selected_fields.csv", header=0, delimiter=",")
    return selected_fields_df


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
def create_gisjoin_to_row_col_mappings(grb_file, csv_file):
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
# into the gisjoin_to_row_cols mapping dictionary.
def read_gisjoin_to_row_col_mappings(csv_file):
    global gisjoin_to_row_col
    data = pd.read_csv(csv_file, sep=',', header='infer')
    for index, df_row in data.iterrows():
        row = df_row['row']
        col = df_row['col']
        gisjoin = df_row['gisjoin']

        if gisjoin not in gisjoin_to_row_col:
            gisjoin_to_row_col[gisjoin] = [(row, col)]
        else:
            gisjoin_to_row_col[gisjoin].append((row, col))


def convert_grb_to_csv(grb_file, out_path, year, month, day, hour, timestep, start_time,
                       lat_lon_to_gisjoin_mappings_file):
    global is_loaded

    if not is_loaded:
        print("Using cached mappings file: ", lat_lon_to_gisjoin_mappings_file)
        if not os.path.isfile(lat_lon_to_gisjoin_mappings_file):
            create_gisjoin_to_row_col_mappings(grb_file, lat_lon_to_gisjoin_mappings_file)

        print("GISJoin mappings not loaded yet, loading now...")
        before = time.time()
        read_gisjoin_to_row_col_mappings(lat_lon_to_gisjoin_mappings_file)
        is_loaded = True
        after = time.time()
        print("Took %s to load gisjoin mappings" % time_elapsed(before, after))

    grbs = pygrib.open(grb_file)

    grb = grbs.message(1)
    lats, lons = grb.latlons()

    year_month_day_hour = f"{year}{month}{day}{hour}"
    out_file = f"{out_path}/{year}_{month}_{day}_{hour}_{timestep}.csv"

    print(f"Processing/Writing {out_file}...")
    selected_fields = load_selected_fields()

    with open(out_file, "w") as f:
        # Create csv header row with new column names
        csv_header_row = "YYYYMMDDHH,TIMESTEP,GISJOIN,LATITUDE,LONGITUDE,"
        csv_header_row += ",".join([field[2] for field in selected_fields])
        f.write(csv_header_row + "\n")

        # Build values list
        values_list = []
        before = time.time()
        for selected_field in selected_fields:
            grb_index = selected_field[0]
            grb = grbs.message(grb_index)
            values_list.append(grb.values)

        after = time.time()
        print("Took %s to complete building values_list" % (time_elapsed(before, after)))

        # Retrieve values and print to file in csv format
        processed_gisjoins = 0
        total_gisjoins = 3912
        next_target = 10.0
        for gisjoin, row_col_tuples in gisjoin_to_row_col.items():
            for row_col_tuple in row_col_tuples:
                row = row_col_tuple[0]
                col = row_col_tuple[1]
                lat = lats[row][col]
                lon = lons[row][col]

                percent_done = (processed_gisjoins / total_gisjoins) * 100.0
                if percent_done >= next_target:
                    print("  %.2f percent done: gisjoins (%d/%d), time elapsed: %s" % (
                        percent_done, processed_gisjoins, total_gisjoins, time_elapsed(start_time, time.time())))
                    next_target += 10.0

                csv_row = f"{year_month_day_hour},{timestep},{gisjoin},{lat},{lon}"
                for values in values_list:
                    csv_row += f",{values[row][col]}"

                f.write(csv_row + "\n")
            processed_gisjoins += 1

    grbs.close()


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
    return len(grb_filenames)


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


def print_usage():
    print("\nUSAGE\n\t./bin/python3 process.py <input_dir> <output_dir>")
    print("\tNote: if the lats/lons -> gisjoins csv mapping file does not exist yet, it will be created.")
    print("\tExample:\t./bin/python3 process.py ~/NOAA/original ~/NOAA/processed\n")


def test():
    selected_fields = load_selected_fields()
    csv_header_row = "YYYYMMDDHH,TIMESTEP,GISJOIN,LATITUDE,LONGITUDE,"
    csv_header_row += ",".join([field[2] for field in selected_fields])
    print(csv_header_row)


def main():
    test()
    '''
    if len(sys.argv) != 3:
        print_usage()
        exit(1)

    lat_lon_to_gisjoin_mappings_file = "./gisjoin_mappings.csv"
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    start_time = time.time()

    file_count = 0
    total_files = count_files(input_path)
    print(f"Total files to process: {total_files}")
    print_time(start_time)

    filenames = sorted(os.listdir(input_path))
    grb_filenames = [filename for filename in filenames if filename.endswith(".grb")]
    for grb_file in grb_filenames:
        # Filename looks like: "namanl_218_20101129_0600_006.grb"
        grb_file_fields = grb_file.split('_')
        yyyymmdd = grb_file_fields[2]
        hour = grb_file_fields[3]
        timestep = grb_file_fields[4][:-4]
        yyyy = yyyymmdd[:4]
        mm = yyyymmdd[4:6]
        dd = yyyymmdd[6:]

        input_file_path = f"{input_path}{grb_file}" if input_path.endswith("/") else f"{input_path}/{grb_file}"
        output_path = output_path if not output_path.endswith("/") else output_path[:-1]  # remove trailing slash

        convert_grb_to_csv(input_file_path, output_path, yyyy, mm, dd, hour, timestep,
                           start_time, lat_lon_to_gisjoin_mappings_file)
        file_count += 1
        print(f"Finished converting file: [{file_count}/{total_files}]")
        print_time(start_time)

    '''


if __name__ == '__main__':
    main()

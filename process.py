import pygrib
import time
import pymongo
import sys
import os
import pandas as pd

# 428 rows x 614 cols = 262,792 entries. Some do not fall within a gis_join
# { key=row: value={ key=col: value=gis_join } }
row_col_to_gis_joins = {}

# 3192 gis_joins
# { key=gis_join: value=[ (row,col), (row,col), ..., ] ) }
gis_join_to_row_col = {}

is_gis_join_mappings_loaded = False


def load_selected_fields():
    selected_fields_df = pd.read_csv("selected_fields.csv", header=0, delimiter=",")
    return selected_fields_df


# Finds the GISJoin that a lat/lon point falls into.
# If the point doesn't fall into any GISJoin, None is returned.
def lat_lon_to_gis_join(mongo_client, lat, lon):
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
        gis_join = doc["properties"]["GISJOIN"]
        return gis_join
    except Exception:
        return None


# Iterates over all latitude, longitude pairs in grb file,
# looks up which gis_join they fall into, then saves the
# row/col->gis_join mapping to a csv file.
def create_gis_join_to_row_col_mappings(grb_file, csv_file):
    print("Converting lats/lons to gis_joins and caching results to file {csv_file}...")
    grbs = pygrib.open(grb_file)
    grb = grbs.message(1)
    lats, lons = grb.latlons()
    rows, cols = (lats.shape[0], lats.shape[1])
    mongo_client = pymongo.MongoClient("mongodb://lattice-100:27018/")
    with open(csv_file, "w") as f:
        f.write("gis_join,row,col\n")
        for row in range(71, rows):  # there are no points within gis_joins until row 72
            for col in range(cols):
                lat = lats[row][col]
                lon = lons[row][col]
                gis_join = lat_lon_to_gis_join(mongo_client, lat, lon)
                if gis_join:
                    f.write(f"{gis_join},{row},{col}\n")

    grbs.close()


# Reads a previously-saved csv file containing row/col->gis_join mappings
# into the gis_join_to_row_cols mapping dictionary.
def read_gis_join_to_row_col_mappings(csv_file):
    global gis_join_to_row_col
    data = pd.read_csv(csv_file, sep=',', header='infer')
    for index, df_row in data.iterrows():
        row = df_row['row']
        col = df_row['col']
        gis_join = df_row['gis_join']

        if gis_join not in gis_join_to_row_col:
            gis_join_to_row_col[gis_join] = [(row, col)]
        else:
            gis_join_to_row_col[gis_join].append((row, col))


def convert_grb_to_csv(grb_file, out_path, year, month, day, hour, timestep, start_time,
                       lat_lon_to_gis_join_mappings_file):

    global is_gis_join_mappings_loaded
    if not is_gis_join_mappings_loaded:
        print("Using cached mappings file: ", lat_lon_to_gis_join_mappings_file)
        if not os.path.isfile(lat_lon_to_gis_join_mappings_file):
            create_gis_join_to_row_col_mappings(grb_file, lat_lon_to_gis_join_mappings_file)

        print("GISJoin mappings not loaded yet, loading now...")
        before = time.time()
        read_gis_join_to_row_col_mappings(lat_lon_to_gis_join_mappings_file)
        is_gis_join_mappings_loaded = True
        after = time.time()
        print("Took %s to load gis_join mappings" % time_elapsed(before, after))

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
        csv_header_row += ",".join([row['field_name'] for index, row in selected_fields.iterrows()])
        f.write(csv_header_row + "\n")

        # Build values list
        values_list = []
        before = time.time()
        for index, row in selected_fields.iterrows():
            grb_index = row['grb_row_index']
            grb = grbs.message(grb_index)
            values_list.append(grb.values)

        after = time.time()
        print("Took %s to complete building values_list" % (time_elapsed(before, after)))

        # Retrieve values and print to file in csv format
        processed_gis_joins = 0
        total_gis_joins = 3912
        next_target = 10.0
        for gis_join, row_col_tuples in gis_join_to_row_col.items():
            for row_col_tuple in row_col_tuples:
                row = row_col_tuple[0]
                col = row_col_tuple[1]
                lat = lats[row][col]
                lon = lons[row][col]

                percent_done = (processed_gis_joins / total_gis_joins) * 100.0
                if percent_done >= next_target:
                    print("  %.2f percent done: gis_joins (%d/%d), time elapsed: %s" % (
                        percent_done, processed_gis_joins, total_gis_joins, time_elapsed(start_time, time.time())))
                    next_target += 10.0

                csv_row = f"{year_month_day_hour},{timestep},{gis_join},{lat},{lon}"
                for values in values_list:
                    csv_row += f",{values[row][col]}"

                f.write(csv_row + "\n")
            processed_gis_joins += 1

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
    grb_filenames = [filename for filename in filenames if filename.endswith(".grb2")]
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
    print("\tNote: if the lats/lons -> gis_joins csv mapping file does not exist yet, it will be created.")
    print("\tExample:\t./bin/python3 process.py ~/local-disk/b/nobackup/galileo/datasets/noaa_nam/original "
          "~/local-disk/b/nobackup/galileo/datasets/noaa_nam/processed\n")


def main():

    if len(sys.argv) != 3:
        print_usage()
        exit(1)

    lat_lon_to_gis_join_mappings_file = "gis_join_mappings.csv"
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    start_time = time.time()

    file_count = 0
    total_files = count_files(input_path)
    print(f"Total files to process: {total_files}")
    print_time(start_time)

    filenames = sorted(os.listdir(input_path))
    grb_filenames = [filename for filename in filenames if filename.endswith(".grb2")]
    for grb_file in grb_filenames:

        # Filename looks like: "namanl_218_20101129_0600_006.grb2"
        grb_file_fields = grb_file.split('_')
        yyyymmdd = grb_file_fields[2]
        hour = grb_file_fields[3]
        timestep = grb_file_fields[4][:-5]
        yyyy = yyyymmdd[:4]
        mm = yyyymmdd[4:6]
        dd = yyyymmdd[6:]

        # Fix input/output paths' slashes for consistency
        input_file_path = f"{input_path}{grb_file}" if input_path.endswith("/") else f"{input_path}/{grb_file}"
        output_path = output_path if not output_path.endswith("/") else output_path[:-1]  # remove trailing slash

        # Convert grb file to csv
        convert_grb_to_csv(input_file_path, output_path, yyyy, mm, dd, hour, timestep,
                           start_time, lat_lon_to_gis_join_mappings_file)
        file_count += 1
        print(f"Finished converting file: [{file_count}/{total_files}]")
        print_time(start_time)


if __name__ == '__main__':
    main()

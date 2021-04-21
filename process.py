import pygrib
import time
import pymongo
import sys
import os
import math


class GISJoinLatLon:

    def __init__(self, center_lat, center_lon, gisjoin):
        self.center_lat = center_lat
        self.center_lon = center_lon
        self.gisjoin = gisjoin
        self.lat_index = -1
        self.lon_index = -1
        self.lat_entry = -1
        self.lon_entry = -1
        self.distance  = -1


    def __repr__(self):
        return f"GISJoin: {self.gisjoin}, Center Latitude/Longitude: ({self.center_lat}, {self.center_lon}),\nLat/Lon Indexes: [{self.lat_index}][{self.lon_index}], Lat/Lon Entries: ({self.lat_entry}, {self.lon_entry}), Distance: {self.distance}\n"


gisjoin_centers = []


def init_gisjoin_centers():
    global gisjoin_centers

    mongo_client = pymongo.MongoClient("mongodb://lattice-100:27018/")
    sustain_db = mongo_client["sustaindb"]
    county_geo_col = sustain_db["county_geo"]

    for doc in county_geo_col.find({}, {"properties.INTPTLAT10": 1, "properties.INTPTLON10": 1, "properties.GISJOIN": 1, "_id": 0}):
        lat = float(doc['properties']['INTPTLAT10'])
        lon = float(doc['properties']['INTPTLON10'])
        gisjoin = doc['properties']['GISJOIN']

        gisjoin_latlon = GISJoinLatLon(lat,lon,gisjoin)
        gisjoin_centers.append(gisjoin_latlon)


def euclidean_distance(lat1, lon1, lat2, lon2):
    return math.sqrt( (lat1 - lat2)**2 + (lon1 - lon2)**2 )


def find_gisjoin_indices(file_path):
    grbs = pygrib.open(file_path)
    grb  = grbs.message(1)
    lats, lons = grb.latlons()
    latsize = lats.shape[0]
    lonsize = lons.shape[1]

    for index, center in enumerate(gisjoin_centers):
        min_distance = sys.maxsize # infinity

        for lat_index in range(latsize):

            for lon_index in range(lonsize):
                lat = lats[lat_index][lon_index]
                lon = lons[lat_index][lon_index]
                distance_from_center = euclidean_distance(center.center_lat, center.center_lon, lat, lon)
                
                if distance_from_center < min_distance:
                    center.lat_index = lat_index
                    center.lon_index = lon_index
                    center.lat_entry = lat
                    center.lon_entry = lon
                    center.distance = distance_from_center
                    min_distance = distance_from_center

        print(f"[{index}/{len(gisjoin_centers)}] found center indexes for GISJOIN {center.gisjoin}")
        
    grbs.close()



def convert_grb_to_csv(file_path, out_path, year, month, day, hour, timestep):
    grbs = pygrib.open(file_path)
    grb = grbs.message(1)
    lats, lons = grb.latlons()
    latsize = lats.shape[0]
    lonsize = lons.shape[1]

    selected_fields = [
            (2,   "Mean sea level pressure", "mean_sea_level_pressure_pascal"),
            (3,   "Surface pressure", "surface_pressure_surface_level_pascal"),
            (4,   "Orography", "orography_surface_level_meters"),
            (5,   "Temperature", "temp_surface_level_kelvin"),
            (6,   "2 metre temperature", "2_metre_temp_kelvin"),
            (7,   "2 metre dewpoint temperature", "2_metre_dewpoint_temp_kelvin"),
            (8,   "Relative humidity", "relative_humidity_percent"),
            (9,   "10 metre U wind component", "10_metre_u_wind_component_meters_per_second"),
            (10,  "10 metre V wind component", "10_metre_v_wind_component_meters_per_second"),
            (11,  "Total Precipitation", "total_precipitation_kg_per_squared_meter"),
            (12,  "Convective precipitation (water)", "water_convection_precipitation_kg_per_squared_meter"),
            (291, "Soil Temperature", "soil_temperature_kelvin"),
            (339, "Pressure", "pressure_pascal"),
            (347, "Visibility", "visibility_meters"),
            (348, "Precipitable water", "precipitable_water_kg_per_squared_meter"),
            (358, "Total Cloud Cover", "total_cloud_cover_percent"),
            (362, "Snow depth", "snow_depth_meters"),
            (382, "Ice cover (1=ice, 0=no ice)", "ice_cover_binary")
        ]

    out_file = f"{out_path}/{year}_{month}_{day}_{hour}_{timestep}.csv"
    print(f"Processing/Writing {out_file}...")

    with open(out_file, "w") as f:
        # Create csv header row with new column names
        csv_header_row = "year_month_day_hour,timestep,gis_join"
        csv_header_row += ",".join([field[2] for field in selected_fields])
        f.write(csv_header_row + "\n")

        year_month_day_hour = f"{year}{month}{day}{hour}"

        for index, center in enumerate(gisjoin_centers):
            csv_row = f"{year_month_day_hour},{timestep},{center.gisjoin}"

            for selected_field in selected_fields:
                    grb = grbs.message(selected_field[0])
                    value_for_field = grb.values[center.lat_index][center.lon_index]
                    csv_row += f",{value_for_field}"

            f.write(csv_row + "\n")
            
    grbs.close()


def print_usage():
    print("\nUSAGE:\n\tpython3 process.py <year> <month> <in_prefix> <out_prefix>")
    print("\nEXAMPLE:\n\tpython3 process.py 2010 01 ~/NOAA/original ~/NOAA/processed\n")


def test():
    for month in range (1, 13):
        print(str(month).zfill(2))
       

def get_files(dir_name):
    list_of_files = os.listdir(dir_name)
    complete_file_list = list()
    for file in list_of_files:
        complete_path = os.path.join(dir_name, file)
        if os.path.isdir(complete_path):
            complete_file_list = complete_file_list + get_files(complete_path)
        else:
            complete_file_list.append(complete_path)

    return completeFileList



def count_files(path):
    filenames = get_files(path)
    grb_filenames = [ filename for filename in filenames if filename.endswith(".grb") ]
    count = len(grb_filenames)

    return count


def print_time(start_time):
    print(f"Time elapsed: {time_elapsed(start_time, time.time())}")


def time_elapsed(t1, t2):
    diff = t2 - t1
    seconds = diff
    minutes = -1
    hours = -1

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
    if len(sys.argv) < 5:
       print_usage()
       exit(1)


    year_str            = sys.argv[1]
    month_str           = sys.argv[2]
    data_dirs_prefix    = sys.argv[3]
    out_file_prefix     = sys.argv[4]

    start_time = time.time()

    init_gisjoin_centers()
    gisjoin_indices_found = False
    file_count = 0
    total_files = count_files(data_dirs_prefix)
    print(f"Total files to process: {total_files}")
    print_time(start_time)

    year_month_dir_path = f"{data_dirs_prefix}/{year_str}{month_str}"

    for day_dir in sorted(os.listdir(year_month_dir_path)):
        day_str = day_dir[len(day_dir)-2:]
        day_dir_path = f"{year_month_dir_path}/{day_dir}"
        filenames = os.listdir(day_dir_path)
        grb_filenames = [ filename for filename in filenames if filename.endswith(".grb") ]

        for grb_file in sorted(grb_filenames):
            hour_str = grb_file[20:22]
            timestep = grb_file[27:28]
            grb_file_path = f"{day_dir_path}/{grb_file}"
            
            if not gisjoin_indices_found:
                find_gisjoin_indices(grb_file_path)
                print_time(start_time)
                gisjoin_indices_found = True

            convert_grb_to_csv(grb_file_path, out_file_prefix, year_str, month_str, day_str, hour_str, timestep)
            file_count += 1
            print(f"Finished converting file: [{file_count}/{total_files}]")
            print_time(start_time)
            
            

if __name__ == '__main__':
    main()

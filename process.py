import pygrib

data_prefix = "/s/lattice-100/a/nobackup/galileo/sustain/noaa-nam/www.ncei.noaa.gov/data/north-american-mesoscale-model/access/historical/analysis"
year_month = "201001"
year_month_day = "20100105"
year_month_day_hour = "201001050000"
year_month_day_hour_int = int(year_month_day_hour)
filename = "namanl_218_20100105_0000_000.grb"
file_path = f"{data_prefix}/{year_month}/{year_month_day}/{filename}"

out_path = "/s/lattice-100/d/nobackup/galileo/NOAA/processed_csv"
out_filename = filename[:-4]
out_file = f"{out_path}/{out_filename}.csv"

grbs = pygrib.open(file_path)
grb = grbs.message(1)
lats, lons = grb.latlons()
latsize = lats.shape[0]
lonsize = lons.shape[1]

top_left_lat = 49.0
top_left_lon = -125.0
bottom_right_lat = 25.0
bottom_right_lon = -67.0

min_lat_index = 99999
max_lat_index = -1
min_lon_index = 99999
max_lon_index = -1

for lat_index in range(latsize):
    for lon_index in range(lonsize):
        lat = lats[lat_index][lon_index]
        lon = lons[lat_index][lon_index]

        if lat < top_left_lat and lat > bottom_right_lat and lon > top_left_lon and lon < bottom_right_lon:
            if lat_index < min_lat_index:
                min_lat_index = lat_index
            if lat_index > max_lat_index:
                max_lat_index = lat_index
            if lon_index < min_lon_index:
                min_lon_index = lon_index
            if lon_index > max_lon_index:
                max_lon_index = lon_index

print(f"Min lat,lon indices ({min_lat_index},{min_lon_index}) at lat,lon ({lats[min_lat_index][min_lon_index]},{lons[min_lat_index][min_lon_index]})")
print(f"Max lat,lon indices ({max_lat_index},{max_lon_index}) at lat,lon ({lats[max_lat_index][max_lon_index]},{lons[max_lat_index][max_lon_index]})")
lat_index_count = max_lat_index - min_lat_index
lon_index_count = max_lon_index - min_lon_index
total_index_count = lat_index_count * lon_index_count
print(f"Creating a box of [{lat_index_count},{lon_index_count}], or {total_index_count} indices")


#data, lats, lons = grb.data(lat1=48,lat2=25,lon1=-125,lon2=-66)
#print(f"Data shape: {data.shape}")
print(f"Latitude shape:  {lats.shape}")
print(f"Longitude shape: {lons.shape}")

# total   = latsize * lonsize

selected_fields = [
            (2,  "Mean sea level pressure", "mean_sea_level_pressure_pascal"),
            (3,  "Surface pressure", "surface_pressure_surface_level_pascal"),
            (4,  "Orography", "orography_surface_level_meters"),
            (5,  "Temperature", "temp_surface_level_kelvin"),
            (6,  "2 metre temperature", "2_metre_temp_kelvin"),
            (7,  "2 metre dewpoint temperature", "2_metre_dewpoint_temp_kelvin"),
            (8,  "Relative humidity", "relative_humidity_percent"),
            (9,  "10 metre U wind component", "10_metre_u_wind_component_meters_per_second"),
            (10, "10 metre V wind component", "10_metre_v_wind_component_meters_per_second"),
            (11, "Total Precipitation", "total_precipitation_kg_per_squared_meter"),
            (12, "Convective precipitation (water)", "water_convection_precipitation_kg_per_squared_meter"),
            (13, "Convective precipitation (water)", "water_convection_precipitation_kg_per_squared_meter"),
        ]

'''

# Each grb value is of shape (428, 614), or roughly 262,792 entries
with open(out_file, "w") as f:

    # Create csv header row with new column names
    csv_header_row = "year_month_day_hour,"
    csv_header_row += ",".join([field[2] for field in selected_fields])
    f.write(csv_header_row + "\n")

    # Iterate over all lat/lon indices
    current = 1
    for x in range(latsize):
        for y in range(lonsize):
            
            # Create csv row with first element being timestamp
            csv_row = f"{year_month_day_hour},"

            # Iterate over all selected variables
            for selected_field in selected_fields:
                index = selected_field[0]
                actual_field_name = selected_field[1]

                # Get next variable's value for lat/lon pair and add to csv row
                grbs.seek(index-1)
                grb = grbs.read(1)[0]
                value_for_field = grb.values[x][y]
                csv_row += f",{value_for_field}"
            

            f.write(csv_row + "\n")
            print("Lat: %.7f\tLon: %.7f\t[%d / %d]" % (lats[x][y], lons[x][y], current, total))
            current += 1
            grbs.seek(0) # Go back to beginning

'''
grbs.close()

def main():
    pass


if __name__ == '__main__':
    main()

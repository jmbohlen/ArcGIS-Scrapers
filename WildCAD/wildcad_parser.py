import requests
import geopandas as gpd
import geopy.distance
import datetime as dt
from shapely.geometry import Point
from IPython.core.display import clear_output
from bs4 import BeautifulSoup
from arcgis.features import FeatureLayerCollection
from arcgis.gis import GIS

# main WildCAD webpage with links to individual dispatch centers
response = requests.get('http://www.wildcad.net/WildCADWeb.asp')
wildweb_table = BeautifulSoup(response.text, 'html.parser').table('tr')

# Parse list of links to individual dispatch centers and format to Frame response of each dispatch center's incident data page
wildCAD_list = []
for row in wildweb_table[2:]:
    abbr = row('a')[0].text
    wildCAD_list.append('http://www.wildcad.net/WC' + abbr + 'open.htm')

# iterate through links to Frame responses and add data to GeoDataFrame. Each dispatch center has a different table with columns in different positions so the parsing consists of a lot of if/then statements to determine the table structure and scrape it properly.
wildCAD_df = gpd.GeoDataFrame()
for address in wildCAD_list:
    response = requests.get(address)
    try:
        data_rows = BeautifulSoup(response.text, 'html.parser').table('tr')
    except TypeError:
        continue
    data_columns = []
    wildfire_rows = []
    count = 0
    for x in BeautifulSoup(response.text, 'html.parser').table('tr')[1]('td'):
        data_columns.append(x.text)
    try:
        type_index = data_columns.index('Type')
    except ValueError:
        continue
    try:
        for row in data_rows[2:]:
            count += 1
            if row('td')[type_index].text == 'Wildfire':
                wildfire_rows.append(count)
    except IndexError:
        continue
    for y in wildfire_rows:
        if 'Date' in data_columns:
            date_index = data_columns.index('Date')
            date = data_rows[y + 1]('td')[date_index].text
        else:
            date = ''
        if 'Name' in data_columns:
            name_index = data_columns.index('Name')
            name = data_rows[y + 1]('td')[name_index].text
        else:
            name = ''
        if 'Location' in data_columns:
            location_index = data_columns.index('Location')
            location = data_rows[y + 1]('td')[location_index].text
        else:
            location = ''
        if 'Fuels' in data_columns:
            fuels_index = data_columns.index('Fuels')
            fuels = data_rows[y + 1]('td')[fuels_index].text
        else:
            fuels = ''
        if 'Acres' in data_columns:
            acres_index = data_columns.index('Acres')
            acres = data_rows[y + 1]('td')[acres_index].text
        else:
            acres = ''

        # Latitude and Longitued values provided in WildCAD are not in DDM so we must convert.
        if 'Lat/Lon' in data_columns:
            lat_lon_index = data_columns.index('Lat/Lon')
            lat_lon = data_rows[y + 1]('td')[lat_lon_index].text
            try:
                comma_index = lat_lon.index(',')
                lat_ddm = lat_lon[:comma_index]
                lon_ddm = lat_lon[(comma_index + 2):]
                try:
                    lat_space_index = lat_ddm.index(' ')
                    lat_degrees = float(lat_ddm[:lat_space_index])
                    lat_minutes = float(lat_ddm[lat_space_index + 1:])
                    lon_space_index = lon_ddm.index(' ')
                    lon_degrees = float(lon_ddm[:lon_space_index])
                    lon_minutes = float(lon_ddm[lon_space_index + 1:])
                    lat_dd = lat_degrees + (lat_minutes / 60)
                    lon_dd = -1 * ((-1 * lon_degrees) + (lon_minutes / 60))
                    geometry = Point(lon_dd, lat_dd)
                except ValueError:
                    geometry = Point(float(lon_ddm), float(lat_ddm))
            except ValueError:
                geometry = float('NaN')
        else:
            lat_lon = ''
            geometry = float('NaN')
        inc_type = data_rows[y + 1]('td')[type_index].text
        source = address
        fire_data = [[
            date, source, name, inc_type, location, fuels, acres, lat_lon, geometry
        ]]
        wildCAD_df = wildCAD_df.append(gpd.GeoDataFrame(fire_data, columns=[
            'Date', 'Source', "Name", 'Inc-type', 'Location', 'Fuels', 'Acres',
            'Lat_lon', 'geometry'
        ]))

wildCAD_df = wildCAD_df.dropna()

wildCAD_df.to_csv('/var/www/caiso/wildCAD_fires.csv')

# filter by incidents within the last 2 days
today = dt.datetime.today()
week_ago = today - dt.timedelta(days=2)

wildCAD_df['Date'] = wildCAD_df['Date'].apply(lambda x: dt.datetime.strptime(x, '%m/%d/%Y %H:%M'))

fire_gdf = wildCAD_df[wildCAD_df['Date'] > week_ago]
fire_gdf.to_file('/var/www/caiso/wildCAD_fires.geojson', driver='GeoJSON')

gis = GIS("https://www.arcgis.com", "username", "password")
incidents = gis.content.search(query="title:wildCAD_fires")
try:
    wildcad_incidents_flc = incidents[1]
    flayer_collection = FeatureLayerCollection.fromitem(wildcad_incidents_flc)
    flayer_collection.manager.overwrite('/var/www/caiso/wildCAD_fires.geojson')
except (IndexError, TypeError):
    wildcad_incidents_flc = incidents[0]
    flayer_collection = FeatureLayerCollection.fromitem(wildcad_incidents_flc)
    flayer_collection.manager.overwrite('/var/www/caiso/wildCAD_fires.geojson')

# import californie t-lines. This portion of the script was for another project I was working on that will display the distance of the fires to the nearest transmission line.
gdf = gpd.read_file('/var/www/caiso/CAISO_tlines')

num_fires = fire_gdf.shape[0]
count = 0
for x in range(num_fires):
    fire_name = fire_gdf.Name.iloc[x]
    fire_point = fire_gdf.geometry.iloc[x]
    distance_list = []
    count += 1
    print('Checking: {} Number: {}'.format(fire_name, count))
    clear_output(wait=True)
    for index, row in gdf.iterrows():
        line = row['geometry']
        nearest_line_point = line.interpolate(line.project(fire_point))
        fire_distance = geopy.distance.distance((fire_point.y, fire_point.x), (nearest_line_point.y, nearest_line_point.x)).miles
        distance_list.append(fire_distance)
    gdf[fire_name] = distance_list

fire_gdf['close_lines'] = ''
fire_gdf['close_lines_distance'] = ''

for x in range(num_fires):
    close_lines = []
    distance_list = []
    fire_name = fire_gdf.Name.iloc[x]
    gdf = gdf.sort_values(by=fire_name, ascending=True)
    distance = gdf.loc[gdf.index[0], fire_name]
    if distance <= 10:
        line_name = gdf.loc[gdf.index[0], 'SUB_1'] + '-' + gdf.loc[gdf.index[0], 'SUB_2'] + ' ' + str(gdf.loc[gdf.index[0], 'VOLTAGE'])[:-2] + 'kV line'
        close_lines.append(line_name)
        distance_list.append(round(distance, 2))
    else:
        close_lines.append("")
        distance_list.append("")
    fire_gdf['close_lines'][x] = close_lines
    fire_gdf['close_lines_distance'][x] = distance_list

fire_gdf.to_file('/var/www/caiso/current_wildcad_near_lines.geojson', driver='GeoJSON')
fire_gdf.to_csv('/var/www/caiso/current_wildcad_near_lines.csv')

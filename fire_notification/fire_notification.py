from arcgis.features import FeatureLayerCollection
from arcgis.gis import GIS
from shapely.ops import nearest_points
import pandas as pd
import geopandas as gpd
import datetime as dt
import geopy.distance
import smtplib

# silence chained assignment error default='warn'
pd.options.mode.chained_assignment = None

distance_threshold = 2 

irwin_gdf = gpd.read_file('https://services9.arcgis.com/RHVPKKiFTONKtxq3/arcgis/rest/services/USA_Wildfires_v1/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson')

# cols = ['IncidentName', 'FireDiscoveryDateTime', 'geometry', ]
irwin_gdf['FireDiscoveryDateTime'] = pd.to_datetime(irwin_gdf['FireDiscoveryDateTime'], unit='ms')
irwin_gdf['ModifiedOnDateTime'] = pd.to_datetime(irwin_gdf['ModifiedOnDateTime'], unit='ms')
irwin_gdf = irwin_gdf.sort_values(by='FireDiscoveryDateTime', ascending=False)
irwin_gdf = irwin_gdf[irwin_gdf.IncidentTypeCategory == 'WF']

posted_fires_gdf = gpd.read_file('/var/www/caiso/bpa_fire_threats.geojson')
posted_fire_names = posted_fires_gdf.IncidentName.tolist()
active_irwin_names = irwin_gdf.IncidentName.tolist()

# import bpa gen-drop t-lines
bpa_lines_gdf = gpd.read_file('/var/www/caiso/bpa_gendrop_lines.geojson')
# grab column names to reset later on
bpa_lines_gdf_columns = bpa_lines_gdf.columns.tolist()

current_time = dt.datetime.now().strftime('%D %T')
most_recent_fire_update = list(irwin_gdf['FireDiscoveryDateTime'])[0]
bpa_lines_gdf['LAST_UPDATE'] = pd.to_datetime(bpa_lines_gdf['LAST_UPDATE'])
last_update = bpa_lines_gdf['LAST_UPDATE'][0]

def arcgis_update():
    cs = {}
    with open('/var/www/caiso/clown.txt', 'r') as f2:
        for line in f2:
            u, p = line.strip().split(':')
            cs[u] = p
    u = list(cs.keys())[0]
    p = cs[u]
    f2.close()

    gis = GIS('https://www.arcgis.com', u, p)
    incidents = gis.content.search(query='title:bpa_fire_threats')

    try:
       incidents_flc = incidents[0]
       flayer_collection = FeatureLayerCollection.fromitem(incidents_flc)
    except(TypeError):
       incidents_flc = incidents[1]
       flayer_collection = FeatureLayerCollection.fromitem(incidents_flc)
    flayer_collection.manager.overwrite('/var/www/caiso/bpa_fire_threats.geojson')
    print("ArcGIS Updated {}".format(current_time))

concerning_fires_list = []
if most_recent_fire_update > last_update:
    # output will return the gen-drop lines dataframe with columns added for each
    # fire. Column data will reflect the distance
    # of the fire to the nerest point on each transmission line.
    new_fires_gdf = irwin_gdf[irwin_gdf['FireDiscoveryDateTime'] > last_update]
    new_fires_gdf = new_fires_gdf.reset_index(drop=True)
    num_fires = new_fires_gdf.shape[0]
    for x in range(num_fires):
        fire_name = new_fires_gdf.IncidentName.iloc[x]
        fire_point = new_fires_gdf.geometry.iloc[x]
        new_fires_gdf.at[x, 'InitialLatitude'] = fire_point.y
        new_fires_gdf.at[x, 'InitialLongitude'] = fire_point.x
        distance_list = []
        for index, row in bpa_lines_gdf.iterrows():
            line = row['geometry']
            nearest_line_point = line.interpolate(line.project(fire_point))
            fire_distance = geopy.distance.distance((fire_point.y, fire_point.x), (nearest_line_point.y, nearest_line_point.x)).miles
            distance_list.append(fire_distance)
        bpa_lines_gdf[fire_name] = distance_list

    bpa_lines_gdf.to_csv('/var/www/caiso/bpa_lines_fires.csv')

    with open('/var/www/caiso/fire_report.txt', 'w') as f:
        print('', file=f)
        print('----FIRE ALERT----', file=f)
        print('', file=f)
        for x in range(num_fires):
            close_lines = []
            close_lines_id = []
            distance_list = []
            fire_name = new_fires_gdf.IncidentName.iloc[x]
            fire_county = new_fires_gdf.POOCounty.iloc[x]
            fire_state = new_fires_gdf.POOState.iloc[x]
            fire_longitude = new_fires_gdf.InitialLongitude.iloc[x]
            fire_latitude = new_fires_gdf.InitialLatitude.iloc[x]
            bpa_lines_gdf = bpa_lines_gdf.sort_values(by=fire_name, ascending=True)
            concerning_fires_gdf = bpa_lines_gdf[bpa_lines_gdf[fire_name] <= distance_threshold]
            if concerning_fires_gdf.shape[0] != 0:
                concerning_fires_list.append(fire_name)
                print('{} FIRE:'.format(fire_name), file=f)
                print('{} County, {}'.format(fire_county, fire_state[-2:]), file=f)
                print('{}, {}'.format(str(round(fire_longitude, 3)), str(round(fire_latitude, 3))), file=f)
                for index, row in concerning_fires_gdf.iterrows():
                    if row['PORTION_OF'] != 'x':
                        line_name = row['SUB_1'] + '-' + row['SUB_2'] + ' portion of ' + row['PORTION_OF']
                        line_id = row['ID']
                        rounded_distance = round(row[fire_name], 2)
                        # irwin_gdf.at[x, 'close_lines'] = line_name
                        print('    -{} miles from {}'.format(rounded_distance, line_name), file=f)
                    else:
                        line_name = row['SUB_1'] + '-' + row['SUB_2'] + ' ' + str(row['VOLTAGE']) + 'kV line'
                        line_id = row['ID']
                        rounded_distance = round(row[fire_name], 2)
                        # irwin_gdf.at[x, 'close_lines'] = line_name
                        print('    -{} miles from {}'.format(rounded_distance, line_name), file=f)
    f.close()

if len(concerning_fires_list) > 0:
    # Update bpa layer for ArcGIS Dashboard
    irwin_gdf = irwin_gdf.reset_index(drop=True)
    irwin_gdf['threatened_lines'] = ''
    num_fires = irwin_gdf.shape[0]
    for x in range(num_fires):
        fire_name = irwin_gdf.IncidentName.iloc[x]
        fire_point = irwin_gdf.geometry.iloc[x]
        threatened_lines_list = []
        distance_list = []
        for index, row in bpa_lines_gdf.iterrows():
            line = row['geometry']
            nearest_line_point = line.interpolate(line.project(fire_point))
            fire_distance = geopy.distance.distance((fire_point.y, fire_point.x), (nearest_line_point.y, nearest_line_point.x)).miles
            if fire_distance < distance_threshold:
                threatened_lines = str(round(fire_distance, 2)) + " miles from " + row['SUB_1'] + '-' + row['SUB_2']
                threatened_lines_list.append(threatened_lines)
                distance_list.append(fire_distance)
        irwin_gdf.at[x, 'threatened_lines'] = '/'.join(threatened_lines_list)

    concerning_fires_gdf = irwin_gdf[irwin_gdf['threatened_lines'] != '']

    concerning_fires_gdf.to_file('/var/www/caiso/bpa_fire_threats.geojson', driver='GeoJSON')

    arcgis_update()

    # Send email with new fire
    bpa_lines_gdf_raw = bpa_lines_gdf[bpa_lines_gdf_columns]
    bpa_lines_gdf_raw['LAST_UPDATE'] = most_recent_fire_update
    bpa_lines_gdf_raw.to_file('/var/www/caiso/bpa_gendrop_lines.geojson', driver='GeoJSON')
    with open('/var/www/caiso/fire_report.txt', 'r') as myfile:
        data = myfile.read()
    print(data)

    receivers = ['jbohlen@caiso.com', 'justin.bohlen@yahoo.com', ]
    smtpObj = smtplib.SMTP_SSL('smtp.mail.yahoo.com', 465)
    smtpObj.ehlo()
    smtpObj.login('justin.bohlen@yahoo.com', 'sqtmgiwmixlkcrhk')
    smtpObj.sendmail('justin.bohlen@yahoo.com', receivers, "Subject: BPA FIRE ALERT" + data)
    smtpObj.quit()
# If files aren't updated due to new fires, check if any existing fires have been removed.
elif not set(posted_fire_names).issubset(active_irwin_names):
    fires_to_remove_list = []
    for fire in posted_fire_names:
        if fire not in active_irwin_names:
            fires_to_remove_list.append(fire)
    updated_posted_fires_gdf = posted_fires_gdf[~posted_fires_gdf.IncidentName.isin(fires_to_remove_list)]
    try:
        updated_posted_fires_gdf.to_file('/var/www/caiso/bpa_fire_threats.geojson', driver='GeoJSON')
    except ValueError:
        pass

    arcgis_update()

else:
    print("{}: No new fires within {} miles of BPA lines".format(current_time, str(distance_threshold)))


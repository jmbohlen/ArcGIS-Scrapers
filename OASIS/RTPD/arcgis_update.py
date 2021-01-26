import geopandas as gpd
import pandas as pd
import datetime as dt
from dateutil import tz
import os
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
from shapely.geometry import Point

RTPD_data_df = pd.read_csv('/var/www/caiso/RTPD_Prices.csv', index_col=0)

# Read the file that contains the node coordinates saved from a previous scrape of the CAISO public price map
gdf = pd.read_csv('/var/www/caiso/node_locs.csv')
gdf = gdf[['node_ba', 'node_name', 'latitude', 'longitude']]

geo_node_list = []
for x in gdf['node_name']:
    geo_node_list.append(x)

interval_columns = []
interval_columns.append('node')
interval_columns.append('interval')

from_zone = tz.gettz('UTC')
to_zone = tz.gettz('America/Los_Angeles')

utc = dt.datetime.utcnow()
utc = utc.replace(tzinfo=from_zone)
pacific_hour = utc.astimezone(to_zone).strftime('%H')
minute = int(dt.datetime.utcnow().strftime("%M"))

# Identify the current time to determine which interval to post to ArcGIS
if 0 <= minute < 15:
    interval_columns.append('p1_level')
    RTPD_data_df['interval'] = str(int(pacific_hour)) + ':00' + ' - ' + str(int(pacific_hour)) + ':15'
    for x in RTPD_data_df.columns:
        if x.startswith("RTPD1") and not x.endswith("int_end"):
            interval_columns.append(x)
if 15 <= minute < 30:
    interval_columns.append('p2_level')
    RTPD_data_df['interval'] = str(int(pacific_hour)) + ':15' + ' - ' + str(int(pacific_hour)) + ':30'
    for x in RTPD_data_df.columns:
        if x.startswith("RTPD2") and not x.endswith("int_end"):
            interval_columns.append(x)
if 30 <= minute < 45:
    interval_columns.append('p3_level')
    RTPD_data_df['interval'] = str(int(pacific_hour)) + ':30' + ' - ' + str(int(pacific_hour)) + ':45'
    for x in RTPD_data_df.columns:
        if x.startswith("RTPD3") and not x.endswith("int_end"):
            interval_columns.append(x)
if 45 <= minute <= 59:
    interval_columns.append('p4_level')
    RTPD_data_df['interval'] = str(int(pacific_hour)) + ':45' + ' - ' + str(int(pacific_hour) + 1) + ':00'
    for x in RTPD_data_df.columns:
        if x.startswith("RTPD4") and not x.endswith("int_end"):
            interval_columns.append(x)

RTPD_data_df = RTPD_data_df[interval_columns]

RTPD_data_df.columns = ['Node', 'Interval', 'Price Level', 'LMP', 'Energy', 'Congestion', 'Losses']

# Filter the nodes by those that have coordinates that were previously scraped from the public CAISO price map.
RTPD_data_df = RTPD_data_df[RTPD_data_df['Node'].isin(geo_node_list)]
combined = pd.merge(RTPD_data_df, gdf, how='left', left_on='Node', right_on='node_name')
combined = combined.drop('node_name', axis=1)

# Some node coordinates are bad and skew the mapping of the good nodes. I need to filter the node_locs.csv file instead of doing this each time I update the file on ArcGIS.
list1 = []
list2 = []
for index, row in combined.iterrows():
    if (row['longitude'] > -50) or (row['latitude'] < 30):
        list1.append(float('NaN'))
        list2.append(float('NaN'))
    else:
        list1.append(row['longitude'])
        list2.append(row['latitude'])
combined['longitude'] = list1
combined['latitude'] = list2

combined['geometry'] = [Point(xy) for xy in zip(combined.longitude, combined.latitude)]
gdf = gpd.GeoDataFrame(combined, geometry='geometry')

# There is a bug in Fiona (used by GeoPandas) that does not allow a GeoJSON file to be overwritten. Here we attempt to delete the old file before saving the new. https://github.com/geopandas/geopandas/issues/367
try:
    os.remove('var/www/caiso/node_data.json')
except OSError:
    pass
gdf.to_file('/var/www/caiso/node_data.json', driver='GeoJSON')

cs = {}
with open('/var/www/caiso/clown.txt', 'r') as f:
    for line in f:
        u, p = line.strip().split(':')
        cs[u] = p
u = list(cs.keys())[0]
p = cs[u]

gis = GIS("https://www.arcgis.com", "username", "password")
node_locs = gis.content.search(query="title:RTPD_Prices")


# Sometimes the GeoJSON file is returned first instead of the featurelayer. This checks to make sure we are updating the featurelayer.
try:
    node_locs_flc = node_locs[0]
    price_flayer_collection = FeatureLayerCollection.fromitem(node_locs_flc)
except TypeError:
    node_locs_flc = node_locs[1]
    price_flayer_collection = FeatureLayerCollection.fromitem(node_locs_flc)

price_flayer_collection.manager.overwrite('/var/www/caiso/node_data.json')

date = str(dt.datetime.utcnow().strftime('%m/%d/%Y'))
print(date + ' - ' + str(pacific_hour) + ":" + str(minute) + " - successfully posted to arcgis")

import io
import os
import zipfile
import requests
import pandas as pd
import geopandas as gpd
import datetime as dt
import xml.etree.ElementTree as ET
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
from shapely.geometry import Point

date1 = (dt.datetime.utcnow().strftime("%Y%m%d"))
time1 = (dt.datetime.utcnow().strftime("%H:%M"))

RTDurl = 'http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_CURR_LMP&node=ALL&startdatetime=' + date1 + 'T' + time1 + '-0000&enddatetime=' + date1 + 'T' + time1 + '-0000&version=1'
r = requests.get(RTDurl)
z = zipfile.ZipFile(io.BytesIO(r.content))
z.extractall()

RTD_int_end_list = []
RTD_energy_list = []
RTD_lmp_list = []
RTD_losses_list = []
RTD_cong_list = []
RTD_green_list = []
RTD_lmp_node_list = []
RTD_energy_node_list = []
RTD_losses_node_list = []
RTD_congestion_node_list = []
RTD_green_node_list = []

file_name = z.namelist()
tree = ET.parse(file_name[0])
root = tree.getroot()
node_data = root[1][0]
for node in node_data[1:-1]:
    data_item = node[1][0].text
    if data_item == 'LMP_PRC':
        RTD_lmp_node_list.append(node[1][1].text)
        interval_end = node[1][5].text
        RTD_int_end_list.append(interval_end[:-6].replace('T', ' '))
        RTD_lmp_list.append(node[1][6].text)
    if data_item == 'LMP_CONG_PRC':
        RTD_congestion_node_list.append(node[1][1].text)
        RTD_cong_list.append(node[1][6].text)
    if data_item == 'LMP_ENE_PRC':
        RTD_energy_node_list.append(node[1][1].text)
        RTD_energy_list.append(node[1][6].text)
    if data_item == 'LMP_LOSS_PRC':
        RTD_losses_node_list.append(node[1][1].text)
        RTD_losses_list.append(node[1][6].text)

lmp_df = pd.DataFrame({'node': RTD_lmp_node_list,
                       'Interval_end': RTD_int_end_list,
                       'RTD_lmp': RTD_lmp_list
                       })
energy_df = pd.DataFrame({'node': RTD_energy_node_list,
                          'RTD_energy': RTD_energy_list
                          })
cong_df = pd.DataFrame({'node': RTD_congestion_node_list,
                        'RTD_congestion': RTD_cong_list
                        })
loss_df = pd.DataFrame({'node': RTD_losses_node_list,
                        'RTD_losses': RTD_losses_list
                        })

RTD_combined = pd.merge(lmp_df, energy_df, how='left', left_on='node', right_on='node')
RTD_combined = pd.merge(RTD_combined, cong_df, how='left', left_on='node', right_on='node')
RTD_combined = pd.merge(RTD_combined, loss_df, how='left', left_on='node', right_on='node')

# I manually define price levels to match the color gradient shown on the public price map. ArcGIS won't divide the prices equally so I create this price level and assign it a specific color within ArcGIS.
p_level = []
for index, row in RTD_combined.iterrows():
    p = row['RTD_lmp']
    if float(p) <= -50:
        p_level.append(1)
    elif -50 < float(p) <= 0:
        p_level.append(2)
    elif 0 < float(p) <= 50:
        p_level.append(3)
    elif 50 < float(p) <= 55:
        p_level.append(4)
    elif 55 < float(p) <= 60:
        p_level.append(5)
    elif 60 < float(p) <= 65:
        p_level.append(6)
    elif 65 < float(p) <= 70:
        p_level.append(7)
    elif 70 < float(p) <= 75:
        p_level.append(8)
    elif 75 < float(p) <= 80:
        p_level.append(9)
    elif 80 < float(p) <= 85:
        p_level.append(10)
    elif 85 < float(p) <= 90:
        p_level.append(11)
    elif 90 < float(p) <= 95:
        p_level.append(12)
    elif 95 < float(p) <= 100:
        p_level.append(13)
    elif 100 < float(p) <= 125:
        p_level.append(14)
    elif 125 < float(p) <= 150:
        p_level.append(15)
    elif 150 < float(p) <= 200:
        p_level.append(16)
    elif 200 < float(p):
        p_level.append(17)
    else:
        p_level.append(float("NaN"))
RTD_combined['p_level'] = p_level

gdf = pd.read_csv('/var/www/caiso/node_locs.csv')
gdf = gdf[['node_ba', 'node_name', 'latitude', 'longitude']]
geo_node_list = []
for x in gdf['node_name']:
    geo_node_list.append(x)

RTD_data_df = RTD_combined[RTD_combined['node'].isin(geo_node_list)]
combined = pd.merge(RTD_data_df, gdf, how='left', left_on='node', right_on='node_name')
combined = combined.drop('node_name', axis=1)

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
current_gdf = gpd.GeoDataFrame(combined, geometry='geometry')
current_gdf.to_file('/var/www/caiso/RTD_data.json', driver='GeoJSON')

# Connect to ArcGIS API and search for the calfire feature layer collection to be updated. https://developers.arcgis.com/python/guide/accessing-and-creating-content/
gis = GIS("https://www.arcgis.com", 'username', 'password')
past_node_locs = gis.content.search(query="title:RTD prices")
past_node_locs_flc = past_node_locs[0]
past_price_flayer_collection = FeatureLayerCollection.fromitem(past_node_locs_flc)
past_price_flayer_collection.manager.overwrite('/var/www/caiso/RTD_data.json')
# Remove the .xml file that was saved after accessing OASIS API
try:
    os.remove(file_name[0])
except OSError as e:
    print(e)

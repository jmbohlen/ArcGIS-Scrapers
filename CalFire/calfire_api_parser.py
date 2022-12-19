import geopandas as gpd
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection

calfire_url = 'https://www.fire.ca.gov/umbraco/api/IncidentApi/GeoJsonList?inactive=true'

gdf = gpd.read_file(calfire_url)

# gdf.to_file('calfire_active_incidents_api.json', driver='GeoJSON')

gdf.to_csv('calfire_incidents_api.csv', index=False, mode='w')

cs = {}
with open('clown.txt', 'r') as f:
    for line in f:
        u, p = line.strip().split(':')
        cs[u] = p
u = list(cs.keys())[0]
p = cs[u]

gis = GIS('https://www.arcgis.com', u, p)
incidents = gis.content.search(query='title:calfire_incidents_api')
try:
    incidents_flc = incidents[0]
    flayer_collection = FeatureLayerCollection.fromitem(incidents_flc)
except(TypeError):
    incidents_flc = incidents[1]
    flayer_collection = FeatureLayerCollection.fromitem(incidents_flc)
flayer_collection.manager.overwrite('calfire_incidents_api.csv')

import geopandas as gpd
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection

calfire_url = 'https://www.fire.ca.gov/umbraco/api/IncidentApi/GeoJsonList?inactive=true'

# Read calfire API URL into a GeoPandas GeoDataFrame
gdf = gpd.read_file(calfire_url)

active_fires_gdf = gdf[gdf.IsActive == True]

# Save a copy of the GDF as a GeoJSON file
active_fires_gdf.to_file('/var/www/caiso/calfire_active_incidents_api.geojson', driver='GeoJSON')

# # Login info stored in a text file to obfuscate from the script itself
# cs = {}
# with open('/var/www/caiso/clown.txt', 'r') as f:
#     for line in f:
#         u, p = line.strip().split(':')
#         cs[u] = p
# u = list(cs.keys())[0]
# p = cs[u]

# Connect to ArcGIS API and search for the calfire feature layer collection to be updated. https://developers.arcgis.com/python/guide/accessing-and-creating-content/
gis = GIS('https://www.arcgis.com', 'username', 'password')
incidents = gis.content.search(query='title:calfire_incidents_api')
incidents_flc = incidents[0]
flayer_collection = FeatureLayerCollection.fromitem(incidents_flc)
flayer_collection.manager.overwrite('/var/www/caiso/calfire_active_incidents_api.geojson')

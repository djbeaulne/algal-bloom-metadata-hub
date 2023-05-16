# python3 /Users/danielle/Work/AlgalBlooms/AlgalBloomWebApp/scripts/radarsat2.py


###
### SCRIPT DESCRIPTION ###
### 

# AUTHOR: Danielle Beaulne
# DATE CREATED: 9 May 2022
# LAST MODIFIED: 13 May 2023

### SHORT DESCRPITION ###
# This script works with RADARSAT-2 metadata
#   - downloads the metadata for RADARSAT-2 files from EODMSRAPI
#       - scope of download is determined by parameters specified by user
#   - reduces metadata to information relevant for understanding algal blooms
#   - outputs shapefile compatible with ArcGIS Online

### INTPUTS/OUTPUTS ###
# Inputs: 
#   - EODMSRAPI login information
#   - start/end date of interest
#   - region of interest (e.g. shapefile, geojson)    #### NOT YET
#   - output filename (shapefile)

# Outputs: 
#   - shapefile for RADARSAT-2 metadata, compatible with ArcGIS Online

###########################################################################

import fiona
import pandas as pd
import geopandas as gpd

from eodms_rapi import EODMSRAPI
from datetime import datetime
from shapely import wkt



###
### USER INPUT ###
###

# EODMSRAPI login information 
username = 'DJBEAULNE'
password = 'MadW0rld!'

# time period of interest
date_start = '20210101_000000'  # yyyymmdd_hhmmss
date_end = '20211231_235959'    # yyyymmdd_hhmmss

# region of interest
#   operator can be: contains, contained by, crosses, disjoint with, intersects, overlaps, touches, or within
#   geometry can be: ESRI Shapefile, KML, GML or GeoJSON
roi_shortname = ''   # used in the output filename 
roi = [('intersects', "Polygon ((-95.155703 41.68132, -74.343472 41.68132, -74.343472 56.861705, -95.155703 56.861705, -95.155703 41.68132))")]

# output filename 
outfile = '/Users/danielle/Work/AlgalBlooms/AlgalBloomWebApp/data/data_github_test/RADARSAT-2_metadata_' + date_start[0:8] + '_' + date_end[0:8] + '_' + roi_shortname + '.shp'

# parameters of interest
parameters_pass = { 'recordId', 'collectionId', 'incidenceAngle', 'beam', 'segmentQuality', 'absoluteOrbit', 'polarization', 'transmitPolarization', 'spatialResolution', 'startDate', 'position', 'sensorMode', 'featureId', 'lookOrientation', 'orbitDirection', 'title', 'thisRecordUrl', 'geometry', 'wktGeometry' }

# specify product of interest
product = 'Radarsat2RawProducts' ### NEED TO UPDATE QUERY LINE 85



###
### SCRIPT START ###
###


###
### DOWNLOAD THE METADATA FROM EODMSRAPI ###
###

# login to EODMSRAPI
rapi = EODMSRAPI(username, password)
print('\n\nLogged in successfully')

# set up query parameters
dates = [{"start": date_start, "end": date_end}]

# submit the search to the EODMSRAPI, specifying the Collection (?)
print('Querying data... \n')
rapi.search("Radarsat2RawProducts", features=roi, dates=dates)

# retrieve the results from the search
res = rapi.get_results('full')

### SOME OPTIONAL QUALITY ANALYSIS 
# check to see is all the keys are the same
product_keys = [res[footprint].keys() for footprint in range(len(res))]
product_keys_check = set([res[0].keys() == res[footprint].keys() for footprint in range(len(res))])

if False in product_keys_check:
    print('\nERROR: Product keys are inconsistent between products \n\n')
else:
    print('\nGOOD TO GO: Product keys are consistent between products :) \n\n')



###
### FILTER/SUBSET THE METADATA TO INFORMATION RELEVANT TO ALGAL BLOOMS TO TRIM AND SAVE SPACE ###
###

# filter the data to parameters of interest
products = [{k1: v1 for k1, v1 in res[j].items() if k1 in parameters_pass} for j in range(len(res))]

# numerical data are stored as strings; convert to numerical 
# add a date column comptible with shapefile 
radarsat_2_datetime_fmt = '%Y-%m-%dT%H:%M:%S'
for i in range(len(products)): # CAN I GET RID OF THIS FOR LOOP?
    products[i]['startDate'] = products[i]['startDate'][:-6] # need to strip the last few digits ' +0000' - it's all the same anyways 
    products[i]['starttime'] = datetime.strptime(products[i]['startDate'], radarsat_2_datetime_fmt)
    products[i]['starttime'] = products[i]['starttime'].strftime('%Y-%m-%d %H:%M:%S.%f')
    
    products[i]['recordId'] = int(products[i]['recordId'])
    products[i]['incidenceAngle'] = int(products[i]['incidenceAngle'])
    products[i]['absoluteOrbit'] = float(products[i]['absoluteOrbit'])
    products[i]['spatialResolution'] = int(products[i]['spatialResolution'])


# save as shapefile 
#   converting through dict > pandas dataframe > geopandas geodataframe # IS THIS STILL TRUE? 
#   create a geometry column to convert to geodataframe 
df = pd.DataFrame(products)
df['geometry'] = gpd.GeoSeries.from_wkt(df['wktGeometry'])
gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')


# write data to shapefile
gdf.to_file(outfile)





    
    
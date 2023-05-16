# python3 /Users/danielle/Work/AlgalBlooms/AlgalBloomWebApp/scripts/Sentinel3_OLCI.py


###
### SCRIPT DESCRIPTION ###
### 

# AUTHOR: Danielle Beaulne
# LAST MODIFIED: 13 May 2023

### SHORT DESCRPITION ###
# This script works with SENTINEL-3 OLCI data
#   - downloads the metadata for Sentinel-3 files from Copernicus API Hub
#       - following time period of interest & region of interest as specified by user
#   - reduces metadata to information relevant for understanding algal blooms
#   - outputs shapefile compatible with ArcGIS Online

### INTPUTS/OUTPUTS ###
# Inputs: 
#   - copernicus login information  *** requires username & password for registered user
#   - start/end date of interest
#   - region of interest (e.g. shapefile, geojson)
#   - output filename (shapefile)

# Outputs: 
#   - shapefile for Sentinel-3 OLCI data, compatible with ArcGIS Online

###############################################################################################

import json
import datetime 
import collections
import os
import math
import pandas as pd
import geopandas as gpd

from timeit import default_timer as timer
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
from datetime import date
from collections import OrderedDict
from sys import argv
from os.path import exists
from timeit import default_timer as timer
from datetime import timedelta
from shapely import wkt
from datetime import datetime
from datetime import date

def defaultconverter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()

def convert_size(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])



###
### USER INPUT ###
###

# Copernicus login information
username = 'danielle.beaulne'
password = 'MadW0rld'
url = 'https://apihub.copernicus.eu/apihub'

#  time period of interest
start_date = '20210101' # format: YYYYMMDD; 1 Jan 2021
end_date = '20211231' # format: YYYYMMDD; 31 Dec 2021

# region of interest
roi_shortname = 'ontario'
roi = '/Users/danielle/Work/AlgalBlooms/ontario_extent.geojson' 

# output file/folder information
outfile = '/Users/danielle/Work/AlgalBlooms/AlgalBloomWebApp/data/data_github_test/S3_metadata_' + start_date + '_' + end_date + '_' + roi_shortname + '.shp'

# parameters of interest; different parameters for Level-1 and Level-2 data
parameters_pass_L1 = {  'filename', 'link', 'link_icon', 'summary', 'ondemand', 'beginposition', 'endposition', 'creationdate', 'ingestiondate', 'orbitnumber', 'relativeorbitnumber', 'ecmwf', 'productlevel', 'platformname', 'platformidentifier', 'instrumentshortname', 'producttype', 'timeliness', 'size', 'orbitdirection', 'relpassnumber', 'passnumber', 'processinglevel', 'procfacilityname', 'procfacilityorg', 'onlinequalitycheck', 'identifier', 'uuid', 'footprint' }

parameters_pass_L2 = {  'filename', 'link', 'link_icon', 'summary', 'ondemand', 'beginposition', 'endposition', 'creationdate', 'ingestiondate', 'orbitnumber', 'relativeorbitnumber', 'cloudcoverpercentage', 'ecmwf', 'productlevel', 'platformname', 'platformidentifier', 'instrumentshortname', 'producttype', 'timeliness', 'size', 'orbitdirection', 'relpassnumber', 'passnumber', 'processinglevel', 'procfacilityname', 'procfacilityorg', 'onlinequalitycheck', 'identifier', 'uuid', 'footprint' }

# specify platform and instrumentshortname; needs to be supported by Copernicus API hub
platformname = 'Sentinel-3'
instrumentshortname = 'OLCI'



###
### SCRIPT START ###
###


###
### DOWNLOAD THE METADATA FROM COPERNICUS API HUB ###
###

# login to Copernicus
api = SentinelAPI(username, password, url)
print('\nSentinel API login successful \n')

# query products; gets some metadata information for each file
footprint = geojson_to_wkt(read_geojson(roi))
print('\n\nDownloading metadata for: ', platformname, ' ', instrumentshortname, ': ', start_date, '-', end_date, ' for ', roi_shortname)
start = timer()
products = api.query(footprint, date=(start_date, end_date), platformname=platformname, instrumentshortname=instrumentshortname)



###
### FILTER/SUBSET THE METADATA TO INFORMATION RELEVANT TO ALGAL BLOOMS TO TRIM AND SAVE SPACE ###
###

# split the data into groups that have the same keys 
products_L1 = [products[footprint] for footprint in products if products[footprint]['productlevel'] == 'L1']
products_L2 = [products[footprint] for footprint in products if products[footprint]['productlevel'] == 'L2']

# extract a subset of keys from these sub-groups 
products_L1_subset = [{k1: v1 for k1, v1 in products_L1[j].items() if k1 in parameters_pass_L1} for j in range(len(products_L1))]
products_L2_subset = [{k1: v1 for k1, v1 in products_L2[j].items() if k1 in parameters_pass_L2} for j in range(len(products_L2))]

# re-merge these subgroups
products_subset = products_L1_subset + products_L2_subset

print('... subsetted products successfully ...')



###
### OUTPUT DATA ###
###

# save as shapefile 
#   converting through dict > pandas dataframe > geopandas geodataframe
#   create a geometry column to convert to geodataframe 
products_subset_df = pd.DataFrame(products_subset)
products_subset_df['footprint'] = gpd.GeoSeries.from_wkt(products_subset_df['footprint'])
products_subset_gdf = gpd.GeoDataFrame(products_subset_df, geometry=products_subset_df['footprint'], crs='EPSG:4326')

# drop the temporary footprint column... 
products_subset_gdf = products_subset_gdf.drop(['footprint'], axis=1)

# convert columns with datetime format (not supported to conversion to shapefile)
datetime_format = '%Y-%m-%d %H:%M:%S.%f'
products_subset_gdf['beginposition'] = products_subset_gdf['beginposition'].dt.strftime(datetime_format)
products_subset_gdf['endposition'] = products_subset_gdf['endposition'].dt.strftime(datetime_format)
products_subset_gdf['ingestiondate'] = products_subset_gdf['ingestiondate'].dt.strftime(datetime_format)
products_subset_gdf['creationdate'] = products_subset_gdf['creationdate'].dt.strftime(datetime_format)

print('... conversion complete ...')


### output data 
products_subset_gdf.to_file(outfile)

print('... data output!')



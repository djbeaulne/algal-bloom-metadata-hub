# python3 /Users/danielle/Work/AlgalBlooms/AlgalBloomWebApp/scripts_s1sar/data_download_sentinelsat_query_sentinel1sar_2021.py


###
### SCRIPT DESCRIPTION ###
### 

# AUTHOR: Danielle Beaulne
# LAST MODIFIED: 22 June 2022

# modified from script to download Sentinel-2 MSI data: /Users/danielle/Work/AlgalBlooms/data_download_sentinelsat_query.py

### SHORT DESCRPITION ###
# This script works with SENTINEL-1 SAR data
#   - downloads the metadata for Sentinel-1 files
#   - downloads data as determined by parameters set by user
#   - converts data into geojson files (readable by ArcGIS Online)
#   - outputs geojson file

### INTPUTS/OUTPUTS ###
# Inputs: 
#   - copernicus login information
#   - start/end date of interest
#   - region of interest (geojson) 
#   - output filename (geojson)

#########################


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



###
### USER INPUT ###
###

# Copernicus login information
username = 'danielle.beaulne'
password = 'MadW0rld'
url = 'https://apihub.copernicus.eu/apihub'

# output file/folder information
outdir = '/Users/danielle/Work/AlgalBlooms/AlgalBloomWebApp/data_s1sar_intermediate/'
outfilename_base = 'S1_metadata'
outfile_ext = '.shp'

# Region of interest
roi = '/Users/danielle/Work/AlgalBlooms/ontario_extent.geojson' 

# subset of parameters to keep in the final files
# not keeping all information to reduce file size
parameters_pass_L1_notWV = { 'filename', 'link', 'link_alternative', 'beginposition', 'endposition', 'ingestiondate', 'slicenumber', 'orbitnumber', 'relativeorbitnumber', 'sensoroperationalmode', 'swathidentifier', 'orbitdirection', 'producttype', 'platformname', 'platformidentifier', 'polarisationmode', 'status', 'size', 'gmlfootprint', 'uuid', 'footprint_geojson', 'footprint_format', 'footprint' }
parameters_pass_WV = { 'filename', 'link', 'link_alternative', 'beginposition', 'endposition', 'ingestiondate', 'orbitnumber', 'relativeorbitnumber', 'sensoroperationalmode', 'swathidentifier', 'orbitdirection', 'producttype', 'platformname', 'platformidentifier', 'polarisationmode', 'status', 'size', 'gmlfootprint', 'uuid', 'footprint_geojson', 'footprint_format', 'footprint'  }
parameters_pass_raw = { 'filename', 'link', 'link_alternative', 'beginposition', 'endposition', 'ingestiondate', 'slicenumber', 'orbitnumber', 'relativeorbitnumber', 'sensoroperationalmode', 'orbitdirection', 'producttype', 'platformname', 'platformidentifier', 'polarisationmode', 'status', 'size', 'gmlfootprint', 'uuid', 'footprint_geojson', 'footprint_format', 'footprint'  }

# Sentinel-1 identifying parameters
### TO SEPARATE DATA INTO WORKABLE CHUNKS ###
### these chunks also have all the same info (dictionary keys) ###
### I found out these codes while investigating the file types/file information ### 
# producttypes = ['SLC', 'OCN', 'RAW', 'GRD']    # found by investigation; will check in the script if they fit the data
# sensoroperationalmodes = ['SM', 'WV', 'EW', 'IW']    # found by investigation; will check in the script if they fit the data

#   only looking at 2019 bloom season 
start_date = '20210101' # format: YYYYMMDD; 1 Jan 2021
end_date = '20211231' # format: YYYYMMDD; 31 Dec 2021

outfilename = outfilename_base + '_' + start_date + '_' + end_date + '_ontario_' + str(date.today()) + outfile_ext
outfile = outdir + outfilename


###
### SCRIPT START ###
###

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


# login to Copernicus
api = SentinelAPI(username, password, url)
#print('\nSentinel API login successful \n')


# output geojson filename
print('\n\nDownloading metadata for: Sentinel 1 SAR-C: YEAR 2021\n')
start = timer()
#print(datetime.datetime.now())

# query products; gets some metadata information for each file
footprint = geojson_to_wkt(read_geojson(roi))
products = api.query(footprint, date=(start_date, end_date), platformname='Sentinel-1')


# extract subset of keys
# split the data into group that have the same keys 
products_L1_notWV = [products[footprint] for footprint in products if products[footprint]['sensoroperationalmode'] != 'WV' and products[footprint]['producttype'] != 'RAW']
products_WV = [products[footprint] for footprint in products if products[footprint]['sensoroperationalmode'] == 'WV']
products_raw = [products[footprint] for footprint in products if products[footprint]['producttype'] == 'RAW']

# extract a subset of keys from these sub-groups 
products_L1_notWV_subset = [{k1: v1 for k1, v1 in products_L1_notWV[j].items() if k1 in parameters_pass_L1_notWV} for j in range(len(products_L1_notWV))]
products_WV_subset = [{k1: v1 for k1, v1 in products_WV[j].items() if k1 in parameters_pass_WV} for j in range(len(products_WV))]
products_raw_subset = [{k1: v1 for k1, v1 in products_raw[j].items() if k1 in parameters_pass_raw} for j in range(len(products_raw))]

# re-merge these subgroups
products_subset = products_L1_notWV_subset + products_WV_subset + products_raw_subset


# save as shapefile directly instead of geojson
### converting through dict > pandas dataframe > geopandas geodataframe
### need to create a geometry column to convert to geodataframe 
products_subset_df = pd.DataFrame(products_subset)
products_subset_df['footprint'] = gpd.GeoSeries.from_wkt(products_subset_df['footprint'])
products_subset_gdf = gpd.GeoDataFrame(products_subset_df, geometry=products_subset_df['footprint'], crs='EPSG:4326')

# drop some unnecessary columns... 
products_subset_gdf = products_subset_gdf.drop(['footprint', 'gmlfootprint', 'link_alternative'], axis=1)

# convert columns with datetime format (not supported to conversion to shapefile)
datetime_format = '%Y-%m-%d %H:%M:%S.%f'
products_subset_gdf['beginposition'] = products_subset_gdf['beginposition'].dt.strftime(datetime_format)
products_subset_gdf['endposition'] = products_subset_gdf['endposition'].dt.strftime(datetime_format)
products_subset_gdf['ingestiondate'] = products_subset_gdf['ingestiondate'].dt.strftime(datetime_format)


### SAVE !!! 
products_subset_gdf.to_file(outfile)

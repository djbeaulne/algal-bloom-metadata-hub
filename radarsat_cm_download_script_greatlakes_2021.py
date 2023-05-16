# python3 /Users/danielle/Work/AlgalBlooms/AlgalBloomWebApp/radarsat_cm/radarsat_cm_download_script_greatlakes_2021.py


###
### SCRIPT DESCRIPTION ###
### 

# AUTHOR: Danielle Beaulne
# DATE CREATED: 10 May 2022
# LAST MODIFIED: 23 June 2022

# modified from script to download RADARSAT-2 SAR data: /Users/danielle/Work/AlgalBlooms/AlgalBloomWebApp/radarsat_2/radarsat_2_download_script.py

### SHORT DESCRPITION ###
# This script works with RADARSAT-CM data
#   - downloads the metadata for RADARSAT-CM files
#   - downloads data as determined by parameters set by user
#   - manipulates data within lists/dictionaries/pandas dataframe/geopandas geodataframe
#   - outputs shapefile file with date field compatible with ArcGIS Online; can be converted to datetime format in ArcGIS Online 

### INTPUTS/OUTPUTS ###
# Inputs: 
#   - copernicus login information
#   - start/end date of interest
#   - region of interest  
#   - output filename (shapefile)

# Outputs: 
#   - shapefile file with polygons & metadata for RADARSAT-CM SAR data (compatible with ArcGIS Online) 
#   - note: need to compress all constituent shapefiles into a .zip folder before importing into ArcGIS Online

### LIMITATIONS ### 
# FILE SIZE
# ArcGIS online can only handle files up to 10 MB in size

#########################



###
### USER INPUT ###
###

# info for querying scenes/metadata
username = 'DJBEAULNE'
password = 'MadW0rld!'

date_start_1 = '20220101_000000'  # yyyymmdd_hhmmss   ### JAN
date_end_1 = '20220131_235959'    # yyyymmdd_hhmmss
dates_1 = [{"start": date_start_1, "end": date_end_1}]

date_start_2 = '20210201_000000'  # yyyymmdd_hhmmss   ### FEB
date_end_2 = '20210228_235959'    # yyyymmdd_hhmmss
dates_2 = [{"start": date_start_2, "end": date_end_2}]

date_start_3 = '20210301_000000'  # yyyymmdd_hhmmss   ### MAR
date_end_3 = '20210331_235959'    # yyyymmdd_hhmmss
dates_3 = [{"start": date_start_3, "end": date_end_3}]

date_start_4 = '20210401_000000'  # yyyymmdd_hhmmss   ### APR
date_end_4 = '20210430_235959'    # yyyymmdd_hhmmss
dates_4 = [{"start": date_start_4, "end": date_end_4}]

date_start_5 = '20210501_000000'  # yyyymmdd_hhmmss   ### MAY
date_end_5 = '20210531_235959'    # yyyymmdd_hhmmss
dates_5 = [{"start": date_start_5, "end": date_end_5}]

date_start_6 = '20210601_000000'  # yyyymmdd_hhmmss   ### JUN
date_end_6 = '20210630_235959'    # yyyymmdd_hhmmss
dates_6 = [{"start": date_start_6, "end": date_end_6}]

date_start_7 = '20210701_000000'  # yyyymmdd_hhmmss   ### JUL
date_end_7 = '20210731_235959'    # yyyymmdd_hhmmss
dates_7 = [{"start": date_start_7, "end": date_end_7}]

date_start_8 = '20210801_000000'  # yyyymmdd_hhmmss   ### AUG
date_end_8 = '20210831_235959'    # yyyymmdd_hhmmss
dates_8 = [{"start": date_start_8, "end": date_end_8}]

date_start_9 = '20210901_000000'  # yyyymmdd_hhmmss   ### SEP
date_end_9 = '20210930_235959'    # yyyymmdd_hhmmss
dates_9 = [{"start": date_start_9, "end": date_end_9}]

date_start_10 = '20211001_000000'  # yyyymmdd_hhmmss   ### OCT
date_end_10 = '20211031_235959'    # yyyymmdd_hhmmss
dates_10 = [{"start": date_start_10, "end": date_end_10}]

date_start_11 = '20211101_000000'  # yyyymmdd_hhmmss   ### NOV
date_end_11 = '20211130_235959'    # yyyymmdd_hhmmss
dates_11 = [{"start": date_start_11, "end": date_end_11}]

date_start_12 = '20211201_000000'  # yyyymmdd_hhmmss   ### DEC
date_end_12 = '20211231_235959'    # yyyymmdd_hhmmss
dates_12 = [{"start": date_start_12, "end": date_end_12}]



roi = [('intersects', "Polygon ((-95.155703 41.68132, -74.343472 41.68132, -74.343472 56.861705, -95.155703 56.861705, -95.155703 41.68132))")]

# filters = {'Beam Mnemonic': ('=', ['16M11', '16M13']), 'Incidence Angle': ('>=', '35')}


# info for saving metadata for scenes 
parameters_pass = { 'recordId', 'collectionId', 'beamModeVersion', 'beamMnemonic', 'processingLevel', 'incidenceAngle', 'numberOfAzimuthLooks', 'productType', 'polarization', 'beamModeType', 'orbitDirection', 'numberOfRangeLooks', 'spatialResolution', 'polarizationInProduct', 'sampledPixelSpacing', 'acquisitionEndDate', 'lookOrientation', 'satelliteId', 'processorVersion', 'productApplication', 'lutApplied', 'clientProjectName', 'geodeticTerrainHeight', 'relativeOrbit', 'beamModeDescription', 'polarizationDataMode', 'absoluteOrbit', 'acquisitionStartDate', 'beamModeDefinitionId', 'title', 'featureId', 'sensorFolder', 'thisRecordUrl', 'geometry', 'wktGeometry' }


# output information
outfile = '/Users/danielle/Work/AlgalBlooms/AlgalBloomWebApp/radarsat_cm/data/RADARSAT-cm_' + date_start_1[0:8] + '_' + date_end_2[0:8] + '_ontario.shp'



###
### SCRIPT START ###
###

import fiona
import pandas as pd
import geopandas as gpd
from eodms_rapi import EODMSRAPI
from datetime import datetime
from shapely import wkt


# set up & perform search
rapi = EODMSRAPI(username, password)
print('\n\nLogged in successfully')

#dates = [{"start": date_start, "end": date_end}]

print('Querying data... \n')
# can search for relevant collections using: >>> print(rapi.get_collections(as_list=True))


### NEED TO BREAK IT UP .... not sure why but throws an error if I try to do Jan - Sept 2021 all at once... 
print('\n\nJanuary... ')
rapi.search("RCMImageProducts", features=roi, dates=dates_1)   # Submit the search to the EODMSRAPI, specifying the Collection
res_1 = rapi.get_results('full')   # Get the results from the search

print('\n\nFebruary... ')
rapi.search("RCMImageProducts", features=roi, dates=dates_2)   # Submit the search to the EODMSRAPI, specifying the Collection
res_2 = rapi.get_results('full')   # Get the results from the search

print('\n\nMarch... ')
rapi.search("RCMImageProducts", features=roi, dates=dates_3)   # MAR
res_3 = rapi.get_results('full')  

print('\n\nApril... ')
rapi.search("RCMImageProducts", features=roi, dates=dates_4)   # ARP
res_4 = rapi.get_results('full')  

print('\n\nMay... ')
rapi.search("RCMImageProducts", features=roi, dates=dates_5)   # MAY
res_5 = rapi.get_results('full')  

print('\n\nJune... ')
rapi.search("RCMImageProducts", features=roi, dates=dates_6)   # JUN
res_6 = rapi.get_results('full')  

print('\n\nJuly... ')
rapi.search("RCMImageProducts", features=roi, dates=dates_7)   # JUL
res_7 = rapi.get_results('full')  

print('\n\nAugust... ')
rapi.search("RCMImageProducts", features=roi, dates=dates_8)   # AUG
res_8 = rapi.get_results('full')  

print('\n\nSeptember... ')
rapi.search("RCMImageProducts", features=roi, dates=dates_9)   # SEP
res_9 = rapi.get_results('full')  

print('\n\nOctober... ')
rapi.search("RCMImageProducts", features=roi, dates=dates_10)   # OCT
res_10 = rapi.get_results('full')  

print('\n\nNovember... ')
rapi.search("RCMImageProducts", features=roi, dates=dates_11)   # NOV
res_11 = rapi.get_results('full')  

print('\n\nDecember... ')
rapi.search("RCMImageProducts", features=roi, dates=dates_12)   # DEC
res_12 = rapi.get_results('full')  



# test that these lists are actually populated... 
print( '\n LENGTH RADARSAT-CM Jan:  ', str(len(res_1)))
print( '\n LENGTH RADARSAT-CM Feb:  ', str(len(res_2)))
print( '\n LENGTH RADARSAT-CM Mar:  ', str(len(res_3)))
print( '\n LENGTH RADARSAT-CM Apr:  ', str(len(res_4)))
print( '\n LENGTH RADARSAT-CM May:  ', str(len(res_5)))
print( '\n LENGTH RADARSAT-CM Jun:  ', str(len(res_6)))
print( '\n LENGTH RADARSAT-CM Jul:  ', str(len(res_7)))
print( '\n LENGTH RADARSAT-CM Aug:  ', str(len(res_8)))
print( '\n LENGTH RADARSAT-CM Sep:  ', str(len(res_9)))
print( '\n LENGTH RADARSAT-CM Oct:  ', str(len(res_10)))
print( '\n LENGTH RADARSAT-CM Nov:  ', str(len(res_11)))
print( '\n LENGTH RADARSAT-CM Dec:  ', str(len(res_12)))


# re-merge... res_1/2 are both lists, so it's easy 
res = res_1 + res_2 + res_3 + res_4 + res_5 + res_6 + res_7 + res_8 + res_9 + res_10 + res_11 + res_12



# Print results
#rapi.print_results()


# check to see is all the keys are the same
product_keys = [res[footprint].keys() for footprint in range(len(res))]
product_keys_check = set([res[0].keys() == res[footprint].keys() for footprint in range(len(res))])  # {TRUE}

if False in product_keys_check:
    print('\nERROR: Product keys are inconsistent between products \n\n')
else:
    print('\nGOOD TO GO: Product keys are consistent between products :) \n\n')




# filter the data to only parameters of interest
products = [{k1: v1 for k1, v1 in res[j].items() if k1 in parameters_pass} for j in range(len(res))]


# numerical data is stored as strings, so need to convert them to numbers 
# also add a compatible date column
radarsat_cm_datetime_fmt = '%Y-%m-%dT%H:%M:%S'
for i in range(len(products)):
    products[i]['acquisitionStartDate'] = products[i]['acquisitionStartDate'][:-6] # need to strip the last few digits ' +0000' - it's all the same anyways 
    products[i]['starttime'] = datetime.strptime(products[i]['acquisitionStartDate'], radarsat_cm_datetime_fmt)
    products[i]['starttime'] = products[i]['starttime'].strftime('%Y-%m-%d %H:%M:%S.%f')
    
    products[i]['acquisitionEndDate'] = products[i]['acquisitionEndDate'][:-6] # need to strip the last few digits ' +0000' - it's all the same anyways 
    products[i]['endtime'] = datetime.strptime(products[i]['acquisitionEndDate'], radarsat_cm_datetime_fmt)
    products[i]['endtime'] = products[i]['endtime'].strftime('%Y-%m-%d %H:%M:%S.%f')
    
    products[i]['recordId'] = int(products[i]['recordId'])
    products[i]['incidenceAngle'] = int(products[i]['incidenceAngle'])
    if products[i]['numberOfAzimuthLooks'] != '': products[i]['numberOfAzimuthLooks'] = int(products[i]['numberOfAzimuthLooks'])
    if products[i]['numberOfRangeLooks'] != '': products[i]['numberOfRangeLooks'] = int(products[i]['numberOfRangeLooks'])
    products[i]['spatialResolution'] = int(products[i]['spatialResolution'])
    if products[i]['sampledPixelSpacing'] != '': products[i]['sampledPixelSpacing'] = float(products[i]['sampledPixelSpacing'])
    if products[i]['geodeticTerrainHeight'] != '': products[i]['geodeticTerrainHeight'] = float(products[i]['geodeticTerrainHeight'])
    products[i]['relativeOrbit'] = int(products[i]['relativeOrbit'])
    products[i]['absoluteOrbit'] = float(products[i]['absoluteOrbit'])
    if products[i]['beamModeDefinitionId'] != '': products[i]['beamModeDefinitionId'] = float(products[i]['beamModeDefinitionId'])
    



# lets try using pandas dataframe instead... could save a lot of time... 
df = pd.DataFrame(products)
df['geometry'] = gpd.GeoSeries.from_wkt(df['wktGeometry'])
gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')


# write data to shapefile
gdf.to_file(outfile)





###
### investigating parts of the data
###

# set([res[i]['productFormat'] for i in range(len(res))])
# numberOfAzimuthLooks = [products[j]['numberOfAzimuthLooks'] for j in range(len(products))]
# len(set([res[i]['thisRecordUrl'] for i in range(len(res))]))

# print out the type/class, key name, and value of one of the records/footprints
# for key, value in products[0].items():
#     print(str(type(value)) + ' - ' + key + '   :   ' + str(value))







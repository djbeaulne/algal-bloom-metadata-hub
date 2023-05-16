# python3 /Users/danielle/Work/AlgalBlooms/AlgalBloomWebApp/scripts/landsat89.py


###
### SCRIPT DESCRIPTION ###
### 

# AUTHOR: Danielle Beaulne
# LAST MODIFIED: 8 May 2023


### SHORT DESCRPITION ###
# This script works with LANDSAT-8/9 metadata
#   - the metadata is downloaded from: https://www.usgs.gov/core-science-systems/nli/landsat/bulk-metadata-service (hosts LANDSAT-8/9 data from April 2013 - present)
#       - 1 file for level 1 data
#       - 1 file for level 2 data
#   - subsets and reduces data as determined by parameters set by user
#       - time period of interest
#       - region of interest
#       - parameters of interest
#   - outputs 3 shapefiles
#       - 1 shapefile for metadata of Level 1 LANDSAT 8/9 data
#       - 1 shapefile for metadata of Level 2 LANDSAT 8/9 data
#       - 1 shapefile for merged metadata of Level 1 & Level 2 LANDSAT 8/9 data

### INTPUTS/OUTPUTS ###
# Inputs: 
#   - input files for LANDSAT-8/9 Level 1 and Level 2 metadata
#   - start/end date of interest
#   - region of interest (geojson) 
#   - output filename (shapefile)
#   - parameters of interest

# Outputs: 
#   - shapefile with polygons & metadata for LANDSAT-8/9 data (compatible with ArcGIS Online) 

### LIMITATIONS ### 
# FILE SIZE
#   - ArcGIS online can only handle files up to 10 MB in size

###########################################################################

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

from shapely.geometry import Polygon
from shapely.geometry import Point
from datetime import datetime
from datetime import date



###
###
### USER INPUT ###
###
###

# time period of interest
date_start = '2022-01-01'
date_end = '2022-12-31'

# region of interest
roi_shortname = 'ramsey'
roi = '/Users/danielle/Work/AlgalBlooms/AlgalBloomWebApp/data/ROIs/GreatLakes_extent.geojson' 

# LANDSAT metadata files
infile_LANDSAT_Level1 = '/Users/danielle/Work/AlgalBlooms/AlgalBloomWebApp/data_landsat8/LANDSAT_OT_C2_L1-2_downloaded-07-nov-2022.csv'
infile_LANDSAT_Level2 = '/Users/danielle/Work/AlgalBlooms/AlgalBloomWebApp/data_landsat8/LANDSAT_OT_C2_L2-2_downloaded-07-nov-2022.csv'

# output filename
outfile_merge = '/Users/danielle/Work/AlgalBlooms/AlgalBloomWebApp/data/data_github_test/LANDSAT-8-9_data_Level_1-2_merge_' + date_start + '_' + date_end + '_' + roi_shortname+ '.shp'

outfile_level1 = '/Users/danielle/Work/AlgalBlooms/AlgalBloomWebApp/data/data_github_test/LANDSAT-8-9_data_Level_1_' + date_start + '_' + date_end + '_' + roi_shortname+ '.shp'
outfile_level2 = '/Users/danielle/Work/AlgalBlooms/AlgalBloomWebApp/data/data_github_test/LANDSAT-8-9_data_Level_2_' + date_start + '_' + date_end + '_' + roi_shortname+ '.shp'

# parameters of interest; different parameters for Level-1 and Level-2 data
parameter_pass_L1 = ['Landsat Product Identifier L1', 'Landsat Scene Identifier', 'Date Acquired', 'Collection Category', 'Collection Number', 'WRS Path', 'WRS Row', 'Nadir/Off Nadir', 'Land Cloud Cover', 'Scene Cloud Cover L1', 'Start Time', 'Stop Time', 'Station Identifier', 'Day/Night Indicator', 'Sun Elevation L0RA', 'Sun Azimuth L0RA', 'Data Type L1', 'Sensor Identifier', 'Product Map Projection L1', 'UTM Zone', 'Ellipsoid', 'geometry', 'Satellite']

parameter_pass_L2 = ['Landsat Product Identifier L2', 'Landsat Product Identifier L1', 'Landsat Scene Identifier', 'Date Acquired', 'Collection Category', 'Collection Number', 'WRS Path', 'WRS Row', 'Target WRS Path', 'Target WRS Row', 'Nadir/Off Nadir', 'Land Cloud Cover', 'Scene Cloud Cover L1', 'Start Time', 'Stop Time', 'Station Identifier', 'Day/Night Indicator', 'Sun Elevation L0RA', 'Sun Azimuth L0RA', 'Sensor Identifier', 'Product Map Projection L1', 'UTM Zone', 'Ellipsoid', 'Data Type L2', 'geometry', 'Satellite']



###
### SCRIPT START ###
###

roi_gpd = gpd.read_file(roi)



###
### IMPORT LEVEL-1 DATA ###
###
print('Reading LANDSAT Level 1 metadata...')
data_level_1 = pd.read_csv(infile_LANDSAT_Level1)


###
### FILTERING ###
###

### filter to time period of interest
data_level_1['Date Acquired_datetime'] = pd.to_datetime(data_level_1['Date Acquired'], format='%Y/%m/%d')   # convert to datetime
data_level_1 = data_level_1.loc[(data_level_1['Date Acquired_datetime'] >= date_start) & (data_level_1['Date Acquired_datetime'] < date_end)]
data_level_1 = data_level_1.reset_index(drop=True)


### filter to study area
# metadata doesn't have a geometry column; it has multiple columns with the nodes for the extent
# create geometry column from provided information 
geometry_lst = [None]*len(data_level_1)
geometry_wkt = [None]*len(data_level_1)
for i in range(len(data_level_1)):
    
    # reassign the values to make things more legible... 
    ULY = data_level_1['Corner Upper Left Latitude'][i]
    ULX = data_level_1['Corner Upper Left Longitude'][i]
    URY = data_level_1['Corner Upper Right Latitude'][i]
    URX = data_level_1['Corner Upper Right Longitude'][i]
    LLY = data_level_1['Corner Lower Left Latitude'][i]
    LLX = data_level_1['Corner Lower Left Longitude'][i]
    LRY = data_level_1['Corner Lower Right Latitude'][i]
    LRX = data_level_1['Corner Lower Right Longitude'][i]
    
    geometry_lst[i] = [[ULX, ULY], [URX, URY], [LRX, LRY], [LLX, LLY], [ULX, ULY]]
    geometry_wkt[i] = Polygon(geometry_lst[i])
    
    # this take a while, so keep track of progress
    if i in list(range(10000, len(data_level_1), 10000)):
        print(str(i) + '/' + str(len(data_level_1)) + ' records processed')


# assign the geometry columns to the existing dataframe
data_level_1 = data_level_1.assign(geometry_lst = geometry_lst)
data_level_1 = data_level_1.assign(geometry_wkt = geometry_wkt)

# convert to GeoPandas
data_level_1_gpd = gpd.GeoDataFrame(data_level_1, geometry = data_level_1['geometry_wkt'])
data_level_1_gpd = data_level_1_gpd.set_crs('EPSG:4326')

# FILTER TO STUDY AREA
intersection = gpd.overlay(roi_gpd, data_level_1_gpd, how='intersection')
data_level_1_subset_gpd = data_level_1_gpd.loc[data_level_1_gpd['Landsat Scene Identifier'].isin(intersection['Landsat Scene Identifier'])]

data_level_1_subset_gpd = data_level_1_subset_gpd.reset_index(drop=True)
data_level_1_subset_gpd = data_level_1_subset_gpd[parameter_pass_L1]


# write LANDSAT Level 1 metadata to file
data_level_1_subset_gpd.to_file(outfile_level1)





###
### IMPORT LEVEL-2 DATA ###
###


# this is literally every scene that LANDSAT-8 has ever collected... 
### downloaded from: https://www.usgs.gov/core-science-systems/nli/landsat/bulk-metadata-service
print('Reading LANDSAT Level 2 metadata...')
data_level_2 = pd.read_csv(infile_LANDSAT_Level2)

# investigate the columns 
#headers = data.columns

###
### FILTERING ###
###

# FILTER TO DESIRED TIMELINE
# need to convert the date to datetime64
data_level_2['Date Acquired_datetime'] = pd.to_datetime(data_level_2['Date Acquired'], format='%Y/%m/%d')
data_level_2 = data_level_2.loc[(data_level_2['Date Acquired_datetime'] >= date_start) & (data_level_2['Date Acquired_datetime'] < date_end)]

data_level_2 = data_level_2.reset_index(drop=True)


### FILTER TO STUDY AREA 
# create geometry column from provided information 
geometry_lst = [None]*len(data_level_2) # column with list of coordinates
geometry_wkt = [None]*len(data_level_2) # column for the actual new geometry cokunb
for i in range(len(data_level_2)):
    
    # reassign the values to make things more legible... 
    ULY = data_level_2['Corner Upper Left Latitude'][i]
    ULX = data_level_2['Corner Upper Left Longitude'][i]
    URY = data_level_2['Corner Upper Right Latitude'][i]
    URX = data_level_2['Corner Upper Right Longitude'][i]
    LLY = data_level_2['Corner Lower Left Latitude'][i]
    LLX = data_level_2['Corner Lower Left Longitude'][i]
    LRY = data_level_2['Corner Lower Right Latitude'][i]
    LRX = data_level_2['Corner Lower Right Longitude'][i]
    
    geometry_lst[i] = [[ULX, ULY], [URX, URY], [LRX, LRY], [LLX, LLY], [ULX, ULY]]
    geometry_wkt[i] = Polygon(geometry_lst[i])
    
    # this takes a while, so keep track of progress
    if i in list(range(10000, len(data_level_2), 10000)):
        print(str(i) + '/' + str(len(data_level_2)) + ' records processed')


# assign the geometry columns to the existing dataframe
data_level_2 = data_level_2.assign(geometry_lst = geometry_lst)
data_level_2 = data_level_2.assign(geometry_wkt = geometry_wkt)

# convert to GeoPandas
data_level_2_gpd = gpd.GeoDataFrame(data_level_2, geometry = data_level_2['geometry_wkt'])
data_level_2_gpd = data_level_2_gpd.set_crs('EPSG:4326')

# FILTER TO STUDY AREA
intersection = gpd.overlay(roi_gpd, data_level_2_gpd, how='intersection')
data_level_2_subset_gpd = data_level_2_gpd.loc[data_level_2_gpd['Landsat Scene Identifier'].isin(intersection['Landsat Scene Identifier'])]

data_level_2_subset_gpd = data_level_2_subset_gpd.reset_index(drop=True)
data_level_2_subset_gpd = data_level_2_subset_gpd[parameter_pass_L2]


# write LANDSAT Level 2 metadata to file
data_level_2_subset_gpd.to_file(outfile_level2)





###
### MERGE DATA & 
### CHECK CONSISTENCY BETWEEN LEVEL-1 AND LEVEL-2 DATA ###
###

# rename columns that I want to merge together... & make columns that I want...
data_level_1_subset_gpd = data_level_1_subset_gpd.rename(columns={'Data Type L1': 'Data Type'})
data_level_2_subset_gpd = data_level_2_subset_gpd.rename(columns={'Data Type L2': 'Data Type'})
data_level_1_subset_gpd['Product Level'] = 'Level 1' 
data_level_2_subset_gpd['Product Level'] = 'Level 2' 


# merge data & consistency analysis 
data_merge = pd.concat([data_level_1_subset_gpd, data_level_2_subset_gpd])
data_merge = data_merge.reset_index(drop=True)


# add/replace/delete/rename data to make them compatible for making the data summary file (update fields)
#   need to also combine the file names since it's a merged file of Level 1 & level 2 data
#   if the data is level 2, use that filename only
#   column names max 10 characters (it'll be truncated to 10 when exporting anyways)
data_merge['filename'] = ''
for i in range(len(data_merge)):
    if pd.isna(data_merge['Landsat Product Identifier L2'][i]):
        data_merge['filename'][i] = data_merge['Landsat Product Identifier L1'][i]
    else:
        data_merge['filename'][i] = data_merge['Landsat Product Identifier L2'][i]


data_merge['mission'] = 'LANDSAT'
data_merge = data_merge.replace({'Satellite': {8:'LANDSAT-8', 9:'LANDSAT-9'}})
data_merge = data_merge.drop('Collection Number', axis=1)
data_merge.rename(columns={'Landsat Scene Identifier':'sceneid', 'Scene Cloud Cover L1':'cloudcover', 'Start Time':'starttime', 'Sun Elevation L0RA':'sun_elev', 'Sun Azimuth L0RA':'sun_azim', 'Data Type':'datatype', 'Sensor Identifier':'sensorid', 'Satellite':'platform', 'Landsat Product Identifier L1':'filenameL1', 'Landsat Product Identifier L2':'filenameL2', 'Target WRS Path': 'WRS Path_T', 'Target WRS Row':'WRS Row_T', 'Collection Category':'Collection', 'Date Acquired':'DateAcquir', 'Nadir/Off Nadir':'Nadir/Off', 'Land Cloud Cover':'Land Cloud', 'Day/Night Indicator':'Day/Night', 'Product Map Projection L1':'ProductMap', 'Product Level':'ProductLev', 'Station Identifier':'Station Id'}, inplace=True)


###
### OUTPUT MERGED FILE
###
data_merge.to_file(outfile_merge)




# python3 /Users/danielle/Work/AlgalBlooms/AlgalBloomWebApp/AlgalBloom_WebApp_DataUpdateFields_DataSummary_2022.py


###
### SCRIPT DESCRIPTION ###
### 

# AUTHOR: Danielle Beaulne
# DATE CREATED: 26 Apr 2022
# LAST MODIFIED: 18 Nov 2022

### SHORT DESCRPITION ###
# This script cleans up the field names and values for all of the data used in the web app
#   - reads in all of the pre-formatted shapefile (for ArcGIS online) data, renames fields, and renames values as necessary
#   - outputs a combined shapefile compatible with ArcGIS online

### INTPUTS/OUTPUTS ###
# Inputs: 
#   - data: 
#       - LANDSAT 8
#       - Sentinel-1 SAR
#       - Sentinel-2 MSI
#       - Sentinel-3 OLCI
#       - RADARSAT-2 SAR
#       - RADARSAT-CM SAR

# Outputs: 
#   - individual files per mission with updated field names and/or values (shapefile; compatible with ArcGIS Online) 

#########################

import geopandas as gpd
import pandas as pd
import statistics
import numpy as np

from datetime import datetime
from datetime import date

# pd.set_option('display.max_columns', None)



###
### USER INPUT ###
###

# mostly 2022.... 
#   - RS-1 is acutally 2011, since it doesn't extend to 2022

root = '/Users/danielle/Work/AlgalBlooms/AlgalBloomWebApp/'
output_folder = 'ArcGIS_online/'

outfile_datasum = root + output_folder + 'data_algalbloom_summary_2022-May-Nov07_' + str(date.today()) + '.shp'

missions = ['LANDSAT', 'Sentinel-1', 'Sentinel-2', 'Sentinel-3', 'RADARSAT-1', 'RADARSAT-2', 'RADARSAT-CM']

data = {
    'LANDSAT': {'folder': 'data_landsat8/', 
                'infile': 'LANDSAT-8-9_data_Level_1-2_merge_2022-01-01_2022-11-07.shp', 
                'outfile': 'LANDSAT-8-9_data_Level_1-2_merge_2022-01-01_2022-11-07_updatedFields.shp'},
    'Sentinel-1': {'folder': 'data_s1sar_intermediate/', 
                   'infile': 'S1_metadata_20220101_20221107_2022-11-08.shp', 
                   'outfile': 'S1_metadata_20220101_20221107_2022-11-08_updatedFields.shp'}, 
    'Sentinel-2': {'folder': 'data_s2msi_intermediate/', 
                   'infile': 'S2_metadata_20220101_20221107_2022-11-08.shp', 
                   'outfile': 'S2_metadata_20220101_20221107_2022-11-08_updatedFields.shp'},
    'Sentinel-3': {'folder': 'data_s3olci_intermediate/', 
                   'infile': 'S3_metadata_20220101_20221107.shp', 
                   'outfile': 'S3_metadata_20220101_20221107_updatedFields.shp'},
    'RADARSAT-1': {'folder': 'radarsat_1/data/', 
                   'infile': 'RADARSAT-1_20110101_20111231_greatlakes_2022-06-03.shp', 
                   'outfile': 'RADARSAT-1_20110101_20111231_greatlakes_2022-06-03_updatedFields.shp'},
    'RADARSAT-2': {'folder': 'radarsat_2/data/', 
                   'infile': 'RADARSAT-2_20220101_20221107_greatlakes.shp', 
                   'outfile': 'RADARSAT-2_20220101_20221107_greatlakes_updatedFields.shp'},
    'RADARSAT-CM': {'folder': 'radarsat_cm/data/', 
                    'infile': 'RADARSAT-cm_20220101_20221107_greatlakes.shp', 
                    'outfile': 'RADARSAT-cm_20220101_20221107_greatlakes-_updatedFields.shp'}
}


# parameters to keep for the Data Summary file; using the updated field names
data['LANDSAT']['parameter pass'] = ['filename', 'sceneid', 'Collection', 'WRS Path', 'WRS Row', 'cloudcover', 'starttime', 'Day/Night', 'sun_elev', 'sun_azim', 'datatype', 'sensorid', 'UTM Zone', 'Ellipsoid', 'mission', 'geometry', 'platform']
data['Sentinel-1']['parameter pass'] = ['link', 'starttime', 'ingestion', 'slice', 'orbit_abs', 'orbit_rel', 'sensormode', 'orbit_dir', 'datatype', 'mission', 'platform', 'filename', 'polarisati', 'uuid', 'geometry']
data['Sentinel-2']['parameter pass'] = ['link', 'starttime', 'ingestion', 'orbit_abs', 'orbit_rel', 'cloudcover', 'tileid', 'mission', 'filename', 'orbit_dir', 'platform', 'proc_level', 'uuid', 'geometry']
data['Sentinel-3']['parameter pass'] = ['link', 'starttime', 'ingestion', 'orbit_abs', 'orbit_rel', 'proc_level', 'mission', 'platform', 'filename', 'datatype', 'timeliness', 'orbit_dir', 'passnumRel', 'passnumAbs', 'uuid', 'cloudcover', 'geometry']
data['RADARSAT-1']['parameter pass'] = ['recordId', 'mission', 'proc_level', 'angle_inc', 'polarisati', 'orbit_dir', 'orbit_abs', 'spatialRes', 'beam', 'sensormode', 'lookOrient', 'lutApplied', 'filename', 'featureId', 'link', 'thisRecord', 'starttime', 'geometry', 'datatype']
data['RADARSAT-2']['parameter pass'] = ['recordId', 'mission', 'angle_inc', 'quality', 'orbit_dir', 'orbit_abs', 'polarisati', 'polTransmt', 'spatialRes', 'sensormode', 'featureId', 'lookOrient', 'filename', 'starttime', 'geometry', 'datatype']
data['RADARSAT-CM']['parameter pass'] = ['recordId', 'mission', 'proc_level', 'angle_inc', 'AzLookNum', 'datatype', 'polarisati', 'beammode', 'orbit_dir', 'RngLookNum', 'spatialRes', 'PolIn_Prod', 'pxlSpacing', 'lookOrient', 'platform', 'applicatin', 'lutApplied', 'orbit_rel', 'beam_mode', 'orbit_abs', 'filename', 'featureId', 'thisRecord', 'starttime', 'geometry']




###
### SCRIPT START ###
###

for mission in missions:
    print('Reading data... ', mission)
    data[mission]['data'] = gpd.read_file(root + data[mission]['folder'] + data[mission]['infile'])
    


# add/replace/delete/rename data
# LANDSAT
#   need to also combine the file names since it's a merged file of Level 1 & level 2 data
#   if the data is level 2, use that filename only
data['LANDSAT']['data']['filename'] = ''
for i in range(len(data['LANDSAT']['data'])):
    if data['LANDSAT']['data']['Landsat _1'][i] == None:
        data['LANDSAT']['data']['filename'][i] = data['LANDSAT']['data']['Landsat Pr'][i]
    else:
        data['LANDSAT']['data']['filename'][i] = data['LANDSAT']['data']['Landsat _1'][i]

data['LANDSAT']['data']['mission'] = 'LANDSAT'
data['LANDSAT']['data'] = data['LANDSAT']['data'].replace({'Satellite': {8:'LANDSAT-8', 9:'LANDSAT-9'}})
data['LANDSAT']['data'] = data['LANDSAT']['data'].drop('Collecti_1', axis=1)
data['LANDSAT']['data'].rename(columns={'Landsat Sc':'sceneid', 'Scene Clou':'cloudcover', 'Start Time':'starttime', 'Sun Elevat':'sun_elev', 'Sun Azimut':'sun_azim', 'Data Type':'datatype', 'Sensor Ide':'sensorid', 'Satellite':'platform', 'Landsat Pr':'filenameL1', 'Landsat _1':'filenameL2', 'Target WRS': 'WRS Path_T', 'Target W_1':'WRS Row_T'}, inplace=True)

# SENTINEL-1
data['Sentinel-1']['data'] = data['Sentinel-1']['data'].replace({'platformid': {'2014-016A':'Sentinel-1A', '2016-025A':'Sentinel-1B'}})
data['Sentinel-1']['data'] = data['Sentinel-1']['data'].drop('swathident', axis=1)
data['Sentinel-1']['data'].rename(columns={'beginposit':'starttime', 'ingestiond':'ingestion', 'slicenumbe':'slice', 'orbitnumbe':'orbit_abs', 'relativeor':'orbit_rel', 'sensoroper':'sensormode', 'orbitdirec':'orbit_dir', 'platformna': 'mission', 'platformid':'platform', 'producttyp':'datatype'}, inplace=True)

# SENTINEL-2
data['Sentinel-2']['data'] = data['Sentinel-2']['data'].drop(['platformid', 'processing', 'producttyp'], axis=1)
data['Sentinel-2']['data'].rename(columns={'beginposit':'starttime', 'ingestiond':'ingestion', 'orbitnumbe':'orbit_abs', 'relativeor':'orbit_rel', 'platformna':'mission', 'orbitdirec':'orbit_dir', 'platformse':'platform', 'processi_1':'proc_level', 'producttyp':'datatype'}, inplace=True)

# Sentinel-3
data['Sentinel-3']['data'] = data['Sentinel-3']['data'].replace({'platformid': {'2016-011A':'Sentinel-3A', '2018-039A':'Sentinel-3B'}, 'productlev': {'L1': 'Level 1', 'L2': 'Level 2'}})
data['Sentinel-3']['data'] = data['Sentinel-3']['data'].drop(['link_icon', 'summary', 'identifier', 'procfacili', 'procfaci_1', 'processing'], axis=1)
data['Sentinel-3']['data'].rename(columns={'beginposit':'starttime', 'ingestiond':'ingestion', 'orbitnumbe':'orbit_abs', 'relativeor':'orbit_rel', 'productlev':'proc_level', 'platformna':'mission', 'platformid':'platform', 'orbitdirec': 'orbit_dir', 'relpassnum':'passnumRel', 'passnumber':'passnumAbs', 'producttyp':'datatype'}, inplace=True)

# RADARSAT-1 
data['RADARSAT-1']['data'] = data['RADARSAT-1']['data'].replace({'processing': {'l1':'Level 1'}, 'collection': {'Radarsat1': 'RADARSAT-1'}})
data['RADARSAT-1']['data'].rename(columns={'collection':'mission', 'processing':'proc_level', 'incidenceA':'angle_inc', 'orbitDirec':'orbit_dir', 'absoluteOr':'orbit_abs', 'sensorMode':'sensormode', 'downloadLi':'link', 'polarizati':'polarisati', 'productTyp':'datatype', 'title':'filename'}, inplace=True)

# RADARSAT-2
data['RADARSAT-2']['data']['datatype'] = 'Raw'
data['RADARSAT-2']['data'] = data['RADARSAT-2']['data'].replace({'collection': {'Radarsat2RawProducts':'RADARSAT-2'}})
data['RADARSAT-2']['data'] = data['RADARSAT-2']['data'].drop(['startDate', 'position', 'beam', 'wktGeometr'], axis=1)
data['RADARSAT-2']['data'].rename(columns={'collection':'mission', 'incidenceA':'angle_inc', 'segmentQua':'quality', 'absoluteOr':'orbit_abs', 'polarizati':'polarisati', 'transmitPo':'polTransmt', 'sensorMode':'sensormode', 'title':'filename', 'orbitDirec':'orbit_dir'}, inplace=True)

# RADARSAT-CM
data['RADARSAT-CM']['data'] = data['RADARSAT-CM']['data'].replace({'collection': {'RCMImageProducts':'RADARSAT-CM'}, 'processing': {'l1':'Level 1', 'l3':'Level 3'}, 'satelliteI': {'RCM-1':'RADARSAT-CM 1', 'RCM-2':'RADARSAT-CM 2', 'RCM-3':'RADARSAT-CM 3'}})
data['RADARSAT-CM']['data'] = data['RADARSAT-CM']['data'].drop(['acquisitio', 'beamMnemon', 'acquisit_1', 'sensorFold', 'wktGeometr'], axis=1)
data['RADARSAT-CM']['data'].rename(columns={'collection': 'mission', 'processing':'proc_level', 'incidenceA':'angle_inc', 'numberOfAz':'AzLookNum', 'polarizati':'polarisati', 'beamModeTy':'beammode', 'orbitDirec':'orbit_dir', 'numberOfRa':'RngLookNum', 'polariza_1':'PolIn_Prod', 'sampledPix':'pxlSpacing', 'satelliteI':'platform', 'productApp':'applicatin', 'relativeOr':'orbit_rel', 'beamModeDe':'beam_mode', 'absoluteOr':'orbit_abs', 'productTyp':'datatype', 'title':'filename'}, inplace=True)



# check number of characters in each of the columns (shapefiles can have max 10 characters/column)
for mission in missions:
    keys = list(data[mission]['data'].keys())
    for key in keys:
        if len(key) > 10:
            print('\n\nERROR: Column name is longer than 10 characters\n\n', mission, ': ', key)
#        else:
#            print('GOOD: ', mission, ' - ', key)


# OUTPUT AS INDIVIDUAL SHAPEFILES
for mission in missions: 
    data[mission]['data'].to_file(root + output_folder + data[mission]['outfile'])
    print('Sucessfully output to shapefile:   ', mission)




### MAKING THE DATA SUMMARY FILE
print('\n\n\nMaking the data summary file...')

# clip data to only the fields needed for the summary file 
for mission in missions:
    print('Summarizing for data summary file... ', mission)
    data[mission]['data'] = data[mission]['data'][data[mission]['parameter pass']]

# EDIT CLOUD COVER INFORMATION... 
### only keep cloud cover from LANDSAT and Sentinel-2
### Scenes from Sentinel-3 are too large, and the other missions don't have cloud cover
### convert any irrelevant cloud cover data to -1 so that I can update it to NaN in arcGIS after importing
data['Sentinel-3']['data']['cloudcover'] = np.nan

# combine the data
data_sum = pd.concat([data['LANDSAT']['data'], data['Sentinel-1']['data'], data['Sentinel-2']['data'], data['Sentinel-3']['data'], data['RADARSAT-1']['data'], data['RADARSAT-2']['data'], data['RADARSAT-CM']['data']], ignore_index=True)

# clip to relevant timeline
data_sum['starttime_tmp'] = pd.to_datetime(data_sum['starttime'], format='%Y-%m-%d %H:%M:%S.%f')
data_sum = data_sum[(data_sum['starttime_tmp'] > '2022-04-30') & (data_sum['starttime_tmp'] < '2022-11-08')]
data_sum = data_sum.drop('starttime_tmp', axis=1)

### DROP OTHER COLUMNS TO REDUCE FILESIZE
data_sum = data_sum.drop(['applicatin', 'AzLookNum', 'RngLookNum', 'PolIn_Prod', 'beam_mode', 'pxlSpacing'], axis=1)   # only relevant to RADARSAT-CM...
data_sum = data_sum.drop(['Ellipsoid'], axis=1)   # only relevant to LANDSAT-8
data_sum = data_sum.drop(['timeliness'], axis=1)   # only relevant to Sentinel-3
data_sum = data_sum.drop(['slice'], axis=1)   # only relevant to Sentinel-1
data_sum = data_sum.drop(['lutApplied'], axis=1)   # relevant to RADARSAT-1, RADARSAT-CM; but not reallly sure what they are 
data_sum = data_sum[data_sum['quality'] != 'Problem']   # delete low quality records... (although there aren't any in the 2021 data...)
data_sum = data_sum.drop(['polTransmt', 'quality'], axis=1)   # relevant to RADARSAT-2
data_sum = data_sum.reset_index(drop=True)


### convert any irrelevant cloud cover data to -1 so that I can update it to NaN in arcGIS after importing
data_sum['cloudcover'] = data_sum['cloudcover'].fillna(-1)


# ouput as shapefile
data_sum.to_file(outfile_datasum)

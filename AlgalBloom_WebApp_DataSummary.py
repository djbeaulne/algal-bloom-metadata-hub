# python3 /Users/danielle/Work/AlgalBlooms/AlgalBloomWebApp/AlgalBloom_WebApp_DataSummary_extended_2021.py


###
### SCRIPT DESCRIPTION ###
### 

# AUTHOR: Danielle Beaulne
# DATE CREATED: 26 Apr 2022
# LAST MODIFIED: 24 June 2022

### SHORT DESCRPITION ###
# This script combines the most essential data from multiple sensors
#   - reads in all of the pre-formatted shepafile (for ArcGIS online) data, strips it down, and combines them
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
#   - file summarizing key information from multiple sensors (shapefile; compatible with ArcGIS Online) 


#########################
# pd.set_option('display.max_columns', None)

import geopandas as gpd
import pandas as pd
import statistics

from datetime import datetime
from datetime import date


###
### USER INPUT ###
###


root = '/Users/danielle/Work/AlgalBlooms/AlgalBloomWebApp/'

infile_data_s1_sar = 'data_s1sar_intermediate/S1_metadata_20220101_20221107_2022-11-08.shp'
infile_data_s2_msi = 'data_s2msi_intermediate/S2_metadata_20220101_20221107_2022-11-08.shp'
infile_data_s3_olci = 'data_s3olci_intermediate/S3_metadata_20220101_20221107.shp'
infile_data_landsat_oli = 'LANDSAT-8-9_data_Level_1-2_merge_2022-01-01_2022-11-07.shp'
infile_data_radarsat_1 = 'radarsat_1/data/RADARSAT-1_20110101_20111231_greatlakes_2022-06-03.shp'
infile_data_radarsat_2 = 'radarsat_2/data/RADARSAT-2_20220101_20221107_greatlakes.shp'
infile_data_radarsat_cm = 'radarsat_cm/data/RADARSAT-cm_20220101_20221107_greatlakes.shp'


outfile = root + 'data_summary/data_algalbloom_summary_2021-May-Nov_' + str(date.today()) + '.shp'



###
### SCRIPT START ###
###

# import data
data_s1_sar = gpd.read_file(root + infile_data_s1_sar)
data_s2_msi = gpd.read_file(root + infile_data_s2_msi)
data_s3_olci = gpd.read_file(root + infile_data_s3_olci)
data_landsat_oli = gpd.read_file(root + infile_data_landsat_oli)
data_radarsat_1 = gpd.read_file(root + infile_data_radarsat_1)
data_radarsat_2 = gpd.read_file(root + infile_data_radarsat_2)
data_radarsat_cm = gpd.read_file(root + infile_data_radarsat_cm)


# add/replace information to each sensor as necessary
data_radarsat_2['datatype'] = 'Raw'
data_landsat_oli['mission'] = 'LANDSAT'

data_s1_sar = data_s1_sar.replace({'platformid': {'2014-016A':'Sentinel-1A', '2016-025A':'Sentinel-1B'}})
data_s3_olci = data_s3_olci.replace({'platformid': {'2016-011A':'Sentinel-3A', '0000-000A':'Sentinel-3B', '2018-039A':'Sentinel-3B'}})
data_s3_olci = data_s3_olci.replace({'productlev': {'L1':'Level 1', 'L2':'Level 2'}})
data_landsat_oli = data_landsat_oli.replace({'Satellite': {8:'LANDSAT-8', 9:'LANDSAT-9'}})
data_radarsat_1 = data_radarsat_1.replace({'processing': {'l1':'Level 1'}})
data_radarsat_1 = data_radarsat_1.replace({'collection': {'Radarsat1':'RADARSAT-1'}})
data_radarsat_2 = data_radarsat_2.replace({'collection': {'Radarsat2RawProducts':'RADARSAT-2'}})
data_radarsat_cm = data_radarsat_cm.replace({'collection': {'RCMImageProducts':'RADARSAT-CM'}})
data_radarsat_cm = data_radarsat_cm.replace({'processing': {'l1':'Level 1', 'l3':'Level 3'}})
data_radarsat_cm = data_radarsat_cm.replace({'satelliteI': {'RCM-1':'RADARSAT-CM 1', 'RCM-2':'RADARSAT-CM 2', 'RCM-3':'RADARSAT-CM 3'}})


# strip down the data to (a bit more than...) the necessary basics...
parameter_pass_s1_sar = ['link', 'beginposit', 'ingestiond', 'slicenumbe', 'orbitnumbe', 'relativeor', 'sensoroper', 'orbitdirec', 'producttyp', 'platformna', 'platformid', 'filename', 'polarisati', 'uuid', 'geometry']
parameter_pass_s2_msi = ['link', 'beginposit', 'ingestiond', 'orbitnumbe', 'relativeor', 'cloudcover', 'tileid', 'platformna', 'filename', 'orbitdirec', 'platformse', 'processi_1', 'uuid', 'geometry']
parameter_pass_s3_olci = ['link', 'beginposit', 'ingestiond', 'orbitnumbe', 'relativeor', 'productlev', 'platformna', 'platformid', 'filename', 'producttyp', 'timeliness', 'orbitdirec', 'relpassnum', 'passnumber', 'uuid', 'cloudcover', 'geometry']
parameter_pass_landsat_oli = ['Landsat Pr', 'Landsat Sc', 'Collection', 'WRS Path', 'WRS Row', 'Scene Clou', 'Start Time', 'Day/Night', 'Sun Elevat', 'Sun Azimut', 'Data Type', 'Sensor Ide', 'UTM Zone', 'Ellipsoid', 'mission', 'geometry', 'Satellite']
parameter_pass_radarsat_1 = ['recordId', 'collection', 'processing', 'incidenceA', 'polarizati', 'orbitDirec', 'absoluteOr', 'spatialRes', 'beam', 'sensorMode', 'lookOrient', 'lutApplied', 'title', 'featureId', 'downloadLi', 'thisRecord', 'starttime', 'geometry', 'productTyp']
parameter_pass_radarsat_2 = ['recordId', 'collection', 'incidenceA', 'beam', 'segmentQua', 'orbitDirec', 'absoluteOr', 'polarizati', 'transmitPo', 'spatialRes', 'sensorMode', 'featureId', 'lookOrient', 'title', 'starttime', 'geometry', 'datatype']
parameter_pass_radarsat_cm = ['recordId', 'collection', 'processing', 'incidenceA', 'numberOfAz', 'productTyp', 'polarizati', 'beamModeTy', 'orbitDirec', 'numberOfRa', 'spatialRes', 'polariza_1', 'sampledPix', 'lookOrient', 'satelliteI', 'productApp', 'lutApplied', 'relativeOr', 'beamModeDe', 'absoluteOr', 'title', 'featureId', 'thisRecord', 'starttime', 'geometry']


data_s1_sar = data_s1_sar[parameter_pass_s1_sar]
data_s2_msi = data_s2_msi[parameter_pass_s2_msi]
data_s3_olci = data_s3_olci[parameter_pass_s3_olci]
data_landsat_oli = data_landsat_oli[parameter_pass_landsat_oli]
data_radarsat_1 = data_radarsat_1[parameter_pass_radarsat_1]
data_radarsat_2 = data_radarsat_2[parameter_pass_radarsat_2]
data_radarsat_cm = data_radarsat_cm[parameter_pass_radarsat_cm]



# rename information as necessary
data_s1_sar.rename(columns={'beginposit':'starttime', 'ingestiond':'ingestion', 'slicenumbe':'slice', 'orbitnumbe':'orbit_abs', 'relativeor':'orbit_rel', 'sensoroper':'sensormode', 'orbitdirec':'orbit_dir', 'platformna': 'mission', 'platformid':'platform', 'producttyp':'datatype'}, inplace=True)

data_s2_msi.rename(columns={'beginposit':'starttime', 'ingestiond':'ingestion', 'orbitnumbe':'orbit_abs', 'relativeor':'orbit_rel', 'platformna':'mission', 'orbitdirec':'orbit_dir', 'platformse':'platform', 'processi_1':'proc_level', 'producttyp':'datatype'}, inplace=True)

data_s3_olci.rename(columns={'beginposit':'starttime', 'ingestiond':'ingestion', 'orbitnumbe':'orbit_abs', 'relativeor':'orbit_rel', 'productlev':'proc_level', 'platformna':'mission', 'platformid':'platform', 'orbitdirec': 'orbit_dir', 'relpassnum':'passnumRel', 'passnumber':'passnumAbs', 'producttyp':'datatype'}, inplace=True)

data_landsat_oli.rename(columns={'Landsat Pr':'filename', 'Landsat Sc':'sceneid', 'Scene Clou':'cloudcover', 'Start Time':'starttime', 'Sun Elevat':'sun_elev', 'Sun Azimut':'sun_azim', 'Data Type':'datatype', 'Sensor Ide':'sensorid', 'productid1':'filename', 'Satellite':'platform'}, inplace=True)

data_radarsat_1.rename(columns={'collection':'mission', 'processing':'proc_level', 'incidenceA':'angle_inc', 'orbitDirec':'orbit_dir', 'absoluteOr':'orbit_abs', 'sensorMode':'sensormode', 'downloadLi':'link', 'polarizati':'polarisati', 'productTyp':'datatype', 'title':'filename'}, inplace=True)

data_radarsat_2.rename(columns={'collection':'mission', 'incidenceA':'angle_inc', 'segmentQua':'quality', 'absoluteOr':'orbit_abs', 'polarizati':'polarisati', 'transmitPo':'polTransmt', 'sensorMode':'sensormode', 'title':'filename', 'orbitDirec':'orbit_dir'}, inplace=True)

data_radarsat_cm.rename(columns={'collection': 'mission', 'processing':'proc_level', 'incidenceA':'angle_inc', 'numberOfAz':'AzLookNum', 'polarizati':'polarisati', 'beamModeTy':'beammode', 'orbitDirec':'orbit_dir', 'numberOfRa':'RngLookNum', 'polariza_1':'PolIn_Prod', 'sampledPix':'pxlSpacing', 'satelliteI':'platform', 'productApp':'applicatin', 'relativeOr':'orbit_rel', 'beamModeDe':'beam_mode', 'absoluteOr':'orbit_abs', 'productTyp':'datatype', 'title':'filename'}, inplace=True)


# check number of characters in each of the columns (shapefiles can have max 10 characters/column)
sensors = [data_s1_sar, data_s2_msi, data_s3_olci, data_landsat_oli, data_radarsat_1, data_radarsat_2, data_radarsat_cm]
for i in range(len(sensors)):
    for j in range(len(list(sensors[i].keys()))):
        if len(list(sensors[i].keys())[j]) > 10:
            print('\n\nERROR: Column name is longer than 10 characters\n\n')




### need to convert the date fields in all data to be consistent and in the format: 
# print out sample dates from each data set to see what their format is... so that I know how to change it
print('Sentinel-1 SAR datetime format: ', data_s1_sar['starttime'][0], '\n')
print('Sentinel-2 MSI datetime format: ', data_s2_msi['starttime'][0], '\n')
print('Sentinel-3 OLCI datetime format: ', data_s3_olci['starttime'][0], '\n')
print('LANDSAT-8 OLI datetime format: ', data_landsat_oli['starttime'][0], '\n')
print('RADARSAT-1 SAR datetime format: ', data_radarsat_1['starttime'][0], '\n')
print('RADARSAT-2 SAR datetime format: ', data_radarsat_2['starttime'][0], '\n')
print('RADARSAT-CM SAR datetime format: ', data_radarsat_cm['starttime'][0], '\n')

# Sentinel series dates are alright...
# RADARSAT series dates are alright... 
# ...convert LANDSAT-8 datetime to the same format
landsat_datetime_fmt = '%Y:%j:%H:%M:%S.%f'

data_landsat_oli['starttime_tmp'] = ""   # need to create the field... 
data_landsat_oli['starttime_tmp'] = data_landsat_oli.apply(lambda x: x['starttime'][:-1], axis = 1)   # need to strip the last digit for .strptime to recognize it as a date
for i in range(len(data_landsat_oli)):
    data_landsat_oli['starttime_tmp'][i] = datetime.strptime(data_landsat_oli['starttime_tmp'][i], landsat_datetime_fmt)
    data_landsat_oli['starttime'][i] = data_landsat_oli['starttime_tmp'][i].strftime('%Y-%m-%d %H:%M:%S.%f')


# delete unnecessary columns
data_landsat_oli = data_landsat_oli.drop('starttime_tmp', axis=1)


# combine the data
data_sum = pd.concat([data_s1_sar, data_s2_msi, data_s3_olci, data_landsat_oli, data_radarsat_1, data_radarsat_2, data_radarsat_cm], ignore_index=True)


### CLIP DATA to relevant timeline
data_sum['starttime_tmp'] = pd.to_datetime(data_sum['starttime'], format='%Y-%m-%d %H:%M:%S.%f')
data_sum = data_sum[(data_sum['starttime_tmp'] > '2021-04-30') & (data_sum['starttime_tmp'] < '2021-12-01')]
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

# EDIT CLOUD COVER INFORMATION... 
### only keep cloud cover from LANDSAT and Sentinel-2
### Scenes from Sentinel-3 are too large, and the other missions don't have cloud cover
### convert any irrelevant cloud cover data to -1 so that I can update it to NaN in arcGIS after importing
data_sum['cloudcover'] = data_sum['cloudcover'].fillna(-1)


# ouput as shapefile
data_sum.to_file(outfile)



###
### OUTPUT ONLY DATA WITH CLOUD COVER INFO ###
###

### also calculate the area of each footprint & use it as a filter so that the data are relevant to the specific point that users select 
# reproject to projected CRS

data_sum_cloud = data_sum.to_crs("ESRI:102009")
data_sum_cloud = data_sum_cloud[~data_sum_cloud['cloudcover'].isnull()] # eliminate records that don't have cloud information
data_sum_cloud['area_km2'] = data_sum_cloud.area/10**6
data_sum_cloud = data_sum_cloud[data_sum_cloud['area_km2'].notna()].reset_index(drop=True) # eliminate records that don't have area information

data_sum_cloud_platforms = list(data_sum_cloud['mission'].unique())
for i in range(len(data_sum_cloud_platforms)):
    data_sum_cloud_sub = data_sum_cloud[data_sum_cloud['mission']==data_sum_cloud_platforms[i]]
    mmax = max(data_sum_cloud_sub['area_km2'])
    mmin = min(data_sum_cloud_sub['area_km2'])
    mmean = statistics.mean(data_sum_cloud_sub['area_km2'])
    print(data_sum_cloud_platforms[i], ':   MAX: ', mmax, ',    MIN: ', mmin,    ',    MEAN: ', mmean)


# delete Sentinel 3 data because the minimum area for a scene is 1,399,677 km2.... which is big
data_sum_cloud = data_sum_cloud[data_sum_cloud['mission'] != 'Sentinel-3'].reset_index(drop=True)


# delete any other information unique to missions other than Sentinel-2 and LANDSAT-8
data_sum_cloud = data_sum_cloud.drop(['passnumAbs', 'passnumRel', 'beammode', 'polarisati', 'angle_inc', 'featureId', 'lookOrient', 'recordId', 'spatialRes', 'thisRecord', 'sensormode', 'beam'], axis=1)


# check out histogram of scene sizes... 
import matplotlib.pyplot as plt
data_sum_cloud_s2 = data_sum_cloud[data_sum_cloud['mission'] == 'Sentinel-2']
plt.hist(data_sum_cloud_s2['area_km2'])
plt.show()


outfile_cloud = root + 'data_summary/data_algalbloom_summary_2019-May-Nov_' + str(date.today()) + '_cloud.shp'
data_sum_cloud.to_file(outfile_cloud)



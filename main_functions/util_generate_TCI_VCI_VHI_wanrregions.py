import ee
import os
import argparse
import numpy as np
from datetime import datetime, timezone
from pydrive.auth import GoogleAuth
import json
import time
import subprocess
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import storage
import re
import pandas as pd



def determine_run_type():
    """
    Determines the run type based on the existence of the SECRET on the local machine file.

    If the file `config.GDRIVE_SECRETS` exists, sets the run type to 2 (DEV) and prints a corresponding message.
    Otherwise, sets the run type to 1 (PROD) and prints a corresponding message.
    """
    global run_type
    # Set scopes for Google Drive
    scopes = ["https://www.googleapis.com/auth/drive"]

    if os.path.exists(config_GDRIVE_SECRETS):
        run_type = 2

        # Read the service account key file
        with open(config_GDRIVE_SECRETS, "r") as f:
            data = json.load(f)

        # Authenticate with Google using the service account key file
        gauth = GoogleAuth()
        gauth.service_account_file = os.path.join(
            "secrets", "geetest-credentials.secret")
        gauth.service_account_email = data["client_email"]
        print("\nType 2 run PROCESSOR: We are on a local machine")
    else:
        run_type = 1
        gauth = GoogleAuth()
        google_client_secret = os.environ.get('GOOGLE_CLIENT_SECRET')
        google_client_secret = json.loads(google_client_secret)
        gauth.service_account_email = google_client_secret["client_email"]
        google_client_secret_str = json.dumps(google_client_secret)

        # Write the JSON string to a temporary key file
        gauth.service_account_file = "keyfile.json"
        with open(gauth.service_account_file, "w") as f:
            f.write(google_client_secret_str)

        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            gauth.service_account_file, scopes=scopes
        )
        print("\nType 1 run PROCESSOR: We are on GitHub")

    # Test if GEE initialization is successful
    # Initialize Google Earth Engine
    credentials = ee.ServiceAccountCredentials(
        gauth.service_account_email, gauth.service_account_file
    )
    ee.Initialize(credentials)

    image = ee.Image("NASA/NASADEM_HGT/001")
    title = image.get("title").getInfo()

    if title == "NASADEM: NASA NASADEM Digital Elevation 30m":
        print("GEE initialization successful")
    else:
        print("GEE initialization FAILED")

    # Initialize GCS
    global storage_client
    storage_client = storage.Client.from_service_account_json(
        gauth.service_account_file)

def func_cxv(feature):
  region_nr = feature.get('REGION_NR')

  # Get VHI value
  vhiFeature = regionMeans.filter(ee.Filter.eq('REGION_NR', region_nr)).first()
  vhi = vhiFeature.get('mean_VHI')

  # Get TCI value
  tciFeature = TCIregionMeans.filter(ee.Filter.eq('REGION_NR', region_nr)).first()
  tci = tciFeature.get('mean_TCI')

  # Get VCI value
  vciFeature = VCIregionMeans.filter(ee.Filter.eq('REGION_NR', region_nr)).first()
  vci = vciFeature.get('mean_VCI')

  # Get NDVI value
  ndviFeature = NDVIjregionMeans.filter(ee.Filter.eq('REGION_NR', region_nr)).first()
  ndvi = ndviFeature.get('mean_NDVIj')

  # Create a new feature with all properties
  return ee.Feature(None, {
    'DATE': dateString,
    'REGION_NR': region_nr,
    'VHI': ee.Number(vhi).round(),
    'TCI': ee.Number(tci).round(),
    'VCI': ee.Number(vci).round(),
    'NDVI': ee.Number(ndvi).multiply(100).round().divide(100)
  })

def NDVIjcalculateRegionMean(region_nr):
  region = warnregions.filter(ee.Filter.eq('REGION_NR', region_nr)).first()
  NDVIj_masked = NDVIj.updateMask(NDVIj.neq(noDataValue)).updateMask(vegtype_mask)

  NDVIjstats = NDVIj_masked.reduceRegion(
    reducer=ee.Reducer.mean().combine(ee.Reducer.count(), '', True),
    geometry=region.geometry(),
    scale= 10,
    maxPixels= 1e10
  )

  NDVIjmeanValue = ee.Number(NDVIjstats.get('ndvi_mean'))
  NDVIjpixelCount = ee.Number(NDVIjstats.get('ndvi_count'))

  # Assign a value of 120 if there are no valid pixels
  return ee.Feature(None, {
    'REGION_NR': region_nr,
    'mean_NDVIj': ee.Algorithms.If(
      NDVIjpixelCount.eq(0), # Check if pixel count is zero
      120,                  # Assign 120 if no valid pixels
      NDVIjmeanValue        # Otherwise, use the calculated mean value
    ),
    'pixel_count': NDVIjpixelCount
  })

# Function to calculate mean VHI for a region
def VCIcalculateRegionMean(region_nr):
  region = warnregions.filter(ee.Filter.eq('REGION_NR', region_nr)).first()
  VCI_masked = VCI.updateMask(VCI.neq(noDataValue)).updateMask(vegtype_mask)

  VCIstats = VCI_masked.reduceRegion(
    reducer=ee.Reducer.mean().combine(ee.Reducer.count(), '', True),
    geometry=region.geometry(),
    scale=10,
    maxPixels= 1e10
  )

  VCImeanValue = ee.Number(VCIstats.get('vci_mean'))
  VCIpixelCount = ee.Number(VCIstats.get('vci_count'))


  # Assign a value of 120 if there are no valid pixels
  return ee.Feature(None, {
    'REGION_NR': region_nr,
    'mean_VCI': ee.Algorithms.If(
      VCIpixelCount.eq(0), # Check if pixel count is zero
      120,                  # Assign 120 if no valid pixels
      VCImeanValue        # Otherwise, use the calculated mean value
    ),
    'pixel_count': VCIpixelCount
  })

# Function to calculate mean VHI for a region
def TCIcalculateRegionMean(region_nr):
  region = warnregions.filter(ee.Filter.eq('REGION_NR', region_nr)).first()
  TCI_masked = TCI.updateMask(TCI.neq(noDataValue)).updateMask(vegtype_mask)

  TCIstats = TCI_masked.reduceRegion(
    reducer=ee.Reducer.mean().combine(
        reducer2=ee.Reducer.count(),
        sharedInputs=True
    ),
    geometry=region.geometry(),
    scale=10,
    maxPixels=1e10
)

  TCImeanValue = ee.Number(TCIstats.get('tci_mean'))
  TCIpixelCount = ee.Number(TCIstats.get('tci_count'))

  # Assign a value of 120 if there are no valid pixels
  return ee.Feature(None, {
    'REGION_NR': region_nr,
    'mean_TCI': ee.Algorithms.If(
      TCIpixelCount.eq(0), # Check if pixel count is zero
      120,                  # Assign 120 if no valid pixels
      TCImeanValue        # Otherwise, use the calculated mean value
    ),
    'pixel_count': TCIpixelCount
  })

# Function to calculate mean VHI for a region
def calculateRegionMean(region_nr):
  region = warnregions.filter(ee.Filter.eq('REGION_NR', region_nr)).first()
  VHI_masked = VHI.updateMask(VHI.neq(noDataValue)).updateMask(vegtype_mask)

  stats = VHI_masked.reduceRegion(
    reducer=ee.Reducer.mean().combine(
        reducer2=ee.Reducer.count(),
        sharedInputs=True
    ),
    geometry=region.geometry(),
    scale=10,
    maxPixels=1e10
  )

  meanValue = ee.Number(stats.get('vhi_mean'))
  pixelCount = ee.Number(stats.get('vhi_count'))

  # Assign a value of 120 if there are no valid pixels
  return ee.Feature(None, {
    'REGION_NR': region_nr,
    'mean_VHI': ee.Algorithms.If(
      pixelCount.eq(0), # Check if pixel count is zero
      120,                  # Assign 120 if no valid pixels
      meanValue        # Otherwise, use the calculated mean value
    ),
    'pixel_count': pixelCount
  })

def loadLstRefData(doy):
  doy3 = ee.String(ee.Number(doy).format('%03d')).getInfo(); # 1 -> 001
  asset_name = 'projects/satromo-prod/assets/col/1991-2020_LST_SWISS/LST_Stats_DOY' + doy3
  LSTref = ee.Image(asset_name)
  # back to float
  LSTref = LSTref.float()
  # Get scale value
  scale = ee.Number(LSTref.get('scale'))
  # Divide by the scale
  LSTref = LSTref.divide(scale)
  return LSTref



def loadLstCurrentData(date, d, aoi):
  start_date = date.advance((-1*d), 'day')
  end_date = date.advance(1, 'day')
  LST_col = ee.ImageCollection("projects/satromo-prod/assets/col/LST_SWISS") \
                .filterDate(start_date, end_date) \
                .filterBounds(aoi)
  # Sort the collection by time in descending order
  sortedCollection = LST_col.sort('system:time_start', False)
  # Create a mosaic using the latest pixel values
  latestMosaic = sortedCollection.mosaic()
  # Calculate NDVI for the mosaic
  LSTj = latestMosaic.select('LST_PMW').rename('lst')
  LSTj = LSTj.divide(100)
  return LSTj


# Creates a color bar thumbnail image for use in legend from the given color palette
def makeColorBarParams(palette):
  return {
    'bbox': [0, 0, 1, 0.1],
    'dimensions': '100x10',
    format: 'png',
    'min': 0,
    'max': 1,
    'palette': palette,
  }

def loadNdviCurrentData(date, d, aoi):
    start_date = date.advance((-1*d), 'day')
    end_date = date.advance(1, 'day')
    S2_col = ee.ImageCollection('projects/satromo-prod/assets/col/S2_SR_HARMONIZED_SWISS') \
                .filterDate(start_date, end_date) \
                .filterBounds(aoi) \
                .filter(ee.Filter.stringEndsWith('system:index', '10m'))
    # Apply the cloud and terrain shadow mask within the S2 image collection

    def func_roy(image):
        image = image.updateMask(image.select('terrainShadowMask').lt(65))
        image = image.updateMask(image.select('cloudAndCloudShadowMask').eq(0))
        return image

    S2_col_masked = S2_col.map(func_roy)

      # Sort the collection by time in descending order
    sortedCollection = S2_col_masked.sort('system:time_start', False)
    # Create a mosaic using the latest pixel values
    latestMosaic = sortedCollection.mosaic()
    # Calculate NDVI for the mosaic
    NDVIj = latestMosaic.normalizedDifference(['B8', 'B4']).rename('ndvi')
    return NDVIj

# FUNCTIONS
def loadNdviRefData(doy):
    doy3 = ee.String(ee.Number(doy).format('%03d')).getInfo(); # 1 -> 001
    asset_name = 'projects/satromo-prod/assets/col/1991-2020_NDVI_SWISS/NDVI_Stats_DOY' + doy3
    NDVIref = ee.Image(asset_name)
    # back to float
    NDVIref = NDVIref.float()
    # Get offset and scale values
    offset = ee.Number(NDVIref.get('offset'))
    scale = ee.Number(NDVIref.get('scale'))
    # Create an image with a constant value equal to the offset
    offsetImage = ee.Image.constant(offset)
    # Subtract the offset, then divide by the scale
    NDVIref = NDVIref.subtract(offsetImage).divide(scale)
    return NDVIref

if __name__ == "__main__":
    global config_GDRIVE_SECRETS
    config_GDRIVE_SECRETS = r'C:\temp\topo-satromo\secrets\geetest-credentials-int.secret'

    # Authenticate with GEE and GDRIVE
    determine_run_type()

    # SWITCHES
    # The switches enable / disable the execution of individual steps in this script
    applyForestMask = True        # options': True, False - defines if the VHI should be masked for only forests, or for all vegetation (False)
    workWithPercentiles = True     # options': True, False - defines if the p05 and p95 percentiles of the reference data sets are used, otherwise the min and max will be used (False)

    # TIME
    date = ee.Date('2018-08-12T23:59:59')
    d = 7; # window [days] before the date in which to search for current data
    # get day of year, add 1 because JS starts at 0.
    doy = (ee.Number(date.getRelative('day', 'year')).add(1).mod(365)).add(365).mod(365)
    # print('DOY: ', doy)

    ###########################/
    # PARAMETERS
    alpha = 0.5
    collection = 'swissEO_VHI'

    if workWithPercentiles == True:
        CI_method = '5th_and_95th_percentile'
    else :
        CI_method = 'min_and_max'


    ###########################/
    # SPACE
    # Official swisstopo boundaries
    # source: https:#www.swisstopo.admin.ch/de/geodata/landscape/boundaries3d.html#download
    # processing: layer Landesgebiet dissolved  in QGIS and reprojected to epsg32632
    aoi_CH = ee.FeatureCollection("projects/satromo-prod/assets/col/S2_SR_HARMONIZED_SWISS").geometry()

    # Simplified and buffered shapefile of Switzerland to simplify processing
    aoi_CH_simplified = ee.FeatureCollection("projects/satromo-prod/assets/res/CH_boundaries_buffer_5000m_epsg32632").geometry()
    # clipping on complex shapefiles costs more processing resources and can cause memory issues

    aoi = aoi_CH_simplified

    ###########################/
    # MASKS
    # Mask for vegetation
    vegetation_mask = ee.Image('projects/satromo-prod/assets/res/ch_bafu_lebensraumkarte_mask_vegetation_epsg32632')

    # Loaf Warnregions

    warnregions = ee.FeatureCollection("projects/satromo-prod/assets/res/warnregionen_vhi_2056")

    # Mask for the forest
    forest_mask = ee.Image('projects/satromo-prod/assets/res/ch_bafu_lebensraumkarte_mask_forest_epsg32632')

    if applyForestMask == True:
        mask_name = 'forest'
        vegtype_mask = forest_mask
    else:
        mask_name = 'vegetation'
        vegtype_mask = vegetation_mask



    ###########################################
    # PROCESSING
    NDVIref = loadNdviRefData(doy)
    # print('NDVI Reference Data (1991-2020)', NDVIref)

    NDVIj = loadNdviCurrentData(date, d, aoi)
    noDataValue = 110
    NDVIj= NDVIj.unmask(noDataValue)
    # mask for the vegetation type (forest, all vegetation)
    NDVIj = NDVIj.updateMask(vegtype_mask)
    NDVIj = NDVIj.updateMask(NDVIj.neq(noDataValue))
    # print('NDVI (specified date)', NDVIj)

    # calculate VCI --------
    if workWithPercentiles == True:
        VCI = NDVIj.subtract(NDVIref.select('p05')).divide(NDVIref.select('p95').subtract(NDVIref.select('p05'))).multiply(100).rename('vci')
        print('--- VCI calculated (with 5th and 95th percentile reference values) ---')
    else:
        VCI = NDVIj.subtract(NDVIref.select('min')).divide(NDVIref.select('max').subtract(NDVIref.select('min'))).multiply(100).rename('vci')
        print('--- VCI calculated (with min and max reference values) ---')

    noDataValue = 110
    VCI = VCI.unmask(noDataValue)
    # mask for the vegetation type (forest, all vegetation)
    VCI = VCI.updateMask(vegtype_mask)
    VCI = VCI.updateMask(VCI.neq(noDataValue))
    # print('VCI', VCI)

    LSTref = loadLstRefData(doy)
    # print('LST Reference Data (2012-2020)', LSTref)

    LSTj = loadLstCurrentData(date, d, aoi)
    # print('LST (specified date)', LSTj)

    # calculate TCI --------
    if workWithPercentiles == True:
        TCI = LSTj.subtract(LSTref.select('p05')).divide(LSTref.select('p95').subtract(LSTref.select('p05'))).multiply(100).rename('tci')
        print('--- TCI calculated (with 5th and 95th percentile reference values) ---')
    else:
        TCI = LSTj.subtract(LSTref.select('min')).divide(LSTref.select('max').subtract(LSTref.select('min'))).multiply(100).rename('tci')
        print('--- TCI calculated (with min and max reference values) ---')

    noDataValue = 110
    TCI = TCI.unmask(noDataValue)
    # mask for the vegetation type (forest, all vegetation)
    TCI = TCI.updateMask(vegtype_mask)
    TCI = TCI.updateMask(TCI.neq(noDataValue))
    # print('TCI', TCI)


    # calculate VHI --------
    VHI = VCI.multiply(alpha).add(TCI.multiply(1-alpha)).rename('vhi')
    print('--- VHI calculated ---')

    # converting the data type (to UINT8) and force data range (to [0 100])
    VHI = VHI.uint8().clamp(0, 100)

    # add no data value for when one of the datasets is unavailable
    noDataValue = 110
    VHI = VHI.unmask(noDataValue)
    # mask for the vegetation type (forest, all vegetation)
    VHI = VHI.updateMask(vegtype_mask)


    #VHI
    ###################################

    # Get distinct region numbers
    regions = warnregions.aggregate_array('REGION_NR').distinct()

    # Map over all regions to calculate mean VHI
    regionMeans = ee.FeatureCollection(regions.map(calculateRegionMean))
    # Get the first 5 entries of the regionMeans collection


    #TCI
    ###################################

    # Get distinct region numbers
    regions = warnregions.aggregate_array('REGION_NR').distinct()

    # Map over all regions to calculate mean VHI
    TCIregionMeans = ee.FeatureCollection(regions.map(TCIcalculateRegionMean))

    #VCI
    ###################################

    # Get distinct region numbers
    regions = warnregions.aggregate_array('REGION_NR').distinct()

    # Map over all regions to calculate mean VHI
    VCIregionMeans = ee.FeatureCollection(regions.map(VCIcalculateRegionMean))

    #NDVI

    ###################################

    # Get distinct region numbers
    regions = warnregions.aggregate_array('REGION_NR').distinct()

    # Function to calculate mean VHI for a region

    # Map over all regions to calculate mean VHI
    NDVIjregionMeans = ee.FeatureCollection(regions.map(NDVIjcalculateRegionMean))



    # EXPORT
    # Combine all results into a single feature collection
    # Get the date string outside of the mapping function
    dateString = date.format('YYYY-MM-dd').getInfo()

    # Combine all results into a single feature collection



    combinedResults = regionMeans.map(func_cxv)



    # Convert the entire combinedResults to a list of dictionaries
    combinedResults_list = combinedResults.getInfo()['features']

    # Extract the properties from each feature
    data = []
    for feature in combinedResults_list:
        properties = feature['properties']
        data.append([
            properties['DATE'],
            properties['REGION_NR'],
            properties['VHI'],
            properties['TCI'],
            properties['VCI'],
            properties['NDVI']
        ])

    # Create a DataFrame
    df = pd.DataFrame(data, columns=['DATE', 'REGION_NR', 'VHI', 'TCI', 'VCI', 'NDVI'])

    # Export to CSV
    df.to_csv('output.csv', index=False)

    print("CSV export completed.")


    breakpoint()
    print("done")

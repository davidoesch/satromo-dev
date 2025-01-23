import pystac_client
import rasterio
import geopandas as gpd
from pyproj import CRS,Transformer
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from shapely.geometry import Point
import pandas as pd
import os
from datetime import datetime
from tqdm import tqdm

def check_mask(lon, lat, DATETIME):
    # Search for items
    search = catalog.search(
        collections=["ch.swisstopo.swisseo_s2-sr_v100"],
        intersects={"type": "Point", "coordinates": [lon, lat]},
        datetime=DATETIME
    )

    items = list(search.items())
    if len(items) == 0:
        mask_value = 120
        print(f"Found no S2-SR for {DATETIME}")
        return mask_value


    # Checking now if no mask is aplied
    # Get the URL for the MASKS-10M asset
    masks_10m_key = next(key for key in items[0].assets.keys() if key.endswith('_masks-10m.tif'))
    masks_10m_url = items[0].assets[masks_10m_key].href

    #create x and y in EPSG:2056
    crs = CRS.from_epsg(4326)
    crs_lv95 = CRS.from_epsg(2056)

    transformer = Transformer.from_crs(crs, crs_lv95, always_xy=True)
    x, y = transformer.transform(lon, lat)

    # Read the bands
    with rasterio.open(masks_10m_url) as src:
        # Get pixel coordinates
        py, px = src.index(x, y)

        # Read the pixel values at mask 2, if 0, no mask is applied
        #
        mask_value = src.read(2, window=((py, py+1), (px, px+1)))[0, 0]
        print(f"Mask value: {mask_value}")

    return mask_value

def check_vhi(lon, lat, DATETIME):
    # Search for items
    search = catalog.search(
        collections=["ch.swisstopo.swisseo_vhi_v100"],
        intersects={"type": "Point", "coordinates": [lon, lat]},
        datetime=DATETIME
    )

    items = list(search.items())

    if len(items) == 0:
        vhi_value = 120
        print(f"Found no VHI for {DATETIME}")
        return vhi_value


    # Checking now if no mask is aplied
    # Get the URL for the BANDS-10M asset
    bands_10m_key = next(key for key in items[0].assets.keys() if key.endswith('_forest-10m.tif'))
    bands_10m_url = items[0].assets[bands_10m_key].href

        #create x and y in EPSG:2056
    crs = CRS.from_epsg(4326)
    crs_lv95 = CRS.from_epsg(2056)

    transformer = Transformer.from_crs(crs, crs_lv95, always_xy=True)
    x, y = transformer.transform(lon, lat)

    # Read the bands
    with rasterio.open(bands_10m_url) as src:
        # Get pixel coordinates
        py, px = src.index(x, y)

        # Read the pixel values
        #
        vhi_value = src.read(1, window=((py, py+1), (px, px+1)))[0, 0]
        print(f"VHI value: {vhi_value}")

    return vhi_value

def process_csv(input_file, output_file):
    # Normalize paths
    input_file = os.path.normpath(input_file)
    output_file = os.path.normpath(output_file)

    # Read CSV with encoding handling
    try:
        df = pd.read_csv(input_file, encoding='utf-8', low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(input_file, encoding='latin-1', low_memory=False)

    # Create DATETIME column
    df['DATETIME'] = pd.to_datetime(df['year'].astype(str) + '-' + df['doy'].astype(str).str.zfill(3), format='%Y-%j')
    df['DATETIME'] = df['DATETIME'].dt.strftime('%Y-%m-%d')

    # Add progress bar to column additions
    tqdm.pandas(desc="Processing data")

    # Add new columns with progress tracking
    df['swissEOVHI'] = df.progress_apply(lambda row: check_vhi(row['tree_xcor'], row['tree_ycor'], row['DATETIME']), axis=1)
    df['swissEOMASK'] = df.progress_apply(lambda row: check_mask(row['tree_xcor'], row['tree_ycor'], row['DATETIME']), axis=1)

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Save processed CSV
    df.to_csv(output_file, index=False)
    print(f"Processed CSV saved to {output_file}")

#add here the main section
if __name__ == "__main__":
    #define datetime="2021-08-01/2024-08-31"
    # DATETIME = "2017-06-19/2017-06-19"

    # # Define the coordinate in EPSG:4326
    # lon=7.57992972
    # lat=46.29625921




    # Connect to the STAC API
    catalog = pystac_client.Client.open("https://data.geo.admin.ch/api/stac/v0.9/")

    # Swisstopo finish : add the conformance classes :
    catalog.add_conforms_to("COLLECTIONS")
    catalog.add_conforms_to("ITEM_SEARCH")
    #for collection in catalog.get_collections():
    #    print(collection.id)

# Process years 2017-2024
for year in range(2017, 2024):
    input_file = fr'C:\temp\BAFU_TreeNet_Signals_2017_2024\TN_{year}.csv'
    output_file = fr'C:\temp\satromo-dev\output\TN_{year}_swisseo.csv'
    process_csv(input_file, output_file)






import pystac_client
import rasterio
import geopandas as gpd
from pyproj import CRS, Transformer
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from shapely.geometry import Point
import os
from datetime import datetime
from tqdm import tqdm
import time
import logging
from functools import wraps
import requests
from collections import defaultdict

def construct_url(datetime_str):
    base_url = "https://data.geo.admin.ch/ch.swisstopo.swisseo_vhi_v100/"
    timestamp = f"{datetime_str}t235959"
    file_name = f"ch.swisstopo.swisseo_vhi_v100_mosaic_{timestamp}_forest-10m.tif"
    full_url = f"{base_url}{timestamp}/{file_name}"
    return full_url

def retry_on_api_error(max_retries=3, delay=10):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except pystac_client.exceptions.APIError as e:
                    retries += 1
                    logging.warning(f"API Error (Attempt {retries}/{max_retries}): {str(e)}")
                    if retries == max_retries:
                        raise
                    time.sleep(delay)
        return wrapper
    return decorator

class VHIExtractor:
    def __init__(self, catalog):
        self.catalog = catalog
        self.mask_cache = {}  # Cache for mask URLs by date
        self.vhi_cache = {}   # Cache for VHI URLs by date
        self.transformer = Transformer.from_crs(CRS.from_epsg(4326), CRS.from_epsg(2056), always_xy=True)

    @retry_on_api_error(max_retries=20, delay=10)
    def get_mask_url(self, datetime_str):
        if datetime_str not in self.mask_cache:
            search = self.catalog.search(
                collections=["ch.swisstopo.swisseo_s2-sr_v100"],
                datetime=datetime_str
            )
            items = list(search.items())
            if len(items) == 0:
                self.mask_cache[datetime_str] = None
            else:
                masks_10m_key = next(key for key in items[0].assets.keys() if key.endswith('_masks-10m.tif'))
                self.mask_cache[datetime_str] = items[0].assets[masks_10m_key].href
        return self.mask_cache[datetime_str]

    def get_vhi_url(self, datetime_str):
        if datetime_str not in self.vhi_cache:
            url = construct_url(datetime_str)
            response = requests.head(url, timeout=10)
            self.vhi_cache[datetime_str] = url if response.status_code == 200 else None
        return self.vhi_cache[datetime_str]

    def process_coordinates(self, lon, lat):
        return self.transformer.transform(lon, lat)

    def check_mask(self, lon, lat, datetime_str, mask_url):
        if mask_url is None:
            return 120

        x, y = self.process_coordinates(lon, lat)
        with rasterio.open(mask_url) as src:
            py, px = src.index(x, y)
            mask_value = src.read(2, window=((py, py+1), (px, px+1)))[0, 0]
        return mask_value

    def check_vhi(self, lon, lat, datetime_str, vhi_url):
        if vhi_url is None:
            return 120

        x, y = self.process_coordinates(lon, lat)
        with rasterio.open(vhi_url) as src:
            py, px = src.index(x, y)
            vhi_value = src.read(1, window=((py, py+1), (px, px+1)))[0, 0]
        return vhi_value

def process_csv(input_file, output_file, extractor):
    # Normalize paths
    input_file = os.path.normpath(input_file)
    output_file = os.path.normpath(output_file)

    # Read CSV
    try:
        df = pd.read_csv(input_file, encoding='utf-8', low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(input_file, encoding='latin-1', low_memory=False)

    # Sort by DOY
    df = df.sort_values('doy')

    # Create DATETIME column
    df['DATETIME'] = pd.to_datetime(df['year'].astype(str) + '-' + df['doy'].astype(str).str.zfill(3), format='%Y-%j')
    df['DATETIME'] = df['DATETIME'].dt.strftime('%Y-%m-%d')

    # Initialize new columns
    df['swissEOVHI'] = None
    df['swissEOMASK'] = None

    # Process by unique dates to minimize API calls
    unique_dates = df['DATETIME'].unique()

    with tqdm(total=len(df), desc="Processing data") as pbar:
        for date in unique_dates:
            # Get URLs for this date once
            mask_url = extractor.get_mask_url(date)
            vhi_url = extractor.get_vhi_url(date)

            # Process all rows for this date
            date_mask = df['DATETIME'] == date
            date_df = df[date_mask]

            for idx, row in date_df.iterrows():
                df.at[idx, 'swissEOMASK'] = extractor.check_mask(
                    row['tree_xcor'], row['tree_ycor'], date, mask_url)
                df.at[idx, 'swissEOVHI'] = extractor.check_vhi(
                    row['tree_xcor'], row['tree_ycor'], date, vhi_url)
                pbar.update(1)

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Save processed CSV
    df.to_csv(output_file, index=False)
    print(f"Processed CSV saved to {output_file}")

if __name__ == "__main__":
    # Connect to the STAC API
    catalog = pystac_client.Client.open("https://data.geo.admin.ch/api/stac/v0.9/")
    catalog.add_conforms_to("COLLECTIONS")
    catalog.add_conforms_to("ITEM_SEARCH")

    # Create extractor instance
    extractor = VHIExtractor(catalog)

    # Process years 2017-2024
    for year in range(2018, 2024):
        input_file = fr'C:\temp\BAFU_TreeNet_Signals_2017_2024\TN_{year}.csv'
        output_file = fr'C:\temp\satromo-dev\output\TN_{year}_swisseo.csv'
        process_csv(input_file, output_file, extractor)

    # # Process specific year
    # year = "2022_test"
    # input_file = fr'C:\temp\BAFU_TreeNet_Signals_2017_2024\TN_{year}.csv'
    # output_file = fr'C:\temp\satromo-dev\output\TN_{year}_swisseo.csv'
    # process_csv(input_file, output_file, extractor)
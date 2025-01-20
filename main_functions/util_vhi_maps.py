import requests
import numpy as np
from datetime import datetime, timedelta
import os
from PIL import Image, ImageDraw, ImageFont
import concurrent.futures
from tqdm import tqdm
import logging
import json

class VHIMapGridGenerator:
    def __init__(self):
        self.years = range(2017, 2025)
        self.base_url = "https://data.geo.admin.ch/ch.swisstopo.swisseo_vhi_v100"
        self.map_size = (100, 100)  # Increased size for better polygon visibility
        self.max_workers = 4

        # Swiss bounding box (approx)
        self.bbox = {
            'min_lon': 5.9559,
            'max_lon': 10.4921,
            'min_lat': 45.8179,
            'max_lat': 47.8084
        }

        # VHI color scheme
        self.vhi_colors = {
            (0, 9): "#b56a29",    # Extrem trocken
            (10, 19): "#ce8540",  # Sehr trocken
            (20, 29): "#f5cd85",  # Trocken
            (30, 39): "#fff5ba",  # Leicht trocken
            (40, 49): "#cbffca",  # Normal
            (50, 59): "#52bd9f",  # Gut
            (60, 100): "#0470b0", # Exzellent
        }

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('map_generation.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def hex_to_rgb(self, hex_color):
        """Convert hex color to RGB."""
        hex_color = hex_color.lstrip('#')
        return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

    def get_color_for_vhi(self, vhi):
        """Get color for VHI value."""
        if vhi is None or vhi == 110:
            return (255, 255, 255)  # White for no data

        for (min_val, max_val), color in self.vhi_colors.items():
            if min_val <= vhi <= max_val:
                return self.hex_to_rgb(color)
        return (255, 255, 255)  # Default white

    def generate_dates(self):
        """Generate all dates for August across all years."""
        dates = []
        for year in self.years:
            for day in range(1, 32):
                dates.append(datetime(year, 8, day))
        return dates

    def convert_to_pixel_coords(self, lon, lat, width, height):
        """Convert geographic coordinates to pixel coordinates."""
        x = int((lon - self.bbox['min_lon']) / (self.bbox['max_lon'] - self.bbox['min_lon']) * width)
        y = int((self.bbox['max_lat'] - lat) / (self.bbox['max_lat'] - self.bbox['min_lat']) * height)
        return x, y

    def fetch_map_data(self, date, year):
        """Fetch map data for a specific date."""
        try:
            date_str = date.strftime("%Y-%m-%d")
            url = f"{self.base_url}/{date_str}t235959/ch.swisstopo.swisseo_vhi_v100_{date_str}t235959_vegetation-warnregions.geojson"

            response = requests.get(url)
            if response.status_code != 200:
                return None

            return response.json()

        except Exception as e:
            self.logger.error(f"Error fetching data for {date_str}: {str(e)}")
            return None

    def process_coordinates(self, coordinates):
        """Process and flatten GeoJSON coordinates into pixel coordinates."""
        pixel_coords = []
        try:
            # Handle different levels of nesting in GeoJSON coordinates
            if isinstance(coordinates[0][0], (int, float)):
                # Single coordinate pair
                lon, lat = coordinates
                return [self.convert_to_pixel_coords(lon, lat, self.map_size[0], self.map_size[1])]
            elif isinstance(coordinates[0][0][0], (int, float)):
                # Array of coordinate pairs
                for coord in coordinates[0]:
                    lon, lat = coord
                    pixel_coords.append(
                        self.convert_to_pixel_coords(lon, lat, self.map_size[0], self.map_size[1])
                    )
            else:
                # Multiple arrays of coordinate pairs
                for poly in coordinates:
                    for coord in poly[0]:
                        lon, lat = coord
                        pixel_coords.append(
                            self.convert_to_pixel_coords(lon, lat, self.map_size[0], self.map_size[1])
                        )
        except Exception as e:
            self.logger.error(f"Error processing coordinates: {str(e)}")
            return []

        return pixel_coords

    def create_map_image(self, geojson_data):
        """Create a single map image from GeoJSON data."""
        if not geojson_data or 'features' not in geojson_data:
            return None

        image = Image.new('RGB', self.map_size, (255, 255, 255))
        draw = ImageDraw.Draw(image)

        for feature in geojson_data['features']:
            if 'geometry' not in feature or 'properties' not in feature:
                continue

            vhi = feature['properties'].get('vhi_mean')
            availability = feature['properties'].get('availability_percentage')

            if vhi == 110 or availability < 20:
                continue

            coordinates = feature['geometry']['coordinates']
            color = self.get_color_for_vhi(vhi)

            pixel_coords = self.process_coordinates(coordinates)

            if len(pixel_coords) > 2:  # Need at least 3 points for a polygon
                draw.polygon(pixel_coords, fill=color, outline=(0, 0, 0))

        return image

    def create_grid_image(self, maps_data):
        """Create a grid image from all maps."""
        n_cols = len(self.years)  # 8 columns for years
        n_rows = 31  # 31 days of August

        total_width = n_cols * self.map_size[0]
        total_height = n_rows * self.map_size[1]
        grid_image = Image.new('RGB', (total_width, total_height), 'white')

        # Add maps to grid
        for day in range(1, 32):
            for col, year in enumerate(self.years):
                current_date = datetime(year, 8, day)
                if (current_date, year) in maps_data:
                    geojson_data = maps_data[(current_date, year)]
                    map_image = self.create_map_image(geojson_data)
                    if map_image:
                        x = col * self.map_size[0]
                        y = (day - 1) * self.map_size[1]
                        grid_image.paste(map_image, (x, y))

        return grid_image

    def add_legend(self, image):
        """Add color legend and labels to the image."""
        legend_width = 200
        legend_height = 150
        legend = Image.new('RGB', (legend_width, legend_height), 'white')
        draw = ImageDraw.Draw(legend)

        try:
            font = ImageFont.truetype("arial.ttf", 10)
        except:
            font = ImageFont.load_default()

        # Add VHI legend
        y_offset = 10
        draw.text((10, y_offset - 10), "VHI Values:", fill='black', font=font)
        for (min_val, max_val), color in self.vhi_colors.items():
            rgb_color = self.hex_to_rgb(color)
            draw.rectangle([10, y_offset, 30, y_offset + 10], fill=rgb_color, outline='black')
            draw.text((40, y_offset), f"VHI {min_val}-{max_val}", fill='black', font=font)
            y_offset += 15

        # Add year labels at the bottom
        y_offset += 10
        draw.text((10, y_offset), "Years (columns):", fill='black', font=font)
        years_text = ", ".join(str(year) for year in self.years)
        draw.text((10, y_offset + 15), years_text, fill='black', font=font)

        # Paste legend in top-right corner
        image.paste(legend, (image.width - legend_width - 10, 10))

    def run(self):
        """Main execution method."""
        self.logger.info("Starting August VHI map grid generation...")

        os.makedirs("output", exist_ok=True)
        dates = self.generate_dates()

        # Fetch all map data
        maps_data = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_date = {
                executor.submit(self.fetch_map_data, date, date.year): (date, date.year)
                for date in dates
            }

            for future in tqdm(concurrent.futures.as_completed(future_to_date),
                             total=len(dates),
                             desc="Fetching August maps"):
                date, year = future_to_date[future]
                try:
                    geojson_data = future.result()
                    if geojson_data is not None:
                        maps_data[(date, year)] = geojson_data
                except Exception as e:
                    self.logger.error(f"Error processing {date}: {str(e)}")

        self.logger.info("Creating August grid image...")
        grid_image = self.create_grid_image(maps_data)

        self.logger.info("Adding legend...")
        self.add_legend(grid_image)

        # Save output
        output_filename = f"vhi_map_grid_august_polygons_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        grid_image.save(output_filename, optimize=True, quality=85)
        self.logger.info(f"Saved output as {output_filename}")

if __name__ == "__main__":
    generator = VHIMapGridGenerator()
    generator.run()
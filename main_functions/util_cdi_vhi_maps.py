import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import os

# File paths
shapefile_path = r"C:\temp\satromo-dev\assets\warnregionen_vhi_2056.shp"
csv_path = r"C:\temp\temp\CDI_VHI_warnregionen.csv"
output_folder = r"C:\temp\output_vhi_maps"

# VHI to HEX color mapping
vhi_ranges = [(0, 9), (10, 19), (20, 29), (30, 39), (40, 49), (50, 59), (60, 100), (110, 110)]
hex_colors = ['#b56a29', '#ce8540', '#f5cd85', '#fff5ba', '#cbffca', '#52bd9f', '#0470b0', '#b3b6b7']

# Create output folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

# Load data
regions = gpd.read_file(shapefile_path)
data = pd.read_csv(csv_path, encoding='latin1')

# Ensure data types
data['Datum'] = pd.to_datetime(data['Datum'])
data['Year'] = data['Datum'].dt.year
data['Month'] = data['Datum'].dt.month

# Map VHI values to colors
def vhi_to_color(vhi):
    for (lower, upper), color in zip(vhi_ranges, hex_colors):
        if lower < vhi <= upper:
            return color
    return '#ffffff'  # Default white for out-of-range values

data['Color'] = data['VHI'].apply(vhi_to_color)

# Join shapefile with VHI data
regions = regions.rename(columns={'REGION_NR': 'Region_ID'})
regions['Region_ID'] = regions['Region_ID'].astype(int)
merged = regions.merge(data, on='Region_ID')

# Generate yearly maps with a 12x4 layout
for year in data['Year'].unique():
    yearly_data = merged[merged['Year'] == year]

    # Create a figure with 12 rows and 4 columns
    fig, axes = plt.subplots(nrows=12, ncols=4, figsize=(20, 30))
    axes = axes.flatten()  # Flatten for easy indexing

    # Loop through each month and add maps
    for month in range(1, 13):
        monthly_data = yearly_data[yearly_data['Month'] == month]
        dates_in_month = monthly_data['Datum'].dt.date.unique()

        # Loop through dates in the month, up to 4
        for col in range(4):
            ax_idx = (month - 1) * 4 + col  # Calculate position in grid
            ax = axes[ax_idx]

            if col < len(dates_in_month):
                date = dates_in_month[col]
                date_data = monthly_data[monthly_data['Datum'].dt.date == date]
                date_data.plot(ax=ax, color=date_data['Color'], edgecolor='black')
                ax.set_title(str(date), fontsize=8)  # Add date as title
            else:
                ax.axis('off')  # Hide empty cells

            ax.axis('off')  # Remove axes for clean look

    # Adjust spacing
    plt.tight_layout()
    plt.subplots_adjust(hspace=0.4)

    # Save the yearly figure
    plt.savefig(f"{output_folder}/VHI_{year}.png", dpi=300)
    plt.close()
    breakpoint()
    print(f"Saved VHI map for {year}")

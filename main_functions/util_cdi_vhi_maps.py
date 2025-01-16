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

# Get unique years and sort them
years = sorted(data['Year'].unique())

# Create a figure with one column for each year and 48 rows
fig, axes = plt.subplots(nrows=48, ncols=len(years), figsize=(5 * len(years), 100))
axes = axes.flatten()  # Flatten for easy indexing

# Loop through each year and add maps
for col, year in enumerate(years):
    yearly_data = merged[merged['Year'] == year]

    for month in range(1, 13):
        monthly_data = yearly_data[yearly_data['Month'] == month]
        dates_in_month = monthly_data['Datum'].dt.date.unique()

        for row in range(4):
            ax_idx = col + len(years) * ((month - 1) * 4 + row)
            ax = axes[ax_idx]

            if row < len(dates_in_month):
                date = dates_in_month[row]
                date_data = monthly_data[monthly_data['Datum'].dt.date == date]

                if not date_data.empty:
                    # Plot all regions with boundaries
                    regions.boundary.plot(ax=ax, linewidth=0.5, edgecolor='black')
                    # Plot regions with VHI data
                    date_data.plot(
                        ax=ax,
                        color=date_data.apply(lambda row: '#ffffff' if row['Availability'] <= 20 else row['Color'], axis=1),
                        edgecolor='black',
                    )
                    ax.set_title(f"{date}", fontsize=6)  # Add date as title
                else:
                    ax.axis('off')  # Hide empty cells if no data available
            else:
                ax.axis('off')  # Hide empty cells

            ax.axis('off')  # Remove axes for clean look

# Adjust spacing between plots
plt.tight_layout()
plt.subplots_adjust(hspace=0.5, wspace=0.3)

# Save the figure
output_path = os.path.join(output_folder, f"VHI_all_years.png")
plt.savefig(output_path, dpi=300)
plt.close()
print(f"Saved VHI map for all years at {output_path}")

import matplotlib.pyplot as plt
import geopandas as gpd
import contextily as ctx
from deltalake import DeltaTable
import numpy as np

# Load data from Delta tables
location_stats_df = DeltaTable("./delta_lake/location_stats").to_pandas()

# Load geographic data
geo_df = gpd.read_file('./analytics/shapefile.geojson')

# Merge location stats with geographic data
geo_df['location_id'] = geo_df['location_id'].astype(int)
merged_df = geo_df.merge(location_stats_df, on='location_id')

# Plotting the Geographic Heatmap for Location Stats
merged_df['log_num_pickups'] = merged_df['num_pickups'].replace(0, 1).apply(np.log)
ax = merged_df.plot(column='log_num_pickups',
                    cmap='plasma',
                    edgecolor='black',
                    legend=True,
                    legend_kwds={'label': "Number of Pickups"},
                    alpha=0.75,
                    linewidth=0.5)

# Adding a basemap for context
ctx.add_basemap(ax, crs=merged_df.crs.to_string(), source=ctx.providers.CartoDB.Positron)

# Adjusting plot settings
plt.title('NYC Heatmap of Pickups (Log scale)')
ax.axis('off')

# Show the plot
plt.show()

# Plotting the Geographic Heatmap for Location Stats
merged_df['log_num_dropoffs'] = merged_df['num_dropoffs'].replace(0, 1).apply(np.log)
ax = merged_df.plot(column='log_num_dropoffs',
                    cmap='plasma',
                    edgecolor='black',
                    legend=True,
                    legend_kwds={'label': "Number of Dropoffs"},
                    alpha=0.75,
                    linewidth=0.5)

# Adding a basemap for context
ctx.add_basemap(ax, crs=merged_df.crs.to_string(), source=ctx.providers.CartoDB.Positron)

# Adjusting plot settings
plt.title('NYC Heatmap of Dropoffs (Log scale)')
ax.axis('off')

# Show the plot
plt.show()

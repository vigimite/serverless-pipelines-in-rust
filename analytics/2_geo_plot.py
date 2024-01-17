import matplotlib.pyplot as plt
import geopandas as gpd
import contextily as ctx
import numpy as np
from deltalake import DeltaTable
from matplotlib.patches import Patch

# Load data from Delta tables
location_stats_df = DeltaTable("./delta_lake/location_stats").to_pandas()

# Load geographic data
geo_df = gpd.read_file('./analytics/shapefile.geojson')

# Merge location stats with geographic data
geo_df['location_id'] = geo_df['location_id'].astype(int)
merged_df = geo_df.merge(location_stats_df, on='location_id')

# Create a figure with two subplots side by side
fig, axes = plt.subplots(1, 2, figsize=(20, 8))

# Plotting the Geographic Heatmap for Number of Pickups
merged_df['log_num_pickups'] = merged_df['num_pickups'].replace(0, 1).apply(np.log)
ax1 = merged_df.plot(column='log_num_pickups',
                     cmap='plasma',
                     edgecolor='black',
                     alpha=0.75,
                     linewidth=0.5,
                     ax=axes[0])

# Adding a basemap for context
ctx.add_basemap(ax1, crs=merged_df.crs.to_string(), source=ctx.providers.CartoDB.Positron)

# Adjusting plot settings
ax1.set_title('NYC Heatmap of Pickups (Log scale applied)')
ax1.axis('off')

# Create custom legend for the first plot
legend_labels = [f"{int(np.exp(log_tick))}" for log_tick in np.linspace(merged_df['log_num_pickups'].min(), merged_df['log_num_pickups'].max(), num=6)]
legend_elements = [Patch(facecolor=plt.cm.plasma(i/5), edgecolor='black', label=label) for i, label in enumerate(legend_labels)]
ax1.legend(handles=legend_elements, title="Number of Pickups", loc='upper left')

# Plotting the Geographic Heatmap for Number of Dropoffs
merged_df['log_num_dropoffs'] = merged_df['num_dropoffs'].replace(0, 1).apply(np.log)
ax2 = merged_df.plot(column='log_num_dropoffs',
                     cmap='plasma',
                     edgecolor='black',
                     alpha=0.75,
                     linewidth=0.5,
                     ax=axes[1])

# Adding a basemap for context
ctx.add_basemap(ax2, crs=merged_df.crs.to_string(), source=ctx.providers.CartoDB.Positron)

# Adjusting plot settings
ax2.set_title('NYC Heatmap of Dropoffs (Log scale applied)')
ax2.axis('off')

# Create custom legend for the second plot
legend_labels = [f"{int(np.exp(log_tick))}" for log_tick in np.linspace(merged_df['log_num_dropoffs'].min(), merged_df['log_num_dropoffs'].max(), num=6)]
legend_elements = [Patch(facecolor=plt.cm.plasma(i/5), edgecolor='black', label=label) for i, label in enumerate(legend_labels)]
ax2.legend(handles=legend_elements, title="Number of Dropoffs", loc='upper left')

# Show the plot
plt.tight_layout()
plt.show()

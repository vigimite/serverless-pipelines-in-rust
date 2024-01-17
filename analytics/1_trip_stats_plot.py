import matplotlib.pyplot as plt
import seaborn as sns
from deltalake import DeltaTable
from matplotlib.dates import DateFormatter

trip_stats_df = DeltaTable("./delta_lake/trip_stats").to_pandas()

plt.figure(figsize=(20, 10))

sns.lineplot(data=trip_stats_df, x='month_start', y='avg_passengers', marker='o', label='Average # of Passengers')
sns.lineplot(data=trip_stats_df, x='month_start', y='avg_trip_distance_miles', marker='o', label='Average Trip Distance (Miles)')
sns.lineplot(data=trip_stats_df, x='month_start', y='avg_trip_duration_minutes', marker='o', label='Average Trip Duration (Minutes)')
sns.lineplot(data=trip_stats_df, x='month_start', y='avg_amount', marker='o', label='Average $ Amount')
sns.lineplot(data=trip_stats_df, x='month_start', y='avg_tip', marker='o', label='Average $ Tip')
plt.title('Trip Stats Over Time')
plt.ylabel('Values')
plt.xlabel('Month Start')
plt.xticks(rotation=45)
plt.gca().xaxis.set_major_formatter(DateFormatter('%Y-%m'))

# Adjust layout
plt.tight_layout()

# Show the plot
plt.show()

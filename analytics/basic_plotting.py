import pandas as pd
import matplotlib.pyplot as plt
from deltalake import DeltaTable

data = DeltaTable("../delta_lake/trip_stats").to_pandas()

# Convert 'date' to datetime and create a year-month column
data['date'] = pd.to_datetime(data['date'])
data['year_month'] = data['date'].dt.to_period('M')

# Group by year-month and aggregate
monthly_data = data.groupby('year_month').agg({
    'avg_trip_duration_m': 'mean',
    'avg_amount': 'mean',
    'avg_tip': 'mean',
    'trip_count': 'sum'  # Assuming you want the total count per year-month
}).reset_index()

# Create a figure and a set of subplots
fig, axs = plt.subplots(2, 1, figsize=(12, 12))

# Time Series Plot
axs[0].plot(monthly_data['year_month'].astype(str), monthly_data['avg_trip_duration_m'], label='Avg Trip Duration (min)', color='blue')
axs[0].plot(monthly_data['year_month'].astype(str), monthly_data['avg_amount'], label='Avg Amount ($)', color='green')
axs[0].plot(monthly_data['year_month'].astype(str), monthly_data['avg_tip'], label='Avg Tip ($)', color='red')
axs[0].set_xlabel('Year-Month')
axs[0].set_ylabel('Value')
axs[0].set_title('Monthly Average - Trip Duration, Amount, and Tip')
axs[0].legend()
axs[0].grid(True)
axs[0].tick_params(axis='x', rotation=45)

# Histogram for Monthly Trip Counts
axs[1].hist(monthly_data['trip_count'], bins=20, color='purple', edgecolor='black')
axs[1].set_xlabel('Monthly Trip Count')
axs[1].set_ylabel('Frequency')
axs[1].set_title('Histogram of Monthly Trip Counts')
axs[1].grid(True)

# Show the plots
plt.tight_layout()
plt.show()

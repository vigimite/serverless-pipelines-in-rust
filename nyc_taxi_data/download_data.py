import os
import re
import time
import pandas as pd
import requests
from urllib.parse import urlparse

# Define the input file containing the URLs
input_file = 'download_urls.txt'
pattern = r'yellow_tripdata_(\d{4})-(\d{2})\.parquet'

# Define the output folder where you want to save the files
output_folder = 'raw_data'

# Create the output folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

# Read the URLs from the input file
with open(input_file, 'r') as f:
	urls = f.readlines()

# Iterate through each URL and download the corresponding Parquet file
for url in urls:
	url = url.strip()
	try:
		response = requests.get(url, stream=True)
		if response.status_code == 200:
			# Extract the filename from the URL
			filename = os.path.basename(url)
		
			match = re.search(pattern, url)

			if match:
				year = match.group(1)
				month = match.group(2)

			# Create the folder structure if it doesn't exist
			year_month_folder = os.path.join(output_folder, year, month)
			os.makedirs(year_month_folder, exist_ok=True)

			# Save the Parquet file
			file_path = os.path.join(year_month_folder, filename)
			with open(file_path, 'wb') as file:
				for chunk in response.iter_content(chunk_size=8192):
					file.write(chunk)
			print(f"Downloaded: {url} to {file_path}")

			time.sleep(10)
		else:
			print(f"Failed to download: {url}, Status Code: {response.status_code}")
	except Exception as e:
		print(f"Error downloading {url}: {str(e)}")

# Serverless Pipelines in rust

This repository is part of a talk I am holding about writing serverless data pipelines in rust.

In this codebase we explore how to write a data pipeline using technologies like:

- rust
- datafusion
- object store
- delta lake
- cargo lambda

## Setup

To run this example you will have to download the sample dataset using the following commands:

```bash
# Install python code requirements
pip install -r requirements.txt

# Download some data from the NYC open data initiative, in this case data about yellowcab trips from 2014-2023
python nyc_taxi_data/download_data.py

# Build the project from root dir
cargo build --release

# Use the shell script to run the pipeline for all months
./invoke_pipeline.sh

# Check some analytics
python nyc_taxi_data/1_trip_stats_plot.py
python nyc_taxi_data/2_geo_plot.py
```

This will execute the pipeline for all dates between 2014-2023 and aggregate some metrics.

## Objectives

Compute the following values per month

1. avg trip duration
2. avg trip distance
3. avg amount of passengers
4. avg price & tip 

Compute the following location data

1. number of trips from a pick up location
2. number of trips to a destination


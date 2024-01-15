# Serverless Pipelines in rust

This repository is part of a talk I am holding about writing serverless data pipelines in rust.

In this codebase we explore how to write a data pipeline using technologies like:

- rust
- datafusion
- object store
- delta lake
- cargo lambda

To run this example you will have to download the sample dataset using the following commands:

```bash
# Install python code requirements
pip install -r requirements.txt

# Download some data from the NYC open data initiative, in this case data about yellowcab trips from 2014-2022
cd nyc_taxi_data
python download_data.py

# Build the project
cargo build --release

# Use the shell script to run the pipeline
./invoke_pipeline.sh

# Check some analytics
python ./analytics/basic_plotting.py
```

This will execute the pipeline for all dates between 2014-2022 and aggregate some metrics.

## Objectives

Optimization problem from the perspective of a taxi business

Which routes are the most valuable to take?

- corelate date, location, price, trip duration
- chunk day into sections
  - morning/noon/evening/late night

## Steps to solve

1. load all of the data
2. fill table with the aggregated metrics
3. resolve zone info
4. plot data

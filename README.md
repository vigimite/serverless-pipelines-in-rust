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
# Download some data from the NYC open data initiative, in this case data about yellowcab trips from 2014-2022
python ./nyc_taxi_data/download_data.py

# Build the project
cargo build --release

# use the shell script to run the pipeline
./invoke_pipeline.sh
```

This will execute the pipeline for all dates between 2014-2022 and aggregate some metrics.

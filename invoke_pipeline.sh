#!/bin/bash

BINARY_NAME="./target/release/serverless-pipelines-in-rust"

for year in {2014..2023}; do
    for month in {01..12}; do
        DATE="${year}-${month}-01"
        echo "----------------------"
        echo "Running for $DATE"
        time $BINARY_NAME $DATE
    done
done


#!/bin/bash

BINARY_NAME="./target/release/serverless-pipelines-in-rust"

for year in {2014..2022}; do
    for month in {01..12}; do
        DATE="${year}-${month}-01"
        echo "Running for $DATE"
        $BINARY_NAME $DATE
    done
done


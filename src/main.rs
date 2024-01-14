use chrono::NaiveDate;
use datafusion::{error::DataFusionError, prelude::*};
use deltalake::DeltaOps;
use serverless_pipelines_in_rust::*;

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let args: Vec<String> = std::env::args().collect();
    let input_date = NaiveDate::parse_from_str(&args[1], "%Y-%m-%d").expect("incorrect input date");

    // Load or create delta table called trips
    // Check trips_table_schema for schema definition
    let trips_table = get_delta_table(
        "./delta_lake/trips",
        trips_table_schema(),
        Some(vec!["year"]),
    )
    .await?;

    // Read input data
    // can be downloaded using `./nyc_taxi_data/download_data.py`
    let input_path = format!(
        "./nyc_taxi_data/raw_data/{year}/{month}/yellow_tripdata_{year}-{month}.parquet",
        year = input_date.format("%Y"),
        month = input_date.format("%m")
    );

    // Load data frame
    let ctx = SessionContext::new();
    // Workaround until https://github.com/apache/arrow-datafusion/pull/8854
    ctx.sql("set datafusion.execution.parquet.allow_single_file_parallelism = false")
        .await?;

    let df = ctx
        .read_parquet(input_path, ParquetReadOptions::default())
        .await?;

    // println!("\n--- Input Data ---");
    // df.clone().show_limit(10).await?;

    // Select the columns we need for processing
    let df = df
        .select(vec![
            col("tpep_pickup_datetime"),
            col("passenger_count"),
            col("total_amount"),
            col("tip_amount"),
        ])?
        .with_column(
            "date",
            cast(
                col("tpep_pickup_datetime"),
                datafusion::arrow::datatypes::DataType::Date32,
            ),
        )?
        .with_column(
            "year",
            cast(
                date_part(lit("year"), col("date")),
                datafusion::arrow::datatypes::DataType::Int32,
            ),
        )?;

    // println!("\n--- Raw dataset ---");
    // df.clone().show_limit(10).await?;

    // Filter values for input month only
    let filter_date = input_date.format("%Y-%m-%d").to_string();
    let df = df.filter(date_trunc(lit("MONTH"), col("date")).eq(cast(
        date_trunc(lit("MONTH"), lit(filter_date)),
        datafusion::arrow::datatypes::DataType::Date32,
    )))?;

    // Aggregate to get our desired report
    let result = df
        .aggregate(
            vec![col("year"), col("date")],
            vec![
                count(lit(1_i32)).alias("trip_count"),
                sum(col("passenger_count")).alias("num_passengers"),
                avg(col("total_amount")).alias("avg_amount"),
                avg(col("tip_amount")).alias("avg_tip"),
                median(col("total_amount")).alias("median_amount"),
                median(col("tip_amount")).alias("median_tip"),
            ],
        )?
        .sort(vec![col("date").sort(true, false)])?
        .cache()
        .await?;

    // println!("\n--- Result to be written ---");
    // result.clone().show_limit(10).await?;

    // Merge with existing table
    let _ = DeltaOps(trips_table)
        // Full outer-join with target (delta_lake) table on year and date
        .merge(
            result,
            col("target.year")
                .eq(col("source.year"))
                .and(col("target.date").eq(col("source.date"))),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        // Insert or update logic
        // more complex logic can be registered, order of composition matters here
        .when_not_matched_insert(|insert| {
            insert
                .set("year", col("source.year"))
                .set("date", col("source.date"))
                .set("trip_count", col("source.trip_count"))
                .set("num_passengers", col("source.num_passengers"))
                .set("avg_amount", col("source.avg_amount"))
                .set("avg_tip", col("source.avg_tip"))
                .set("median_amount", col("source.median_amount"))
                .set("median_tip", col("source.median_tip"))
        })?
        .when_matched_update(|update| {
            update
                .update("trip_count", col("source.trip_count"))
                .update("num_passengers", col("source.num_passengers"))
                .update("avg_amount", col("source.avg_amount"))
                .update("avg_tip", col("source.avg_tip"))
                .update("median_amount", col("source.median_amount"))
                .update("median_tip", col("source.median_tip"))
        })?
        .await?;

    Ok(())
}

use chrono::NaiveDate;
use datafusion::{arrow::datatypes::DataType, error::DataFusionError, prelude::*};
use deltalake::DeltaOps;
use serverless_pipelines_in_rust::*;

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let args = std::env::args().collect::<Vec<String>>();
    let input_date = NaiveDate::parse_from_str(&args[1], "%Y-%m-%d").expect("incorrect input date");

    // Load or create delta table called trips
    // Check trip_metrics_table_schema for schema definition
    let trips_table = get_delta_table(
        "./delta_lake/trip_stats",
        trip_stats_table_schema(),
        Some(vec!["year"]),
    )
    .await?;

    // Read input data
    // can be downloaded using `./nyc_taxi_data/download_data.py`
    let input_path = format!(
        "./nyc_taxi_data/raw_data/{year}/yellow_tripdata_{year}-{month}.parquet",
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
            col("tpep_dropoff_datetime"),
            col(r#""PULocationID""#).alias("pickup_location_id"),
            col(r#""DOLocationID""#).alias("dropoff_location_id"),
            col("trip_distance"),
            col("passenger_count"),
            col("total_amount"),
            col("tip_amount"),
        ])?
        .with_column("date", cast(col("tpep_pickup_datetime"), DataType::Date32))?
        .with_column(
            "year",
            cast(date_part(lit("year"), col("date")), DataType::Int32),
        )?
        .with_column(
            "hour",
            cast(
                date_part(lit("hour"), col("tpep_pickup_datetime")),
                DataType::Int32,
            ),
        )?
        .with_column(
            "trip_duration_m",
            cast(
                col("tpep_dropoff_datetime") - col("tpep_pickup_datetime"),
                DataType::Int64,
            ) / lit(1000000_i32)
                / lit(60_i32),
        )?
        .with_column(
            "time_of_day",
            when(col("hour").between(lit(5_i32), lit(12_i32)), lit("MORNING"))
                .when(
                    col("hour").between(lit(12_i32), lit(17_i32)),
                    lit("AFTERNOON"),
                )
                .when(
                    col("hour").between(lit(17_i32), lit(21_i32)),
                    lit("EVENING"),
                )
                .otherwise(lit("NIGHT"))?,
        )?;

    // println!("\n--- Raw dataset ---");
    // df.clone().show_limit(10).await?;

    // Filter values for input month only
    let filter_date = input_date.format("%Y-%m-%d").to_string();
    let df = df.filter(date_trunc(lit("MONTH"), col("date")).eq(cast(
        date_trunc(lit("MONTH"), lit(filter_date)),
        DataType::Date32,
    )))?;

    // Aggregate to get our desired report
    let result = df
        .aggregate(
            vec![
                col("year"),
                col("date"),
                col("time_of_day"),
                col("pickup_location_id"),
                col("dropoff_location_id"),
            ],
            vec![
                count(lit(1_i32)).alias("trip_count"),
                sum(coalesce(vec![col("passenger_count"), lit(0)])).alias("num_passengers"),
                avg(col("trip_duration_m")).alias("avg_trip_duration_m"),
                avg(col("total_amount")).alias("avg_amount"),
                avg(col("tip_amount")).alias("avg_tip"),
                min(col("total_amount")).alias("min_amount"),
                max(col("total_amount")).alias("max_amount"),
                min(col("tip_amount")).alias("min_tip"),
                max(col("tip_amount")).alias("max_tip"),
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
                .and(col("target.date").eq(col("source.date")))
                .and(col("target.time_of_day").eq(col("source.time_of_day")))
                .and(col("target.pickup_location_id").eq(col("source.pickup_location_id")))
                .and(col("target.dropoff_location_id").eq(col("source.dropoff_location_id"))),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        // Insert or update logic
        // more complex logic can be registered, order of composition matters here
        .when_not_matched_insert(|insert| {
            insert
                .set("year", col("source.year"))
                .set("date", col("source.date"))
                .set("time_of_day", col("source.time_of_day"))
                .set("pickup_location_id", col("source.pickup_location_id"))
                .set("dropoff_location_id", col("source.dropoff_location_id"))
                .set("trip_count", col("source.trip_count"))
                .set("num_passengers", col("source.num_passengers"))
                .set("avg_trip_duration_m", col("source.avg_trip_duration_m"))
                .set("avg_amount", col("source.avg_amount"))
                .set("avg_tip", col("source.avg_tip"))
                .set("min_amount", col("source.min_amount"))
                .set("max_amount", col("source.max_amount"))
                .set("min_tip", col("source.min_tip"))
                .set("max_tip", col("source.max_tip"))
        })?
        .when_matched_update(|update| {
            update
                .update("trip_count", col("source.trip_count"))
                .update("num_passengers", col("source.num_passengers"))
                .update("avg_trip_duration_m", col("source.avg_trip_duration_m"))
                .update("avg_amount", col("source.avg_amount"))
                .update("avg_tip", col("source.avg_tip"))
                .update("min_amount", col("source.min_amount"))
                .update("max_amount", col("source.max_amount"))
                .update("min_tip", col("source.min_tip"))
                .update("max_tip", col("source.max_tip"))
        })?
        .await?;

    Ok(())
}

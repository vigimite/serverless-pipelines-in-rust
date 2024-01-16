use chrono::NaiveDate;
use datafusion::{arrow::datatypes::DataType, error::DataFusionError, prelude::*};
use deltalake::{DeltaOps, DeltaTableError};
use serverless_pipelines_in_rust::*;

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let args = std::env::args().collect::<Vec<String>>();
    let input_date = NaiveDate::parse_from_str(&args[1], "%Y-%m-%d").expect("incorrect input date");

    // Read input data
    // can be downloaded using `./nyc_taxi_data/download_data.py`
    let input_path = format!(
        "./nyc_taxi_data/raw_data/{year}/yellow_tripdata_{year}-{month}.parquet",
        year = input_date.format("%Y"),
        month = input_date.format("%m")
    );

    // Load data frame
    let ctx = SessionContext::new();

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
            col("passenger_count").alias("passenger_count"),
            col("total_amount"),
            col("tip_amount"),
        ])?
        .with_column(
            "month_start",
            cast(
                date_trunc(lit("MONTH"), col("tpep_pickup_datetime")),
                DataType::Date32,
            ),
        )?
        .with_column(
            "year",
            cast(date_part(lit("year"), col("month_start")), DataType::Int32),
        )?
        .with_column(
            "trip_duration_minutes",
            cast(
                col("tpep_dropoff_datetime") - col("tpep_pickup_datetime"),
                DataType::Int64,
            ) / lit(1_000_000_i32)
                / lit(60_i32),
        )?;

    // println!("\n--- Raw dataset ---");
    // df.clone().show_limit(10).await?;

    // Filter values for input month only
    let filter_date = input_date.format("%Y-%m-%d").to_string();
    let df = df
        .filter(col("month_start").eq(cast(
            date_trunc(lit("MONTH"), lit(filter_date)),
            DataType::Date32,
        )))?
        .cache()
        .await?;

    // Aggregate to get trip data
    let trip_stats = df
        .clone()
        .aggregate(
            vec![col("year"), col("month_start")],
            vec![
                count(lit(1_i32)).alias("trip_count"),
                avg(col("passenger_count")).alias("avg_passengers"),
                avg(col("trip_distance")).alias("avg_trip_distance_miles"),
                avg(col("trip_duration_minutes")).alias("avg_trip_duration_minutes"),
                avg(col("total_amount")).alias("avg_amount"),
                avg(col("tip_amount")).alias("avg_tip"),
            ],
        )?
        .sort(vec![col("month_start").sort(true, false)])?
        .cache()
        .await?;

    // println!("\n--- Trip Stats ---");
    // trip_stats.clone().show_limit(10).await?;

    // Merge trip stats to table
    merge_trip_stats(trip_stats).await?;

    // Aggregate to get location data
    let pickup_location_data = df.clone().aggregate(
        vec![
            col("year"),
            col("month_start"),
            col("pickup_location_id").alias("location_id"),
        ],
        vec![count(lit(1_i32)).alias("num_pickups")],
    )?;

    let dropoff_location_data = df.clone().aggregate(
        vec![
            col("year").alias("y2"),
            col("month_start").alias("ms2"),
            col("dropoff_location_id").alias("lid2"),
        ],
        vec![count(lit(1_i32)).alias("num_dropoffs")],
    )?;

    let location_stats = pickup_location_data
        .join_on(
            dropoff_location_data,
            JoinType::Inner,
            [
                col("year").eq(col("y2")),
                col("month_start").eq(col("ms2")),
                col("location_id").eq(col("lid2")),
            ],
        )?
        .select_columns(&[
            "year",
            "month_start",
            "location_id",
            "num_pickups",
            "num_dropoffs",
        ])?
        .sort(vec![col("month_start").sort(true, false)])?;

    // println!("\n--- Location Stats ---");
    // location_stats.clone().show_limit(10).await?;

    // Merge location stats to table
    merge_location_stats(location_stats).await?;

    Ok(())
}

async fn merge_trip_stats(trip_stats: DataFrame) -> Result<(), DeltaTableError> {
    // Load or create delta table called trips_stats
    // Check trip_stats_table_schema for schema definition
    let trips_table = get_delta_table(
        "./delta_lake/trip_stats",
        trip_stats_table_schema(),
        Some(vec!["year"]),
    )
    .await?;

    // Merge with existing trips table
    let _ = DeltaOps(trips_table)
        // Full outer-join with target (delta_lake) table on year and month_start
        .merge(
            trip_stats,
            col("target.year")
                .eq(col("source.year"))
                .and(col("target.month_start").eq(col("source.month_start"))),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        // Insert or update logic
        // more complex logic can be registered, order of composition matters here
        .when_not_matched_insert(|insert| {
            insert
                .set("year", col("source.year"))
                .set("month_start", col("source.month_start"))
                .set("trip_count", col("source.trip_count"))
                .set("avg_passengers", col("source.avg_passengers"))
                .set(
                    "avg_trip_distance_miles",
                    col("source.avg_trip_distance_miles"),
                )
                .set(
                    "avg_trip_duration_minutes",
                    col("source.avg_trip_duration_minutes"),
                )
                .set("avg_amount", col("source.avg_amount"))
                .set("avg_tip", col("source.avg_tip"))
        })?
        .when_matched_update(|update| {
            update
                .update("trip_count", col("source.trip_count"))
                .update("avg_passengers", col("source.avg_passengers"))
                .update(
                    "avg_trip_distance_miles",
                    col("source.avg_trip_distance_miles"),
                )
                .update(
                    "avg_trip_duration_minutes",
                    col("source.avg_trip_duration_minutes"),
                )
                .update("avg_amount", col("source.avg_amount"))
                .update("avg_tip", col("source.avg_tip"))
        })?
        .await?;
    Ok(())
}

async fn merge_location_stats(location_stats: DataFrame) -> Result<(), DeltaTableError> {
    // Load or create delta table called location_stats
    // Check location_stats_table_schema for schema definition
    let locations_table = get_delta_table(
        "./delta_lake/location_stats",
        location_stats_table_schema(),
        Some(vec!["year"]),
    )
    .await?;

    // Merge with existing locations table
    let _ = DeltaOps(locations_table)
        .merge(
            location_stats,
            col("target.year").eq(col("source.year")).and(
                col("target.month_start")
                    .eq(col("source.month_start"))
                    .and(col("target.location_id").eq(col("source.location_id"))),
            ),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .when_not_matched_insert(|insert| {
            insert
                .set("year", col("source.year"))
                .set("month_start", col("source.month_start"))
                .set("location_id", col("source.location_id"))
                .set("num_pickups", col("source.num_pickups"))
                .set("num_dropoffs", col("source.num_dropoffs"))
        })?
        .when_matched_update(|update| {
            update
                .update("num_pickups", col("source.num_pickups"))
                .update("num_dropoffs", col("source.num_dropoffs"))
        })?
        .await?;
    Ok(())
}

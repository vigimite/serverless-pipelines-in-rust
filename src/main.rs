use chrono::NaiveDate;
use datafusion::error::DataFusionError;
use log::info;
use serverless_pipelines_in_rust::queries::process_month;

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    simple_logger::init_with_level(log::Level::Info).unwrap();

    let args = std::env::args().collect::<Vec<String>>();
    let input_date = NaiveDate::parse_from_str(&args[1], "%Y-%m-%d").expect("incorrect input date");

    info!("Invoked binary for month {}", input_date.to_string());

    process_month(input_date).await?;

    info!("Finished processing {}", input_date);

    Ok(())
}

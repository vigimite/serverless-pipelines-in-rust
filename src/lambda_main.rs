use chrono::NaiveDate;
use datafusion::error::DataFusionError;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use log::info;
use serverless_pipelines_in_rust::queries::process_month;

#[tokio::main]
async fn main() -> Result<(), Error> {
    simple_logger::init_with_level(log::Level::Info)?;

    // execute handler
    lambda_runtime::run(service_fn(lambda_handler)).await
}

async fn lambda_handler(event: LambdaEvent<String>) -> Result<(), DataFusionError> {
    let input_date =
        NaiveDate::parse_from_str(&event.payload, "%Y-%m-%d").expect("incorrect input date");

    info!("Invoked lambda for month {}", input_date.to_string());

    process_month(input_date).await?;

    info!("Finished processing {}", input_date);

    Ok(())
}

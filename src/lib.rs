use deltalake::{
    kernel::{DataType, PrimitiveType, StructField, StructType},
    DeltaOps, DeltaTable, DeltaTableError,
};

pub fn trip_stats_table_schema() -> StructType {
    StructType::new(vec![
        StructField::new("year", DataType::Primitive(PrimitiveType::String), false),
        StructField::new("date", DataType::Primitive(PrimitiveType::Date), false),
        StructField::new(
            "time_of_day",
            DataType::Primitive(PrimitiveType::String),
            false,
        ),
        StructField::new(
            "pickup_location_id",
            DataType::Primitive(PrimitiveType::Integer),
            false,
        ),
        StructField::new(
            "dropoff_location_id",
            DataType::Primitive(PrimitiveType::Integer),
            false,
        ),
        StructField::new(
            "trip_count",
            DataType::Primitive(PrimitiveType::Integer),
            false,
        ),
        StructField::new(
            "num_passengers",
            DataType::Primitive(PrimitiveType::Integer),
            false,
        ),
        StructField::new(
            "avg_trip_duration_m",
            DataType::Primitive(PrimitiveType::Float),
            false,
        ),
        StructField::new(
            "avg_amount",
            DataType::Primitive(PrimitiveType::Float),
            false,
        ),
        StructField::new("avg_tip", DataType::Primitive(PrimitiveType::Float), false),
        StructField::new(
            "min_amount",
            DataType::Primitive(PrimitiveType::Float),
            false,
        ),
        StructField::new(
            "max_amount",
            DataType::Primitive(PrimitiveType::Float),
            false,
        ),
        StructField::new("min_tip", DataType::Primitive(PrimitiveType::Float), false),
        StructField::new("max_tip", DataType::Primitive(PrimitiveType::Float), false),
    ])
}

pub async fn get_delta_table(
    path: &str,
    delta_schema: StructType,
    partitions: Option<Vec<&str>>,
) -> Result<DeltaTable, DeltaTableError> {
    let table = match deltalake::open_table(path).await {
        Ok(table) => table,
        Err(DeltaTableError::InvalidTableLocation(_)) => {
            DeltaOps::try_from_uri(path)
                .await?
                .create()
                .with_columns(delta_schema.fields().clone())
                .with_partition_columns(partitions.unwrap_or_default())
                .await?
        }
        _ => panic!("unexpected err while reading delta_table"),
    };

    Ok(table)
}

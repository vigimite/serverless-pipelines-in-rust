use deltalake::{
    kernel::{DataType, PrimitiveType, StructField, StructType},
    DeltaOps, DeltaTable, DeltaTableError,
};

pub mod queries;

pub fn trip_stats_table_schema() -> StructType {
    StructType::new(vec![
        StructField::new("year", DataType::Primitive(PrimitiveType::String), false),
        StructField::new(
            "month_start",
            DataType::Primitive(PrimitiveType::Date),
            false,
        ),
        StructField::new(
            "trip_count",
            DataType::Primitive(PrimitiveType::Integer),
            false,
        ),
        StructField::new(
            "avg_passengers",
            DataType::Primitive(PrimitiveType::Float),
            false,
        ),
        StructField::new(
            "avg_trip_distance_miles",
            DataType::Primitive(PrimitiveType::Float),
            false,
        ),
        StructField::new(
            "avg_trip_duration_minutes",
            DataType::Primitive(PrimitiveType::Float),
            false,
        ),
        StructField::new(
            "avg_amount",
            DataType::Primitive(PrimitiveType::Float),
            false,
        ),
        StructField::new("avg_tip", DataType::Primitive(PrimitiveType::Float), false),
    ])
}

pub fn location_stats_table_schema() -> StructType {
    StructType::new(vec![
        StructField::new("year", DataType::Primitive(PrimitiveType::String), false),
        StructField::new(
            "month_start",
            DataType::Primitive(PrimitiveType::Date),
            false,
        ),
        StructField::new(
            "location_id",
            DataType::Primitive(PrimitiveType::Integer),
            false,
        ),
        StructField::new(
            "num_pickups",
            DataType::Primitive(PrimitiveType::Integer),
            false,
        ),
        StructField::new(
            "num_dropoffs",
            DataType::Primitive(PrimitiveType::Integer),
            false,
        ),
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

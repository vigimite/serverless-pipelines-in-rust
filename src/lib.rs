use deltalake::{
    kernel::{DataType, PrimitiveType, StructField, StructType},
    DeltaOps, DeltaTable, DeltaTableError,
};

pub fn trips_table_schema() -> StructType {
    StructType::new(vec![
        StructField::new("year", DataType::Primitive(PrimitiveType::String), false),
        StructField::new("date", DataType::Primitive(PrimitiveType::Date), false),
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
            "avg_amount",
            DataType::Primitive(PrimitiveType::Float),
            false,
        ),
        StructField::new(
            "median_amount",
            DataType::Primitive(PrimitiveType::Float),
            false,
        ),
        StructField::new("avg_tip", DataType::Primitive(PrimitiveType::Float), false),
        StructField::new(
            "median_tip",
            DataType::Primitive(PrimitiveType::Float),
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

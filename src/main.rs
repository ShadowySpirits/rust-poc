use arrow_array::record_batch;
use futures::TryStreamExt;
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, NamespaceIdent, Result, TableCreation};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<()> {
    // Connect to a catalog.
    let config = RestCatalogConfig::builder()
        .uri(format!("http://{}:{}", "localhost", 8181))
        .props(HashMap::from([
            (
                S3_ENDPOINT.to_string(),
                format!("http://{}:{}", "localhost", 9000),
            ),
            (S3_ACCESS_KEY_ID.to_string(), "admin".to_string()),
            (S3_SECRET_ACCESS_KEY.to_string(), "password".to_string()),
            (S3_REGION.to_string(), "us-east-1".to_string()),
        ]))
        .build();
    let catalog = RestCatalog::new(config);

    // Create namespace
    let namespace = catalog
        .create_namespace(&NamespaceIdent::new("ns".to_string()), HashMap::new())
        .await?;

    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .with_schema_id(1)
        .with_identifier_field_ids(vec![2])
        .build()?;

    // Create table
    let table_creation = TableCreation::builder()
        .name("data".to_string())
        .schema(schema.clone())
        .build();

    let table = catalog
        .create_table(namespace.name(), table_creation)
        .await?;

    // Write data file
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
        table.file_io().clone(),
        DefaultLocationGenerator::new(table.metadata().clone())?,
        DefaultFileNameGenerator::new("prefix".to_string(), None, DataFileFormat::Parquet),
    );

    let mut writer = DataFileWriterBuilder::new(parquet_writer_builder, None)
        .build()
        .await?;

    let batch = record_batch!(
        ("id", Int32, [1]),
        ("name", Utf8, ["alpha"])
    )?;
    
    writer.write(batch).await?;
    let data_file = writer.close().await?;
    
    // Commit result
    let tx = Transaction::new(&table);
    let mut append_action = tx.fast_append(None, vec![])?;
    append_action.add_data_files(data_file.clone())?;
    let tx = append_action.apply().await?;
    let table = tx.commit(&catalog).await?;

    // Build table scan
    let stream = table
        .scan()
        .select(["name", "id"])
        .build()?
        .to_arrow()
        .await?;

    // Consume this stream like arrow record batch stream
    let data: Vec<_> = stream.try_collect().await?;
    data.as_slice().iter().for_each(|batch| {
        println!("{:?}", batch);
    });

    Ok(())
}

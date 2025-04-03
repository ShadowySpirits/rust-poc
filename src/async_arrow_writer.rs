use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::FutureExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct SizeAwareWriter<W> {
    inner: W,
    size: Arc<AtomicUsize>,
}

impl<W> SizeAwareWriter<W> {
    fn new(inner: W, size: Arc<AtomicUsize>) -> Self {
        Self {
            inner,
            size: size.clone(),
        }
    }
}

impl<W> AsyncFileWriter for SizeAwareWriter<W>
where
    W: Write + Send,
{
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        async move {
            self.size.fetch_add(bs.len(), Ordering::Relaxed);
            self.inner.write_all(&bs)?;
            Ok(())
        }
        .boxed()
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        async move { Ok(()) }.boxed()
    }
}

#[monoio::main]
async fn main() {
    let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
    let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();

    let mut buffer = Vec::new();
    let written_bytes = Arc::new(AtomicUsize::new(0));

    let properties = WriterProperties::builder()
        .set_max_row_group_size(2)
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut writer = AsyncArrowWriter::try_new(
        SizeAwareWriter::new(&mut buffer, written_bytes.clone()),
        to_write.schema(),
        Some(properties),
    )
    .unwrap();

    println!("Writing to parquet file for the first time");
    writer.write(&to_write).await.unwrap();
    println!("Written bytes: {}", written_bytes.load(Ordering::Relaxed));

    println!("Flushing parquet file");
    writer.flush().await.unwrap();
    println!("Written bytes: {}", written_bytes.load(Ordering::Relaxed));

    println!("Writing to parquet file for the second time");
    let col = Arc::new(Int64Array::from_iter_values([4])) as ArrayRef;
    let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();
    writer.write(&to_write).await.unwrap();
    println!("Written bytes: {}", written_bytes.load(Ordering::Relaxed));

    // Arrow writer has a BufWriter internally, which caches the data until it reaches 8KB.
    println!("Closing parquet file");
    let meta_data = writer.close().await.unwrap();
    println!("Written bytes: {}", written_bytes.load(Ordering::Relaxed));

    assert_eq!(meta_data.row_groups.len(), 3);

    let mut reader = ParquetRecordBatchReader::try_new(Bytes::from(buffer), 1024).unwrap();
    let read = reader.next().unwrap().unwrap();

    let result = read
        .column_by_name("col")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(result, &Int64Array::from_iter_values([1, 2, 3, 4]));
}

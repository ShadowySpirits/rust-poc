use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::io::Write;
use std::sync::Arc;

struct Writer<'a> {
    buffer: &'a mut Vec<u8>,
}

impl Write for Writer<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        println!("Writing {} bytes", buf.len());
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        println!("Flushing");
        Ok(())
    }
}

fn main() {
    let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
    let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();

    let mut buffer = Vec::new();

    let properties = WriterProperties::builder()
        .set_max_row_group_size(2)
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut writer = ArrowWriter::try_new(
        Writer {
            buffer: &mut buffer,
        },
        to_write.schema(),
        Some(properties),
    )
    .unwrap();

    println!("Writing to parquet file for the first time");
    writer.write(&to_write).unwrap();

    println!("Flushing parquet file");
    writer.flush().unwrap();

    println!("Writing to parquet file for the second time");
    let col = Arc::new(Int64Array::from_iter_values([4])) as ArrayRef;
    let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();
    writer.write(&to_write).unwrap();

    // Arrow writer has a BufWriter internally, which caches the data until it reaches 8KB.
    println!("Closing parquet file");
    let meta_data = writer.close().unwrap();
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

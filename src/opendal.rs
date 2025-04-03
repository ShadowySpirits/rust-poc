use opendal::layers::LoggingLayer;
use opendal::services;
use opendal::Operator;
use opendal::Result;

#[monoio::main]
async fn main() -> Result<()> {
    // Pick a builder and configure it.
    let builder = services::Memory::default();

    // Init an operator
    let op = Operator::new(builder)?
        // Init with logging layer enabled.
        .layer(LoggingLayer::default())
        .finish();
    
    let mut writer = op.writer_with("hello.txt")
        .chunk(1024)
        .await?;

    writer.write(vec![0; 1024]).await?;
    writer.write(vec![1; 1024]).await?;
    writer.write(vec![2; 1024]).await?;
    writer.write(vec![3; 1024]).await?;
    
    writer.close().await?;
    
    // Fetch this file's metadata
    let meta = op.stat("hello.txt").await?;
    let length = meta.content_length();
    assert_eq!(length, 4096);

    // Read data from `hello.txt` with range `1024..2048`.
    let bs = op.read_with("hello.txt").range(1024..2048).await?;
    assert_eq!(bs.to_vec(), vec![1; 1024]);
    
    Ok(())
}
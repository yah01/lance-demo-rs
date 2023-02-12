use std::sync::Arc;

use arrow::array::{Float32Array, Int64Array};
use arrow::record_batch::RecordBatch;
use lance::arrow::RecordBatchBuffer;
use lance::dataset::Dataset;
use lance::datatypes::{Field, Schema};
use lance::Result;

const STORE_PATH: &str = "lance-demo";
#[tokio::main]
async fn main() -> Result<()> {
    let dataset = match Dataset::open(STORE_PATH).await {
        Ok(dataset) => dataset,
        Err(_) => create_table().await?,
    };

    println!("schema: {:?}", dataset.schema());
    println!("dataset: {:?}", dataset);

    Ok(())
}

async fn create_table() -> Result<Dataset> {
    let ids = Arc::new(Int64Array::from_iter([1, 2, 3])) as _;
    let scores = Arc::new(Float32Array::from_iter([64.0, 89.0, 19.0])) as _;

    let batch = RecordBatch::try_from_iter([("id", ids), ("score", scores)]).unwrap();
    let mut reader = Box::new(RecordBatchBuffer::new(vec![batch])) as _;
    Dataset::write(&mut reader, STORE_PATH, None).await
}

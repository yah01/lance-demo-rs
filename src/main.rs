use arrow::array::{Float32Array, Int64Array, PrimitiveArray};
use arrow::datatypes::{ArrowPrimitiveType, Float32Type, Int64Type};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use lance::arrow::RecordBatchBuffer;
use lance::dataset::Dataset;
use lance::Result;
use std::sync::Arc;

const STORE_PATH: &str = "lance-demo";
#[tokio::main]
async fn main() -> Result<()> {
    let dataset = match Dataset::open(STORE_PATH).await {
        Ok(dataset) => dataset,
        Err(_) => create_table().await?,
    };

    println!(
        "schema: {}\nrow count:{}\n",
        dataset.schema(),
        dataset.count_rows().await?,
    );

    println!("rows:");
    let scanner = dataset.scan();
    let mut stream = scanner.try_into_stream().await?;
    while let Some(Ok(batch)) = stream.next().await {
        let ids = get_column_values::<Int64Type>(0, &batch);
        let scores = get_column_values::<Float32Type>(1, &batch);

        for (id, score) in ids.iter().zip(scores.iter()) {
            print!("id: {}, score: {}\n", id, score);
        }
    }

    Ok(())
}

async fn create_table() -> Result<Dataset> {
    let ids = Arc::new(Int64Array::from_iter([1, 2, 3])) as _;
    let scores = Arc::new(Float32Array::from_iter([64.0, 89.0, 19.0])) as _;

    let batch = RecordBatch::try_from_iter([("id", ids), ("score", scores)]).unwrap();
    let mut reader = Box::new(RecordBatchBuffer::new(vec![batch])) as _;
    Dataset::write(&mut reader, STORE_PATH, None).await
}

fn get_column_values<T: ArrowPrimitiveType>(index: usize, batch: &RecordBatch) -> &[T::Native] {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap()
        .values()
}

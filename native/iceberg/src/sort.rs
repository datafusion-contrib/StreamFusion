use arrow::array::RecordBatch;
use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::config::SessionConfig;
use datafusion_execution::memory_pool::GreedyMemoryPool;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::expressions::{Column, Literal};
use datafusion_physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr, ScalarFunctionExpr};
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion_physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion_physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::StreamExt;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;

struct Input {
    schema: SchemaRef,
    stream: Mutex<Option<SendableRecordBatchStream>>,
}

impl std::fmt::Debug for Input {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("IcebergRewriteInput")
    }
}

impl PartitionStream for Input {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        self.stream
            .lock()
            .unwrap()
            .take()
            .expect("One rewrite sort execution")
    }
}

/// Bounded channels keep file decoding incremental while DataFusion owns sorting and spill.
pub(crate) struct Sorter {
    pub(crate) schema: SchemaRef,
    input: Option<Sender<Result<RecordBatch>>>,
    output: Receiver<Result<RecordBatch>>,
    driver: Option<JoinHandle<()>>,
    plan: Arc<SortExec>,
}

impl Sorter {
    pub(crate) fn new(
        schema: SchemaRef,
        keys: Vec<(usize, Vec<String>, SortOptions)>,
        memory: usize,
        spill_path: &str,
    ) -> Result<Self> {
        let runtime = crate::reader::runtime();
        let incoming = RecordBatchReceiverStreamBuilder::new(schema.clone(), 1);
        let input = incoming.tx();
        let partition = Arc::new(Input {
            schema: schema.clone(),
            stream: Mutex::new(Some(incoming.build())),
        });
        let source = Arc::new(StreamingTableExec::try_new(
            schema.clone(),
            vec![partition],
            None,
            [],
            false,
            None,
        )?);
        let expressions = keys
            .into_iter()
            .map(|(index, children, options)| {
                let column: Arc<dyn PhysicalExpr> =
                    Arc::new(Column::new(schema.field(index).name(), index));
                let expression = if children.is_empty() {
                    column
                } else {
                    let mut args = vec![column];
                    args.extend(children.into_iter().map(|name| {
                        Arc::new(Literal::new(datafusion_common::ScalarValue::Utf8(Some(
                            name,
                        )))) as Arc<dyn PhysicalExpr>
                    }));
                    Arc::new(ScalarFunctionExpr::try_new(
                        datafusion_functions::core::get_field(),
                        args,
                        &schema,
                        Default::default(),
                    )?)
                };
                Ok(PhysicalSortExpr::new(expression, options))
            })
            .collect::<Result<Vec<_>>>()?;
        let ordering = LexOrdering::new(expressions)
            .ok_or_else(|| DataFusionError::Plan("Rewrite sort requires keys".into()))?;
        let plan = Arc::new(SortExec::new(ordering, source));
        let resources = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(memory)))
            .with_temp_file_path(spill_path)
            .build_arc()?;
        let config = SessionConfig::new()
            .with_batch_size(4096)
            .with_sort_spill_reservation_bytes(memory / 8)
            .with_sort_in_place_threshold_bytes(memory / 8);
        let context = Arc::new(TaskContext::new(
            None,
            "iceberg-rewrite".into(),
            config,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            resources,
        ));
        let (sender, output) = tokio::sync::mpsc::channel(1);
        let measured_plan = plan.clone();
        let driver = runtime.spawn(async move {
            let mut sorted = match plan.execute(0, context) {
                Ok(stream) => stream,
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                    return;
                }
            };
            while let Some(batch) = sorted.next().await {
                let failed = batch.is_err();
                if sender.send(batch).await.is_err() || failed {
                    break;
                }
            }
        });
        Ok(Self {
            schema,
            input: Some(input),
            output,
            driver: Some(driver),
            plan: measured_plan,
        })
    }

    pub(crate) async fn push(&self, batch: RecordBatch) -> Result<()> {
        self.input
            .as_ref()
            .ok_or_else(|| DataFusionError::Execution("Sort input closed".into()))?
            .send(Ok(batch))
            .await
            .map_err(|_| DataFusionError::Execution("Rewrite sort failed".into()))
    }

    pub(crate) fn finish(&mut self) {
        self.input.take();
    }

    pub(crate) fn statistics(&self) -> [i64; 2] {
        let metrics = self.plan.metrics().expect("Rewrite sort metrics");
        [
            metrics.spill_count().unwrap_or(0) as i64,
            metrics.spilled_bytes().unwrap_or(0) as i64,
        ]
    }

    pub(crate) async fn next(&mut self) -> Result<Option<RecordBatch>> {
        match self.output.recv().await {
            Some(batch) => batch.map(Some),
            None => {
                if let Some(driver) = self.driver.take() {
                    driver
                        .await
                        .map_err(|error| DataFusionError::External(Box::new(error)))?;
                }
                Ok(None)
            }
        }
    }

    pub(crate) async fn close(&mut self) {
        self.input.take();
        self.output.close();
        while self.output.try_recv().is_ok() {}
        if let Some(driver) = self.driver.take() {
            driver.abort();
            let _ = driver.await;
        }
    }
}

impl Drop for Sorter {
    fn drop(&mut self) {
        if let Some(driver) = &self.driver {
            driver.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn sort_spills_and_preserves_nulls_and_payloads_across_batches() {
        crate::reader::runtime().block_on(async {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("payload", DataType::Utf8, false),
            ]));
            let directory = std::env::temp_dir();
            let mut sorter = Sorter::new(
                schema.clone(),
                vec![(
                    0,
                    vec![],
                    SortOptions {
                        descending: false,
                        nulls_first: true,
                    },
                )],
                2 << 20,
                directory.to_str().unwrap(),
            )
            .unwrap();
            let mut expected_nulls = 0;
            for block in 0..100_i64 {
                let ids = (block * 1024..(block + 1) * 1024)
                    .rev()
                    .map(|id| {
                        if id % 19 == 0 {
                            expected_nulls += 1;
                            None
                        } else {
                            Some(id)
                        }
                    })
                    .collect::<Int64Array>();
                let payloads = StringArray::from(vec![
                        "payload-é🐍-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
                        1024
                    ]);
                sorter
                    .push(
                        RecordBatch::try_new(
                            schema.clone(),
                            vec![Arc::new(ids), Arc::new(payloads)],
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap();
            }
            sorter.finish();
            let mut count = 0;
            let mut nulls = 0;
            let mut previous = None;
            while let Some(batch) = sorter.next().await.unwrap() {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let payloads = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    if ids.is_null(row) {
                        assert!(previous.is_none());
                        nulls += 1;
                    } else {
                        let id = ids.value(row);
                        assert!(previous.is_none_or(|last| last < id));
                        previous = Some(id);
                    }
                    assert_eq!(
                        payloads.value(row),
                        "payload-é🐍-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                    );
                    count += 1;
                }
            }
            assert_eq!(count, 102_400);
            assert_eq!(nulls, expected_nulls);
            assert!(sorter.plan.metrics().unwrap().spill_count().unwrap() > 0);
        });
    }
}

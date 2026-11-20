use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet, SpillMetrics, Time};
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::spill::{get_record_batch_memory_size, SpillManager};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::{stream, StreamExt, TryStreamExt};
use std::fmt::{self, Debug, Formatter};
use std::sync::{Arc, Mutex};

const PARTITIONS: usize = 32;

/// Keeps the ordinary DataFusion hash join for builds that fit. Oversized builds are
/// partitioned with the same key hash on both sides and replayed one pair at a time.
#[derive(Debug)]
pub(super) struct SpillHashJoin {
    join: Arc<HashJoinExec>,
    metrics: ExecutionPlanMetricsSet,
}

impl SpillHashJoin {
    pub(super) fn new(join: HashJoinExec) -> Self {
        Self {
            join: Arc::new(join),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for SpillHashJoin {
    fn fmt_as(&self, format: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        self.join.fmt_as(format, f)
    }
}

impl ExecutionPlan for SpillHashJoin {
    fn name(&self) -> &str {
        "SpillHashJoin"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.join.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.join.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            self.join
                .builder()
                .reset_state()
                .with_new_children(children)?
                .build()?,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let build = self.join.left().execute(partition, context.clone())?;
        let join = self.join.clone();
        let metrics = self.metrics.clone();
        let schema = self.schema();
        let output =
            stream::once(async move { start_join(join, build, partition, context, metrics).await })
                .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

fn hash_build_budget(batch: &RecordBatch) -> usize {
    // HashJoin retains input batches, consolidates them and builds the key table.
    get_record_batch_memory_size(batch)
        .saturating_mul(2)
        .saturating_add(batch.num_rows().saturating_mul(32))
}

fn exhausted(error: &DataFusionError) -> bool {
    matches!(error.find_root(), DataFusionError::ResourcesExhausted(_))
}

async fn start_join(
    join: Arc<HashJoinExec>,
    mut build: SendableRecordBatchStream,
    partition: usize,
    context: Arc<TaskContext>,
    metrics: ExecutionPlanMetricsSet,
) -> Result<SendableRecordBatchStream> {
    let reservation = MemoryConsumer::new("BoundedHashJoinBuild")
        .with_can_spill(true)
        .register(context.memory_pool());
    let mut buffered = Vec::new();
    while let Some(batch) = build.next().await.transpose()? {
        match reservation.try_grow(hash_build_budget(&batch)) {
            Ok(()) => buffered.push(batch),
            Err(error) if exhausted(&error) => {
                return spill_join(
                    join,
                    buffered,
                    batch,
                    build,
                    reservation,
                    partition,
                    context,
                    metrics,
                )
                .await;
            }
            Err(error) => return Err(error),
        }
    }
    let build = Box::pin(RecordBatchStreamAdapter::new(
        build.schema(),
        stream::iter(buffered.into_iter().map(Ok)),
    ));
    let probe = join.right().clone();
    // The DataFusion build charges these same Arrow buffers as it takes ownership.
    drop(reservation);
    execute_hash(&join, stream_input(build)?, probe, partition, context)
}

async fn spill_join(
    join: Arc<HashJoinExec>,
    buffered: Vec<RecordBatch>,
    first: RecordBatch,
    build: SendableRecordBatchStream,
    reservation: MemoryReservation,
    partition: usize,
    context: Arc<TaskContext>,
    metrics: ExecutionPlanMetricsSet,
) -> Result<SendableRecordBatchStream> {
    let build_manager = spill_manager(&context, &metrics, partition, build.schema());
    let build_keys = join.on().iter().map(|(key, _)| key.clone()).collect();
    let prefix = stream::iter(buffered.into_iter().map(Ok));
    let build = partition_input(
        build_manager,
        build_keys,
        prefix.chain(stream::once(async { Ok(first) })).chain(build),
        Some(reservation),
        &context,
    )
    .await?;
    let probe = join.right().execute(partition, context.clone())?;
    let probe_manager = spill_manager(&context, &metrics, partition, probe.schema());
    let probe_keys = join.on().iter().map(|(_, key)| key.clone()).collect();
    let probe = partition_input(probe_manager, probe_keys, probe, None, &context).await?;
    let schema = join.schema();
    let output = stream::iter(build.into_iter().zip(probe))
        .then(move |(left, right)| {
            let context = context.clone();
            let join = join.clone();
            async move { execute_partition(join, left, right, context).await }
        })
        .try_flatten();
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
}

fn spill_manager(
    context: &TaskContext,
    metrics: &ExecutionPlanMetricsSet,
    partition: usize,
    schema: SchemaRef,
) -> SpillManager {
    SpillManager::new(
        context.runtime_env(),
        SpillMetrics::new(metrics, partition),
        schema,
    )
    .with_compression_type(context.session_config().spill_compression())
}

async fn partition_input(
    manager: SpillManager,
    keys: Vec<PhysicalExprRef>,
    input: impl futures::Stream<Item = Result<RecordBatch>>,
    mut prefix_reservation: Option<MemoryReservation>,
    context: &TaskContext,
) -> Result<Vec<DiskPartition>> {
    let mut partitioner =
        BatchPartitioner::new_hash_partitioner(keys, PARTITIONS, Time::default())?;
    let mut files: Vec<_> = (0..PARTITIONS).map(|_| None).collect();
    let mut input = std::pin::pin!(input);
    let workspace = MemoryConsumer::new("BoundedHashJoinPartition").register(context.memory_pool());
    while let Some(batch) = input.next().await.transpose()? {
        // Free the reserved build scratch as the corresponding rows move to disk.
        if let Some(prefix) = &mut prefix_reservation {
            prefix.shrink(hash_build_budget(&batch).min(prefix.size()));
        }
        let bytes = get_record_batch_memory_size(&batch);
        workspace.try_resize(
            bytes
                .saturating_mul(2)
                .saturating_add(batch.num_rows().saturating_mul(12)),
        )?;
        partitioner.partition(batch, |index, batch| {
            let writer = match &mut files[index] {
                Some(writer) => writer,
                slot => {
                    slot.insert(manager.create_in_progress_file("bounded hash join partition")?)
                }
            };
            writer.append_batch(&batch)?;
            Ok(())
        })?;
    }
    files
        .into_iter()
        .map(|file| {
            Ok(DiskPartition {
                manager: manager.clone(),
                file: file
                    .map(|mut writer| writer.finish())
                    .transpose()?
                    .flatten(),
            })
        })
        .collect()
}

struct DiskPartition {
    manager: SpillManager,
    file: Option<RefCountedTempFile>,
}

impl DiskPartition {
    fn input(&self) -> Result<Arc<dyn ExecutionPlan>> {
        match &self.file {
            Some(file) => stream_input(
                self.manager
                    .read_spill_as_stream_unbuffered(file.clone(), None)?,
            ),
            None => Ok(Arc::new(EmptyExec::new(self.manager.schema().clone()))),
        }
    }
}

async fn execute_partition(
    join: Arc<HashJoinExec>,
    left: DiskPartition,
    right: DiskPartition,
    context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    let mut hash = execute_hash(&join, left.input()?, right.input()?, 0, context.clone())?;
    // A hot key can leave a single hash partition oversized. Before emitting any rows,
    // replay that partition through DataFusion's spillable sort-merge join.
    match hash.next().await {
        Some(Err(error)) if exhausted(&error) => {
            drop(hash);
            super::sorted_join(
                left.input()?,
                right.input()?,
                join.on().to_vec(),
                join.filter().cloned(),
                *join.join_type(),
                join.null_equality(),
            )?
            .execute(0, context)
        }
        Some(first) => Ok(Box::pin(RecordBatchStreamAdapter::new(
            hash.schema(),
            stream::once(async { first }).chain(hash),
        ))),
        None => Ok(hash),
    }
}

fn execute_hash(
    join: &HashJoinExec,
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    partition: usize,
    context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    join.builder()
        .reset_state()
        .with_new_children(vec![left, right])?
        .build()?
        .execute(partition, context)
}

struct OneShotInput {
    stream: Mutex<Option<SendableRecordBatchStream>>,
    schema: SchemaRef,
}

impl Debug for OneShotInput {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("BoundedHashJoinInput")
    }
}

impl PartitionStream for OneShotInput {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        self.stream
            .lock()
            .unwrap()
            .take()
            .expect("one hash join execution")
    }
}

fn stream_input(stream: SendableRecordBatchStream) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = stream.schema();
    Ok(Arc::new(StreamingTableExec::try_new(
        schema.clone(),
        vec![Arc::new(OneShotInput {
            stream: Mutex::new(Some(stream)),
            schema,
        })],
        None,
        [],
        false,
        None,
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BinaryArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::execution::context::SessionConfig;
    use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::joins::PartitionMode;

    fn rows(side: &str, ids: std::ops::Range<i64>, build: bool) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(format!("{side}_key"), DataType::Int64, true),
            Field::new(format!("{side}_id"), DataType::Int64, false),
            Field::new(format!("{side}_payload"), DataType::Binary, false),
        ]));
        let keys = Int64Array::from_iter(ids.clone().map(|id| match id % 128 {
            0 => None,
            1 => Some(if build { 2 } else { 3 }),
            _ => Some(1),
        }));
        let payload = BinaryArray::from_iter_values(ids.clone().map(|id| [id as u8; 128]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(keys),
                Arc::new(Int64Array::from_iter_values(ids)),
                Arc::new(payload),
            ],
        )
        .unwrap()
    }

    fn input(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        stream_input(Box::pin(RecordBatchStreamAdapter::new(
            batches[0].schema(),
            stream::iter(batches.into_iter().map(Ok)),
        )))
        .unwrap()
    }

    async fn included_ids(mut result: SendableRecordBatchStream) -> Vec<[Option<i64>; 2]> {
        let schema = result.schema();
        let ids = ["left_id", "right_id"].map(|name| schema.index_of(name).ok());
        let mut output = Vec::new();
        while let Some(batch) = result.next().await.transpose().unwrap() {
            let columns = ids.map(|index| {
                index.map(|index| {
                    batch
                        .column(index)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                })
            });
            for row in 0..batch.num_rows() {
                output.push(columns.map(|column| {
                    column.and_then(|column| (!column.is_null(row)).then(|| column.value(row)))
                }));
            }
        }
        output.sort_unstable();
        output
    }

    #[test]
    fn spilled_hash_join_matches_hash_results_for_skew_and_nulls() {
        crate::runtime().block_on(async {
            let work = tempfile::tempdir().unwrap();
            let left: Vec<_> = (0..64)
                .map(|batch| rows("left", batch * 128..(batch + 1) * 128, true))
                .collect();
            let right = vec![rows("right", 0..4, false)];
            let pool = Arc::new(GreedyMemoryPool::new(512 * 1024));
            let context = Arc::new(
                TaskContext::default()
                    .with_runtime(
                        RuntimeEnvBuilder::new()
                            .with_memory_pool(pool.clone())
                            .with_disk_manager_builder(DiskManagerBuilder::default().with_mode(
                                DiskManagerMode::Directories(vec![work.path().to_owned()]),
                            ))
                            .build_arc()
                            .unwrap(),
                    )
                    .with_session_config(
                        SessionConfig::new()
                            .with_batch_size(128)
                            .with_sort_spill_reservation_bytes(32 * 1024),
                    ),
            );
            for kind in [JoinType::Full, JoinType::LeftAnti, JoinType::RightSemi] {
                for nulls in [
                    NullEquality::NullEqualsNothing,
                    NullEquality::NullEqualsNull,
                ] {
                    let make_join = || {
                        HashJoinExec::try_new(
                            input(left.clone()),
                            input(right.clone()),
                            vec![(
                                Arc::new(Column::new("left_key", 0)),
                                Arc::new(Column::new("right_key", 0)),
                            )],
                            None,
                            &kind,
                            None,
                            PartitionMode::Partitioned,
                            nulls,
                            false,
                        )
                        .unwrap()
                    };
                    let reference = make_join()
                        .execute(0, Arc::new(TaskContext::default()))
                        .unwrap();
                    let spill = SpillHashJoin::new(make_join());
                    let actual = spill.execute(0, context.clone()).unwrap();
                    assert_eq!(included_ids(reference).await, included_ids(actual).await);
                    assert!(spill.metrics().unwrap().spill_count().unwrap_or(0) > 0);
                    assert_eq!(context.memory_pool().reserved(), 0);
                    assert_eq!(context.runtime_env().disk_manager.used_disk_space(), 0);
                }
            }
        });
    }
}

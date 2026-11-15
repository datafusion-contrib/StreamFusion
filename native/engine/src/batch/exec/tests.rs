use super::*;
use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::config::SessionConfig;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::sorts::sort::SortExec;
use futures::task::{waker_ref, ArcWake};
use std::sync::atomic::{AtomicUsize, Ordering};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]))
}

fn batch(values: Vec<Option<i64>>) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(values))]).unwrap()
}

fn sort_plan(input: &BoundedInput, descending: bool, nulls_first: bool) -> Arc<dyn ExecutionPlan> {
    Arc::new(SortExec::new(
        [PhysicalSortExpr {
            expr: Arc::new(Column::new("value", 0)),
            options: SortOptions {
                descending,
                nulls_first,
            },
        }]
        .into(),
        input.execution_plan().unwrap(),
    ))
}

fn context(
    pool: Arc<dyn MemoryPool>,
    directory: &std::path::Path,
    batch_size: usize,
) -> Arc<TaskContext> {
    Arc::new(
        TaskContext::default()
            .with_runtime(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(pool)
                    .with_temp_file_path(directory)
                    .build_arc()
                    .unwrap(),
            )
            .with_session_config(
                SessionConfig::new()
                    .with_batch_size(batch_size)
                    .with_sort_spill_reservation_bytes(32 * 1024),
            ),
    )
}

async fn consume_input(execution: &mut BoundedExecution, input: &BoundedInput) {
    let ExecutionEvent::InputNeeded(needed) = execution.next_event().await.unwrap() else {
        panic!("sort must request another input before emitting output");
    };
    assert_eq!(needed, vec![0]);
    assert!(matches!(
        input.poll_ready(&mut Context::from_waker(futures::task::noop_waker_ref())),
        Poll::Ready(Ok(()))
    ));
}

fn values(batch: &RecordBatch) -> Vec<Option<i64>> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl ArcWake for WakeCount {
    fn wake_by_ref(this: &Arc<Self>) {
        this.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn input_is_pending_until_pushed_and_backpressures_the_producer() {
    let input = BoundedInput::new(schema());
    let mut stream = input.execute(Arc::new(TaskContext::default()));
    let wakes = Arc::new(WakeCount::default());
    let waker = waker_ref(&wakes);
    let mut cx = Context::from_waker(&waker);
    assert!(stream.poll_next_unpin(&mut cx).is_pending());
    input.push(batch(vec![Some(1)])).unwrap();
    assert!(wakes.0.load(Ordering::SeqCst) > 0);
    assert!(input.poll_ready(&mut cx).is_pending());
    assert!(input.push(batch(vec![Some(2)])).is_err());
    let before = wakes.0.load(Ordering::SeqCst);
    let Poll::Ready(Some(Ok(output))) = stream.poll_next_unpin(&mut cx) else {
        panic!("offered batch must be ready");
    };
    assert_eq!(values(&output), vec![Some(1)]);
    assert!(wakes.0.load(Ordering::SeqCst) > before);
    assert!(matches!(input.poll_ready(&mut cx), Poll::Ready(Ok(()))));
    assert!(stream.poll_next_unpin(&mut cx).is_pending());
}

#[test]
fn eof_preserves_the_last_batch_and_prevents_replay() {
    crate::runtime().block_on(async {
        let input = BoundedInput::new(schema());
        let mut stream = input.execute(Arc::new(TaskContext::default()));
        input.push(batch(vec![None, Some(3)])).unwrap();
        input.finish().unwrap();
        assert_eq!(
            values(&stream.next().await.unwrap().unwrap()),
            vec![None, Some(3)]
        );
        assert!(stream.next().await.is_none());
        assert!(input.push(batch(vec![])).is_err());
        let mut replay = input.execute(Arc::new(TaskContext::default()));
        assert!(replay
            .next()
            .await
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("cannot be replayed"));
    });
}

#[test]
fn dropping_execution_releases_pending_arrow_and_wakes_the_producer() {
    let input = BoundedInput::new(schema());
    let execution = BoundedExecution::try_new(
        input.execution_plan().unwrap(),
        vec![input.clone()],
        Arc::new(TaskContext::default()),
    )
    .unwrap();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    let weak = Arc::downgrade(&array);
    input
        .push(RecordBatch::try_new(schema(), vec![array]).unwrap())
        .unwrap();
    let wakes = Arc::new(WakeCount::default());
    let waker = waker_ref(&wakes);
    let mut cx = Context::from_waker(&waker);
    assert!(input.poll_ready(&mut cx).is_pending());
    drop(execution);
    assert!(weak.upgrade().is_none());
    assert!(wakes.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(input.poll_ready(&mut cx), Poll::Ready(Err(_))));
}

#[test]
fn schema_changes_are_rejected_without_consuming_the_slot() {
    let input = BoundedInput::new(schema());
    assert!(input
        .push(RecordBatch::new_empty(Arc::new(Schema::empty())))
        .is_err());
    input.push(batch(vec![])).unwrap();
}

#[test]
fn incremental_sort_waits_for_eof_and_drains_in_bounded_batches() {
    crate::runtime().block_on(async {
        let directory = tempfile::tempdir().unwrap();
        for (descending, nulls_first, expected) in [
            (
                false,
                true,
                vec![
                    None,
                    None,
                    Some(-5),
                    Some(1),
                    Some(2),
                    Some(2),
                    Some(i64::MAX),
                ],
            ),
            (
                true,
                false,
                vec![
                    Some(i64::MAX),
                    Some(2),
                    Some(2),
                    Some(1),
                    Some(-5),
                    None,
                    None,
                ],
            ),
        ] {
            let input = BoundedInput::new(schema());
            let pool = Arc::new(GreedyMemoryPool::new(1024 * 1024));
            let ctx = context(pool.clone(), directory.path(), 2);
            let mut execution = BoundedExecution::try_new(
                sort_plan(&input, descending, nulls_first),
                vec![input.clone()],
                ctx,
            )
            .unwrap();
            for data in [
                vec![Some(2), None],
                vec![],
                vec![Some(i64::MAX), Some(-5)],
                vec![None, Some(1), Some(2)],
            ] {
                input.push(batch(data)).unwrap();
                consume_input(&mut execution, &input).await;
            }
            input.finish().unwrap();
            let mut actual = vec![];
            while let Some(output) = execution.next().await {
                let output = output.unwrap();
                assert!(output.num_rows() <= 2);
                actual.extend(values(&output));
            }
            assert_eq!(actual, expected);
            assert_eq!(pool.reserved(), 0);
        }
    });
}

#[test]
fn empty_input_sort_completes() {
    crate::runtime().block_on(async {
        let input = BoundedInput::new(schema());
        let mut execution = BoundedExecution::try_new(
            sort_plan(&input, false, true),
            vec![input.clone()],
            Arc::new(TaskContext::default()),
        )
        .unwrap();
        input.finish().unwrap();
        while let Some(batch) = execution.next().await {
            assert_eq!(batch.unwrap().num_rows(), 0);
        }
    });
}

#[test]
fn sort_spills_before_eof_and_releases_files_and_memory() {
    crate::runtime().block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let input = BoundedInput::new(schema());
        let pool = Arc::new(GreedyMemoryPool::new(256 * 1024));
        let ctx = context(pool.clone(), directory.path(), 1024);
        let plan = sort_plan(&input, false, false);
        let mut execution =
            BoundedExecution::try_new(plan.clone(), vec![input.clone()], ctx.clone()).unwrap();
        let row_count = 128 * 1024;
        for low in (0..row_count).step_by(1024).rev() {
            input
                .push(batch(
                    (low..low + 1024).rev().map(|v| Some(v as i64)).collect(),
                ))
                .unwrap();
            consume_input(&mut execution, &input).await;
        }
        assert!(
            plan.metrics().unwrap().spill_count().unwrap() > 0,
            "must spill while the input is still open"
        );
        assert!(pool.reserved() <= 256 * 1024);
        input.finish().unwrap();
        let mut expected = 0;
        while let Some(output) = execution.next().await {
            let output = output.unwrap();
            assert!(output.num_rows() <= 1024);
            for value in values(&output) {
                assert_eq!(value, Some(expected));
                expected += 1;
            }
        }
        assert_eq!(expected, row_count as i64);
        drop(execution);
        drop(ctx);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    });
}

#[test]
fn memory_denial_terminates_the_stream_and_closes_the_input() {
    crate::runtime().block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let input = BoundedInput::new(schema());
        let pool = Arc::new(GreedyMemoryPool::new(1));
        let ctx = context(pool.clone(), directory.path(), 1024);
        let mut execution =
            BoundedExecution::try_new(sort_plan(&input, false, false), vec![input.clone()], ctx)
                .unwrap();
        input.push(batch(vec![Some(1)])).unwrap();
        let error = execution.next().await.unwrap().unwrap_err();
        assert!(
            matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
            "{error}"
        );
        assert!(execution.next().await.is_none());
        assert!(input.push(batch(vec![Some(2)])).is_err());
        assert_eq!(pool.reserved(), 0);
    });
}

#[test]
fn worker_driven_inputs_signal_demand_and_finish_independently() {
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::union::UnionExec;

    crate::runtime().block_on(async {
        let inputs = vec![BoundedInput::new(schema()), BoundedInput::new(schema())];
        let union = Arc::new(UnionExec::new(
            inputs
                .iter()
                .map(|input| input.execution_plan().unwrap())
                .collect(),
        ));
        let plan = Arc::new(CoalescePartitionsExec::new(union));
        let mut execution =
            BoundedExecution::try_new(plan, inputs.clone(), Arc::new(TaskContext::default()))
                .unwrap();
        let mut offered = [false; 2];
        let mut actual = vec![];
        loop {
            match execution.next_event().await.unwrap() {
                ExecutionEvent::InputNeeded(needed) => {
                    for index in needed {
                        if offered[index] {
                            inputs[index].finish().unwrap();
                        } else {
                            inputs[index].push(batch(vec![Some(index as i64)])).unwrap();
                            offered[index] = true;
                        }
                    }
                }
                ExecutionEvent::Output(output) => actual.extend(values(&output)),
                ExecutionEvent::Finished => break,
            }
        }
        actual.sort();
        assert_eq!(actual, vec![Some(0), Some(1)]);
        assert!(offered.into_iter().all(|offered| offered));
    });
}

#[test]
fn global_aggregation_consumes_incrementally_and_preserves_empty_input_results() {
    use datafusion::functions_aggregate::{count::count_udaf, sum::sum_udaf};
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};

    crate::runtime().block_on(async {
        let directory = tempfile::tempdir().unwrap();
        for (batches, expected) in [
            (
                vec![vec![Some(2), None], vec![Some(-4), Some(10)]],
                vec![Some(8), Some(3)],
            ),
            (vec![vec![None, None]], vec![None, Some(0)]),
            (vec![], vec![None, Some(0)]),
        ] {
            let input = BoundedInput::new(schema());
            let pool = Arc::new(GreedyMemoryPool::new(1024 * 1024));
            let aggregates = [sum_udaf(), count_udaf()]
                .into_iter()
                .enumerate()
                .map(|(index, function)| {
                    Arc::new(
                        AggregateExprBuilder::new(
                            function,
                            vec![Arc::new(Column::new("value", 0))],
                        )
                        .schema(schema())
                        .alias(format!("result{index}"))
                        .build()
                        .unwrap(),
                    )
                })
                .collect();
            let plan = Arc::new(
                AggregateExec::try_new(
                    AggregateMode::Single,
                    PhysicalGroupBy::new_single(vec![]),
                    aggregates,
                    vec![None, None],
                    input.execution_plan().unwrap(),
                    schema(),
                )
                .unwrap(),
            );
            let mut execution = BoundedExecution::try_new(
                plan,
                vec![input.clone()],
                context(pool.clone(), directory.path(), 1024),
            )
            .unwrap();
            for values in batches {
                input.push(batch(values)).unwrap();
                let ExecutionEvent::InputNeeded(needed) = execution.next_event().await.unwrap()
                else {
                    panic!("aggregate emitted before EOF");
                };
                assert_eq!(needed, vec![0]);
            }
            input.finish().unwrap();
            let ExecutionEvent::Output(output) = execution.next_event().await.unwrap() else {
                panic!("global aggregate must produce one row even for empty input");
            };
            assert_eq!(output.num_rows(), 1);
            let actual: Vec<_> = output
                .columns()
                .iter()
                .map(|column| {
                    let column = column.as_any().downcast_ref::<Int64Array>().unwrap();
                    (!column.is_null(0)).then(|| column.value(0))
                })
                .collect();
            assert_eq!(actual, expected);
            assert!(matches!(
                execution.next_event().await.unwrap(),
                ExecutionEvent::Finished
            ));
            assert_eq!(pool.reserved(), 0);
        }
    });
}

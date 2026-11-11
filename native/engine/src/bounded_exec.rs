//! Incremental inputs for one bounded DataFusion task. The caller supplies the task context,
//! including its memory pool and spill directory; this module never creates an unbounded runtime.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

#[derive(Debug, Default)]
struct InputState {
    batch: Option<RecordBatch>,
    finished: bool,
    cancelled: bool,
    started: bool,
    reader: Option<Waker>,
    writer: Option<Waker>,
    driver: Option<Waker>,
}

/// One pending Arrow batch, with explicit EOF and backpressure. Like Comet's scan, an empty
/// slot is Pending, not EOF. Unlike a pull-based Spark iterator, a Flink input cannot be read
/// recursively from the native poll: its mailbox must remain free to deliver the next batch.
#[derive(Clone, Debug)]
pub(crate) struct BoundedInput {
    schema: SchemaRef,
    state: Arc<Mutex<InputState>>,
}

impl BoundedInput {
    pub(crate) fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            state: Arc::new(Mutex::new(InputState::default())),
        }
    }

    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub(crate) fn execution_plan(&self) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(StreamingTableExec::try_new(
            Arc::clone(&self.schema),
            vec![Arc::new(self.clone())],
            None,
            [],
            false,
            None,
        )?))
    }

    /// Does not import or retain another batch while the previous slot is occupied.
    pub(crate) fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let mut state = self.state.lock().unwrap();
        if state.finished || state.cancelled {
            return Poll::Ready(Err(closed_input()));
        }
        if state.batch.is_none() {
            Poll::Ready(Ok(()))
        } else {
            state.writer = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    pub(crate) fn push(&self, batch: RecordBatch) -> Result<()> {
        if batch.schema() != self.schema {
            return Err(DataFusionError::Execution(
                "bounded input schema changed".into(),
            ));
        }
        let reader = {
            let mut state = self.state.lock().unwrap();
            if state.finished || state.cancelled {
                return Err(closed_input());
            }
            if state.batch.is_some() {
                return Err(DataFusionError::Execution(
                    "bounded input requires backpressure before another batch".into(),
                ));
            }
            state.batch = Some(batch);
            state.reader.take()
        };
        if let Some(reader) = reader {
            reader.wake();
        }
        Ok(())
    }

    /// EOF follows the last offered batch, even when it has not been consumed yet.
    pub(crate) fn finish(&self) -> Result<()> {
        let (reader, writer, driver) = {
            let mut state = self.state.lock().unwrap();
            if state.cancelled {
                return Err(closed_input());
            }
            state.finished = true;
            (
                state.reader.take(),
                state.writer.take(),
                state.driver.take(),
            )
        };
        wake(reader, writer);
        if let Some(driver) = driver {
            driver.wake();
        }
        Ok(())
    }

    fn cancel(&self) {
        let (reader, writer, driver) = {
            let mut state = self.state.lock().unwrap();
            state.cancelled = true;
            state.batch = None;
            (
                state.reader.take(),
                state.writer.take(),
                state.driver.take(),
            )
        };
        wake(reader, writer);
        if let Some(driver) = driver {
            driver.wake();
        }
    }
}

fn closed_input() -> DataFusionError {
    DataFusionError::Execution("bounded input is closed".into())
}

fn wake(reader: Option<Waker>, writer: Option<Waker>) {
    if let Some(reader) = reader {
        reader.wake();
    }
    if let Some(writer) = writer {
        writer.wake();
    }
}

impl PartitionStream for BoundedInput {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut state = self.state.lock().unwrap();
        if state.started {
            return Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                futures::stream::once(async {
                    Err(DataFusionError::Execution(
                        "bounded input cannot be replayed; rebuild the task on retry".into(),
                    ))
                }),
            ));
        }
        state.started = true;
        drop(state);
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            InputStream(self.clone()),
        ))
    }
}

struct InputStream(BoundedInput);

impl Stream for InputStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (result, writer, driver) = {
            let mut state = self.0.state.lock().unwrap();
            if state.cancelled {
                return Poll::Ready(None);
            }
            if let Some(batch) = state.batch.take() {
                (Poll::Ready(Some(Ok(batch))), state.writer.take(), None)
            } else if state.finished {
                (Poll::Ready(None), None, None)
            } else {
                state.reader = Some(cx.waker().clone());
                (Poll::Pending, None, state.driver.take())
            }
        };
        if let Some(writer) = writer {
            writer.wake();
        }
        if let Some(driver) = driver {
            driver.wake();
        }
        result
    }
}

impl Drop for InputStream {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Owns a single execution, not a collect() future. Output batches leave one at a time and the
/// caller's waker observes both input demand and native I/O completion. No mailbox-blocking wait
/// or additional output queue is introduced here.
pub(crate) struct BoundedExecution {
    stream: Option<SendableRecordBatchStream>,
    inputs: Vec<BoundedInput>,
}

pub(crate) enum ExecutionEvent {
    Output(RecordBatch),
    InputNeeded(Vec<usize>),
    Finished,
}

impl BoundedExecution {
    pub(crate) fn try_new(
        plan: Arc<dyn ExecutionPlan>,
        inputs: Vec<BoundedInput>,
        context: Arc<TaskContext>,
    ) -> Result<Self> {
        if plan.output_partitioning().partition_count() != 1 {
            return Err(DataFusionError::Execution(
                "bounded task must have exactly one output partition".into(),
            ));
        }
        if plan.boundedness().is_unbounded() {
            return Err(DataFusionError::Execution(
                "bounded task cannot execute an unbounded plan".into(),
            ));
        }
        let mut execution = Self {
            stream: None,
            inputs,
        };
        execution.stream = Some(plan.execute(0, context)?);
        Ok(execution)
    }

    /// Wait for native I/O, but return to the host when a scan needs another input. A scan may
    /// run on a DataFusion worker task, so it wakes this driver separately from its own consumer.
    pub(crate) async fn next_event(&mut self) -> Result<ExecutionEvent> {
        futures::future::poll_fn(|cx| {
            for input in &self.inputs {
                input.state.lock().unwrap().driver = Some(cx.waker().clone());
            }
            match self.poll_next_unpin(cx) {
                Poll::Ready(Some(batch)) => {
                    return Poll::Ready(batch.map(ExecutionEvent::Output));
                }
                Poll::Ready(None) => return Poll::Ready(Ok(ExecutionEvent::Finished)),
                Poll::Pending => {}
            }
            let needed: Vec<usize> = self
                .inputs
                .iter()
                .enumerate()
                .filter_map(|(index, input)| {
                    let state = input.state.lock().unwrap();
                    (!state.finished
                        && !state.cancelled
                        && state.batch.is_none()
                        && state.reader.is_some())
                    .then_some(index)
                })
                .collect();
            if needed.is_empty() {
                Poll::Pending
            } else {
                Poll::Ready(Ok(ExecutionEvent::InputNeeded(needed)))
            }
        })
        .await
    }
}

impl Stream for BoundedExecution {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(stream) = &mut self.stream else {
            return Poll::Ready(None);
        };
        let result = stream.poll_next_unpin(cx);
        if matches!(result, Poll::Ready(None | Some(Err(_)))) {
            self.stream = None;
            for input in &self.inputs {
                input.cancel();
            }
        }
        result
    }
}

impl Drop for BoundedExecution {
    fn drop(&mut self) {
        for input in &self.inputs {
            input.cancel();
        }
        self.stream = None;
    }
}

#[cfg(test)]
mod tests;

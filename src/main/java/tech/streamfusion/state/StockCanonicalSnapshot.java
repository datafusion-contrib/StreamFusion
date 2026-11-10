package tech.streamfusion.state;

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.AsyncSnapshotCallable;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SavepointSnapshotStrategy;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategyRunner;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.util.function.SupplierWithException;

/** Captures stock keyed and managed operator state at the same synchronous barrier boundary. */
public final class StockCanonicalSnapshot {
  private StockCanonicalSnapshot() {}

  public static OperatorSnapshotFutures capture(
      CheckpointableKeyedStateBackend<?> keyed,
      OperatorStateBackend operator,
      long checkpointId,
      long timestamp,
      CheckpointStreamFactory streams,
      CheckpointOptions options,
      CloseableRegistry cancelables)
      throws Exception {
    return capture(
        keyed::savepoint, operator, checkpointId, timestamp, streams, options, cancelables);
  }

  public static OperatorSnapshotFutures capture(
      SupplierWithException<SavepointResources<?>, Exception> keyed,
      OperatorStateBackend operator,
      long checkpointId,
      long timestamp,
      CheckpointStreamFactory streams,
      CheckpointOptions options,
      CloseableRegistry cancelables)
      throws Exception {
    return capture(keyed, operator, checkpointId, timestamp, streams, options, cancelables, null);
  }

  public static OperatorSnapshotFutures capture(
      SupplierWithException<SavepointResources<?>, Exception> keyed,
      OperatorStateBackend operator,
      long checkpointId,
      long timestamp,
      CheckpointStreamFactory streams,
      CheckpointOptions options,
      CloseableRegistry cancelables,
      SupplierWithException<AutoCloseable, Exception> retain)
      throws Exception {
    if (!options.getCheckpointType().isSavepoint()
        || ((SavepointType) options.getCheckpointType()).getFormatType()
            != SavepointFormatType.CANONICAL)
      throw new IllegalArgumentException("Stock state capture requires a canonical savepoint");
    var result = new OperatorSnapshotFutures();
    SavepointResources<?> resources = null;
    boolean started = false;
    try {
      if (operator != null) {
        result.setOperatorStateManagedFuture(
            operator.snapshot(checkpointId, timestamp, streams, options));
        if (retain != null)
          result.setOperatorStateManagedFuture(
              retainOperatorSnapshot(result.getOperatorStateManagedFuture(), retain, cancelables));
      }
      resources = keyed.get();
      var runner =
          new SnapshotStrategyRunner<>(
              "StreamFusion stock canonical state",
              new SavepointSnapshotStrategy<>(resources.getSnapshotResources()),
              cancelables,
              resources.getPreferredSnapshotExecutionType());
      started = true;
      result.setKeyedStateManagedFuture(runner.snapshot(checkpointId, timestamp, streams, options));
      return result;
    } catch (Exception | Error failure) {
      if (resources != null && !started) {
        try {
          resources.getSnapshotResources().release();
        } catch (Exception | Error cleanup) {
          failure.addSuppressed(cleanup);
        }
      }
      try {
        result.cancel();
      } catch (Exception | Error cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  private static java.util.concurrent.RunnableFuture<SnapshotResult<OperatorStateHandle>>
      retainOperatorSnapshot(
          java.util.concurrent.RunnableFuture<SnapshotResult<OperatorStateHandle>> future,
          SupplierWithException<AutoCloseable, Exception> retain,
          CloseableRegistry cancelables)
          throws Exception {
    var lease = retain.get();
    var released = new java.util.concurrent.atomic.AtomicBoolean();
    Runnable release =
        () -> {
          if (released.compareAndSet(false, true))
            org.apache.flink.util.IOUtils.closeQuietly(lease);
        };
    try {
      return new AsyncSnapshotCallable<SnapshotResult<OperatorStateHandle>>() {
        @Override
        protected SnapshotResult<OperatorStateHandle> callInternal() throws Exception {
          snapshotCloseableRegistry.registerCloseable(() -> future.cancel(true));
          return org.apache.flink.util.concurrent.FutureUtils.runIfNotDoneAndGet(future);
        }

        @Override
        protected void cleanupProvidedResources() {
          try {
            future.cancel(true);
          } finally {
            release.run();
          }
        }
      }.toAsyncSnapshotFutureTask(cancelables);
    } catch (Exception | Error failure) {
      release.run();
      throw failure;
    }
  }
}

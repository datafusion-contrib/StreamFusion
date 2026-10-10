package tech.streamfusion.operator;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.lookup.AsyncLookupJoinRunner;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.ArrowReader;
import tech.streamfusion.compat.FlinkLookupCompat;
import tech.streamfusion.compat.FlinkStreamOperator;

/**
 * Processing-time lookup join against an <b>async</b> connector, columnar in and out. The async
 * sibling of {@link NativeLookupJoinOperator}: for each probe {@link ArrowBatch} it materialises
 * the rows and drives Flink's own {@link AsyncLookupJoinRunner} — the exact generated pipeline the
 * host's async lookup join executes: pre-filter, key building (field references and constants), the
 * connector's real {@code asyncLookup}, the optional projection/filter on the dimension table, the
 * residual join condition, and LEFT null-padding. Lookups overlap within the configured capacity;
 * updating probes remain on Flink's changelog-aware operator. Byte-identical to the host by
 * construction, since the row-level core <em>is</em> the host's code.
 *
 * <p><b>No mailbox, no in-flight state across batches.</b> Unlike Flink's {@code AsyncWaitOperator}
 * — which keeps lookups in flight across records and therefore needs the operator mailbox, an
 * ordered result queue, and a snapshot/replay of in-flight rows at checkpoint — this operator does
 * all of a batch's concurrent lookups <em>inside</em> {@link #processElement} and blocks on the
 * task thread until they finish (RisingWave's temporal-join and Arroyo's {@code lookup_join} do the
 * same: overlap within a batch, await before emitting). The Arrow batch is already the overlap
 * unit, so a checkpoint barrier — itself a task-thread action — can only run between batches, when
 * nothing is in flight; there is no in-flight state to persist, exactly as for the synchronous
 * operator. The cost is that I/O does not overlap <em>across</em> batches, which is the standard
 * bounded-work-per-batch bargain every synchronous operator makes.
 *
 * <p>In-flight concurrency is bounded before invoking the runner, so exhausting its internal result
 * buffer cannot block the task before timeout handling runs. Each lookup uses Flink's configured
 * timeout and the runner's timeout callback. Results retain probe order, which also satisfies the
 * unordered mode's relaxed output contract.
 */
public class NativeAsyncLookupJoinOperator extends FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {
  private final RichAsyncFunction<RowData, RowData> runner;
  private final RowType probeType;
  private final RowType outputType;
  private final int capacity;
  private final long timeoutMillis;

  private transient boolean runnerNeedsClose;

  private transient BufferAllocator allocator;
  private transient RowDataSerializer probeSerializer;

  public NativeAsyncLookupJoinOperator(
      RichAsyncFunction<RowData, RowData> runner,
      RowType probeType,
      RowType outputType,
      int capacity,
      long timeoutMillis) {
    if (capacity < 1) throw new IllegalArgumentException("Async lookup capacity must be positive");
    this.runner = runner;
    this.probeType = probeType;
    this.outputType = outputType;
    this.capacity = capacity;
    this.timeoutMillis = timeoutMillis;
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
    allocator = NativeAllocator.SHARED;
    probeSerializer = new RowDataSerializer(probeType);
    FunctionUtils.setFunctionRuntimeContext(runner, getRuntimeContext());
    runnerNeedsClose = true;
    try {
      FlinkLookupCompat.open(runner);
    } catch (Exception | Error failure) {
      try {
        closeRunner();
      } catch (Exception | Error cleanupFailure) {
        failure.addSuppressed(cleanupFailure);
      }
      throw failure;
    }
  }

  @Override
  public void close() throws Exception {
    try {
      closeRunner();
    } finally {
      super.close();
    }
  }

  private void closeRunner() throws Exception {
    if (runnerNeedsClose) {
      runnerNeedsClose = false;
      FunctionUtils.closeFunction(runner);
    }
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) throws Exception {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    // Materialise the probe rows off the Arrow buffers before the lookup wait: the reader hands back
    // rows backed by the vectors, which are freed when the batch closes — and the runner's joined
    // rows reference the probe row object, so each row must be a distinct stable copy.
    List<RowData> probes = new ArrayList<>();
    try (VectorSchemaRoot root = element.getValue().root()) {
      ArrowReader reader = ArrowConversion.createArrowReader(root, probeType);
      int rowCount = root.getRowCount();
      for (int i = 0; i < rowCount; i++) {
        probes.add(probeSerializer.copy(reader.read(i)));
      }
    }

    Deque<PendingLookup> pending = new ArrayDeque<>();
    CompletableFuture<Collection<RowData>> failure = new CompletableFuture<>();
    List<RowData> outRows = new ArrayList<>();
    int nextProbe = 0;
    try {
      while (nextProbe < probes.size() || !pending.isEmpty()) {
        while (nextProbe < probes.size() && pending.size() < capacity) {
          if (failure.isDone()) failure.get();
          RowData probe = probes.get(nextProbe++);
          CompletableFuture<Collection<RowData>> result = new CompletableFuture<>();
          PendingLookup lookup = new PendingLookup(probe, result, System.nanoTime());
          pending.addLast(lookup);
          result.whenComplete(
              (rows, error) -> {
                if (error != null) failure.completeExceptionally(error);
              });
          runner.asyncInvoke(probe, FlinkLookupCompat.resultFuture(result));
        }
        PendingLookup lookup = pending.getFirst();
        await(lookup, failure);
        outRows.addAll(lookup.result().get());
        pending.removeFirst();
      }
    } catch (ExecutionException error) {
      Object causeCandidate;
      if ((causeCandidate = error.getCause()) instanceof Exception) {
        Exception cause = ((Exception) causeCandidate);
        throw cause;
      }
      Object causeCandidate2;
      if ((causeCandidate2 = error.getCause()) instanceof Error) {
        Error cause = ((Error) causeCandidate2);
        throw cause;
      }
      throw error;
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      throw error;
    } finally {
      // The connector owns its I/O; cancelling our completion handles prevents stale callbacks
      // from becoming output for a failed batch. Flink closes the connector when the task tears
      // down.
      for (PendingLookup lookup : pending) lookup.result().cancel(true);
    }
    // Insert-only: a processing-time lookup requires an append-only probe, so no row-kind column.
    VectorSchemaRoot out = RowDataArrowConverter.write(outRows, outputType, allocator, false);
    ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(out));
  }

  private void await(PendingLookup lookup, CompletableFuture<Collection<RowData>> failure)
      throws Exception {
    CompletableFuture<Object> completion = CompletableFuture.anyOf(lookup.result(), failure);
    if (timeoutMillis <= 0) {
      completion.get();
      return;
    }
    long remaining =
        TimeUnit.MILLISECONDS.toNanos(timeoutMillis) - (System.nanoTime() - lookup.startedNanos());
    try {
      completion.get(Math.max(0, remaining), TimeUnit.NANOSECONDS);
    } catch (TimeoutException timeout) {
      runner.timeout(lookup.probe(), FlinkLookupCompat.resultFuture(lookup.result()));
      completion.get();
    }
  }

  private static final class PendingLookup {
    private final RowData probe;
    private final CompletableFuture<Collection<RowData>> result;
    private final long startedNanos;

    private PendingLookup(
        RowData probe, CompletableFuture<Collection<RowData>> result, long startedNanos) {
      this.probe = probe;
      this.result = result;
      this.startedNanos = startedNanos;
    }

    public RowData probe() {
      return probe;
    }

    public CompletableFuture<Collection<RowData>> result() {
      return result;
    }

    public long startedNanos() {
      return startedNanos;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      PendingLookup that = (PendingLookup) other;
      return java.util.Objects.equals(probe, that.probe)
          && java.util.Objects.equals(result, that.result)
          && startedNanos == that.startedNanos;
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + java.util.Objects.hashCode(probe);
      result = 31 * result + java.util.Objects.hashCode(result);
      result = 31 * result + Long.hashCode(startedNanos);
      return result;
    }

    @Override
    public String toString() {
      return "PendingLookup[probe="
          + probe
          + ", result="
          + result
          + ", startedNanos="
          + startedNanos
          + "]";
    }
  }
}

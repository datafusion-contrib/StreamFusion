package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.arrow.ArrowConversion;
import io.github.jordepic.streamfusion.arrow.ArrowReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.lookup.AsyncLookupJoinRunner;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;

/**
 * Processing-time lookup join against an <b>async</b> connector, columnar in and out. The async
 * sibling of {@link NativeLookupJoinOperator}: for each probe {@link ArrowBatch} it materialises the
 * rows and drives Flink's own {@link AsyncLookupJoinRunner} — the exact generated pipeline the host's
 * async lookup join executes: pre-filter, key building (field references and constants), the
 * connector's real {@code asyncLookup}, the optional projection/filter on the dimension table, the
 * residual join condition, and LEFT null-padding — firing every probe row's lookup before awaiting any
 * of them, so the batch's lookup I/O overlaps rather than being paid serially. Byte-identical to the
 * host by construction, since the row-level core <em>is</em> the host's code.
 *
 * <p><b>No mailbox, no in-flight state across batches.</b> Unlike Flink's {@code AsyncWaitOperator} —
 * which keeps lookups in flight across records and therefore needs the operator mailbox, an ordered
 * result queue, and a snapshot/replay of in-flight rows at checkpoint — this operator does all of a
 * batch's concurrent lookups <em>inside</em> {@link #processElement} and blocks on the task thread
 * until they finish (RisingWave's temporal-join and Arroyo's {@code lookup_join} do the same: overlap
 * within a batch, await before emitting). The Arrow batch is already the overlap unit, so a checkpoint
 * barrier — itself a task-thread action — can only run between batches, when nothing is in flight;
 * there is no in-flight state to persist, exactly as for the synchronous operator. The cost is that
 * I/O does not overlap <em>across</em> batches, which is the standard bounded-work-per-batch bargain
 * every synchronous operator makes.
 *
 * <p>In-flight concurrency is bounded by the runner's buffer (Flink's
 * {@code table.exec.async-lookup.buffer-capacity}, as on the host): past that many outstanding
 * lookups, firing the next blocks until one completes — the same backpressure the host applies.
 */
public class NativeAsyncLookupJoinOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final AsyncLookupJoinRunner runner;
  private final RowType probeType;
  private final RowType outputType;

  private transient BufferAllocator allocator;
  private transient RowDataSerializer probeSerializer;

  public NativeAsyncLookupJoinOperator(
      AsyncLookupJoinRunner runner, RowType probeType, RowType outputType) {
    this.runner = runner;
    this.probeType = probeType;
    this.outputType = outputType;
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    probeSerializer = new RowDataSerializer(probeType);
    FunctionUtils.setFunctionRuntimeContext(runner, getRuntimeContext());
    FunctionUtils.openFunction(runner, DefaultOpenContext.INSTANCE);
  }

  @Override
  public void close() throws Exception {
    FunctionUtils.closeFunction(runner);
    super.close();
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) throws Exception {
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

    // Fire every row's lookup (the runner joins, filters, and null-pads into each future's rows),
    // then wait for them all; assembly in probe order keeps the emitted batch deterministic.
    List<CompletableFuture<Collection<RowData>>> futures = new ArrayList<>(probes.size());
    for (RowData probe : probes) {
      CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
      futures.add(future);
      runner.asyncInvoke(probe, adapt(future));
    }
    List<RowData> outRows = new ArrayList<>();
    try {
      for (CompletableFuture<Collection<RowData>> future : futures) {
        outRows.addAll(future.get());
      }
    } catch (ExecutionException e) {
      throw e.getCause() instanceof Exception ? (Exception) e.getCause() : e;
    }
    // Insert-only: a processing-time lookup requires an append-only probe, so no row-kind column.
    VectorSchemaRoot out = RowDataArrowConverter.write(outRows, outputType, allocator, false);
    output.collect(new StreamRecord<>(new ArrowBatch(out)));
  }

  /** The runner completes each row's joined results through Flink's {@link ResultFuture} shape. */
  private static ResultFuture<RowData> adapt(CompletableFuture<Collection<RowData>> future) {
    return new ResultFuture<>() {
      @Override
      public void complete(Collection<RowData> result) {
        future.complete(result);
      }

      @Override
      public void completeExceptionally(Throwable error) {
        future.completeExceptionally(error);
      }

      @Override
      public void complete(CollectionSupplier<RowData> supplier) {
        throw new UnsupportedOperationException();
      }
    };
  }
}

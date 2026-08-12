package tech.streamfusion.operator;

import tech.streamfusion.Native;
import tech.streamfusion.planner.NativeConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

/**
 * Re-assembles processing-sized batches in front of a keyed native operator. The columnar exchange
 * splits every source batch by destination channel, and a changelog operator feeding another keyed
 * operator can still compound physical fragmentation. Buffered sub-batches are merged
 * natively into one batch once the row target is reached; a processing-time timer bounds the wait
 * on trickle streams. Coalescing changes only physical chunking, never the record-level changelog:
 * the operator must flush before watermarks, checkpoint barriers, and finish, which preserves every
 * row-before-watermark ordering the fine-grained watermark protocol relies on.
 */
public final class BatchCoalescer implements AutoCloseable {

  /** Test hook: input batches, process-wide, that were merged rather than forwarded alone. */
  private static final LongAdder MERGED = new LongAdder();

  public static long merged() {
    return MERGED.sum();
  }

  /** The configured coalescer for a keyed operator's input, or null when disabled. */
  static BatchCoalescer create(ProcessingTimeService timers, Consumer<VectorSchemaRoot> sink) {
    int targetRows = NativeConfig.exchangeCoalesceRows();
    if (targetRows <= 1) {
      return null;
    }
    return new BatchCoalescer(
        targetRows,
        NativeConfig.exchangeCoalesceLatencyMs(),
        NativeAllocator.SHARED,
        NativeAllocator.DICTIONARIES,
        timers,
        sink);
  }

  private final int targetRows;
  private final long latencyMs;
  private final BufferAllocator allocator;
  private final CDataDictionaryProvider dictionaries;
  private final ProcessingTimeService timers;
  private final Consumer<VectorSchemaRoot> sink;
  private final List<VectorSchemaRoot> pending = new ArrayList<>();
  private int pendingRows;
  private boolean timerArmed;

  BatchCoalescer(
      int targetRows,
      long latencyMs,
      BufferAllocator allocator,
      CDataDictionaryProvider dictionaries,
      ProcessingTimeService timers,
      Consumer<VectorSchemaRoot> sink) {
    this.targetRows = targetRows;
    this.latencyMs = latencyMs;
    this.allocator = allocator;
    this.dictionaries = dictionaries;
    this.timers = timers;
    this.sink = sink;
  }

  /** Buffers or forwards a batch; ownership of {@code root} passes to the coalescer. */
  void add(VectorSchemaRoot root) {
    int rows = root.getRowCount();
    if (rows == 0) {
      root.close();
      return;
    }
    if (pending.isEmpty() && rows >= targetRows) {
      sink.accept(root);
      return;
    }
    pending.add(root);
    pendingRows += rows;
    if (pendingRows >= targetRows) {
      flush();
      return;
    }
    if (!timerArmed && latencyMs > 0) {
      timerArmed = true;
      timers.registerTimer(
          timers.getCurrentProcessingTime() + latencyMs,
          timestamp -> {
            timerArmed = false;
            flush();
          });
    }
  }

  /** Delivers everything pending as one batch; the caller invokes this before any watermark, barrier, or finish. */
  void flush() {
    if (pending.isEmpty()) {
      return;
    }
    VectorSchemaRoot batch = pending.size() == 1 ? pending.get(0) : merge();
    pending.clear();
    pendingRows = 0;
    sink.accept(batch);
  }

  private VectorSchemaRoot merge() {
    int count = pending.size();
    MERGED.add(count);
    long[] arrayAddresses = new long[count];
    long[] schemaAddresses = new long[count];
    ArrowArray[] inArrays = new ArrowArray[count];
    ArrowSchema[] inSchemas = new ArrowSchema[count];
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      for (int i = 0; i < count; i++) {
        VectorSchemaRoot root = pending.get(i);
        BufferAllocator rootAllocator =
            root.getFieldVectors().isEmpty()
                ? allocator
                : root.getFieldVectors().get(0).getAllocator();
        inArrays[i] = ArrowArray.allocateNew(rootAllocator);
        inSchemas[i] = ArrowSchema.allocateNew(rootAllocator);
        Data.exportVectorSchemaRoot(rootAllocator, root, dictionaries, inArrays[i], inSchemas[i]);
        arrayAddresses[i] = inArrays[i].memoryAddress();
        schemaAddresses[i] = inSchemas[i].memoryAddress();
      }
      Native.concatBatches(
          arrayAddresses, schemaAddresses, outArray.memoryAddress(), outSchema.memoryAddress());
      return Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
    } finally {
      for (int i = 0; i < count; i++) {
        if (inArrays[i] != null) {
          inArrays[i].close();
        }
        if (inSchemas[i] != null) {
          inSchemas[i].close();
        }
        pending.get(i).close();
      }
    }
  }

  @Override
  public void close() {
    for (VectorSchemaRoot root : pending) {
      root.close();
    }
    pending.clear();
    pendingRows = 0;
  }
}

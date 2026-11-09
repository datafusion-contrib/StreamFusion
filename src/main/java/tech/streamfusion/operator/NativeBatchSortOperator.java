package tech.streamfusion.operator;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import tech.streamfusion.Native;
import tech.streamfusion.compat.FlinkStreamOperator;

/** Bounded columnar sort; input demand returns control to Flink instead of awaiting its mailbox. */
public final class NativeBatchSortOperator extends FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch>, BoundedOneInput {
  private final int[] columns;
  private final int[] flags;
  private final int batchRows;
  private transient long handle;
  private transient Schema inputSchema;
  private transient boolean finished;

  public NativeBatchSortOperator(int[] columns, int[] flags, int batchRows) {
    this.columns = columns.clone();
    this.flags = flags.clone();
    this.batchRows = batchRows;
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    VectorSchemaRoot input = element.getValue().root();
    try {
      if (finished) throw new IllegalStateException("batch sort input already ended");
      ColumnarRecordMetrics.countIngested(getMetricGroup(), input.getRowCount());
      BufferAllocator allocator =
          input.getFieldVectors().isEmpty()
              ? NativeAllocator.SHARED
              : input.getFieldVectors().get(0).getAllocator();
      if (handle == 0) {
        inputSchema = input.getSchema();
        try (ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
          Data.exportSchema(allocator, inputSchema, NativeAllocator.DICTIONARIES, schema);
          try {
            handle =
                Native.createBoundedSorter(
                    schema.memoryAddress(),
                    columns,
                    flags,
                    getContainingTask()
                        .getEnvironment()
                        .getIOManager()
                        .getSpillingDirectoriesPaths(),
                    batchRows,
                    Math.min(64L << 20, Math.max(64L << 10, TaskOffHeapMemory.capacityBytes() / 8)),
                    tech.streamfusion.compat.RuntimeCompat.taskName(getRuntimeContext()));
          } finally {
            schema.release();
          }
        }
      } else if (!inputSchema.equals(input.getSchema())) {
        throw new IllegalStateException("batch sort input schema changed");
      }
      try (ArrowArray array = ArrowArray.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, input, NativeAllocator.DICTIONARIES, array);
        try {
          Native.pushBoundedSorter(handle, array.memoryAddress());
        } finally {
          array.release();
        }
      }
    } finally {
      input.close();
    }
    drain(false);
  }

  @Override
  public void endInput() {
    if (finished) return;
    finished = true;
    if (handle == 0) return;
    Native.finishBoundedSorter(handle);
    drain(true);
  }

  private void drain(boolean endOfInput) {
    while (true) {
      if (Thread.currentThread().isInterrupted()) {
        throw new java.util.concurrent.CancellationException("batch sort task interrupted");
      }
      try (ArrowArray array = ArrowArray.allocateNew(NativeAllocator.SHARED)) {
        int status = Native.nextBoundedSorter(handle, array.memoryAddress(), 0);
        if (status == 2) continue;
        if (status == -1) return;
        if (status == 0) {
          if (endOfInput) throw new IllegalStateException("batch sort requested input after EOF");
          return;
        }
        VectorSchemaRoot result = VectorSchemaRoot.create(inputSchema, NativeAllocator.SHARED);
        try {
          Data.importIntoVectorSchemaRoot(
              NativeAllocator.SHARED, array, result, NativeAllocator.DICTIONARIES);
        } catch (Throwable failure) {
          result.close();
          throw failure;
        }
        ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(result));
      }
    }
  }

  @Override
  public void close() throws Exception {
    try {
      if (handle != 0) {
        long closing = handle;
        handle = 0;
        Native.closeBoundedSorter(closing);
      }
    } finally {
      super.close();
    }
  }
}

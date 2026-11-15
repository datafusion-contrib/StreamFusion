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
import tech.streamfusion.operator.batch.BoundedArrowOutput;

/**
 * Persistent bounded Arrow execution shared by operators with different plans and result schemas.
 */
public abstract class NativeBoundedBatchOperator extends FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch>, BoundedOneInput {
  private transient long handle;
  private transient Schema inputSchema;
  private transient Schema resultSchema;
  private transient boolean finished;
  private transient volatile long[] executionStats;

  protected abstract long createTask(long schemaAddress);

  protected abstract String metricPrefix();

  protected Schema outputSchema(Schema input) {
    return input;
  }

  protected Schema emptyInputSchema() {
    return null;
  }

  private void initialize(Schema incoming, BufferAllocator allocator) {
    inputSchema = incoming;
    resultSchema = outputSchema(incoming);
    try (ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportSchema(allocator, incoming, NativeAllocator.DICTIONARIES, schema);
      try {
        handle = createTask(schema.memoryAddress());
      } finally {
        schema.release();
      }
    }
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
    executionStats = new long[3];
    getMetricGroup().gauge(metricPrefix() + "SpillCount", () -> executionStats[0]);
    getMetricGroup().gauge(metricPrefix() + "SpilledBytes", () -> executionStats[1]);
    getMetricGroup().gauge(metricPrefix() + "ReservedBytes", () -> executionStats[2]);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    VectorSchemaRoot input = element.getValue().root();
    try {
      if (finished) throw new IllegalStateException("bounded task input already ended");
      ColumnarRecordMetrics.countIngested(getMetricGroup(), input.getRowCount());
      BufferAllocator allocator =
          input.getFieldVectors().isEmpty()
              ? NativeAllocator.SHARED
              : input.getFieldVectors().get(0).getAllocator();
      if (handle == 0) {
        initialize(input.getSchema(), allocator);
      } else if (!inputSchema.equals(input.getSchema())) {
        throw new IllegalStateException("bounded task input schema changed");
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
    executionStats = Native.boundedSorterExecutionStats(handle);
  }

  @Override
  public void endInput() {
    if (finished) return;
    finished = true;
    if (handle == 0) {
      Schema empty = emptyInputSchema();
      if (empty == null) return;
      initialize(empty, NativeAllocator.SHARED);
    }
    Native.finishBoundedSorter(handle);
    drain(true);
    executionStats = Native.boundedSorterExecutionStats(handle);
  }

  private void drain(boolean endOfInput) {
    while (true) {
      if (Thread.currentThread().isInterrupted()) {
        throw new java.util.concurrent.CancellationException("bounded task interrupted");
      }
      try (ArrowArray array = ArrowArray.allocateNew(NativeAllocator.SHARED)) {
        int status = Native.nextBoundedSorter(handle, array.memoryAddress(), 0);
        if (status == 2) continue;
        if (status == -1) return;
        if (status == 0) {
          if (endOfInput) throw new IllegalStateException("bounded task requested input after EOF");
          return;
        }
        ColumnarRecordMetrics.emit(
            output, getMetricGroup(), BoundedArrowOutput.importBatch(handle, array, resultSchema));
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

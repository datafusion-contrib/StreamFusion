package tech.streamfusion.operator.batch;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import tech.streamfusion.Native;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ColumnarRecordMetrics;
import tech.streamfusion.operator.NativeAllocator;

/** Task-local EOF, input-demand and output ownership for bounded native execution. */
@org.apache.flink.annotation.Internal
public final class BoundedExecution implements AutoCloseable {
  private final LongUnaryOperator factory;
  private final Function<Schema, Schema> outputSchema;
  private final Supplier<Schema> emptySchema;
  private final Consumer<ArrowBatch> emit;
  private final OperatorMetricGroup metrics;
  private long handle;
  private Schema inputSchema;
  private Schema resultSchema;
  private boolean finished;
  private volatile long[] statistics = new long[3];

  public BoundedExecution(
      LongUnaryOperator factory,
      Function<Schema, Schema> outputSchema,
      Supplier<Schema> emptySchema,
      Consumer<ArrowBatch> emit,
      OperatorMetricGroup metrics) {
    this.factory = factory;
    this.outputSchema = outputSchema;
    this.emptySchema = emptySchema;
    this.emit = emit;
    this.metrics = metrics;
  }

  private void initialize(Schema incoming, BufferAllocator allocator) {
    inputSchema = incoming;
    resultSchema = outputSchema.apply(incoming);
    try (ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportSchema(allocator, incoming, NativeAllocator.DICTIONARIES, schema);
      try {
        handle = factory.applyAsLong(schema.memoryAddress());
      } finally {
        schema.release();
      }
    }
  }

  public void accept(VectorSchemaRoot input) {
    try {
      if (finished) throw new IllegalStateException("bounded task input already ended");
      ColumnarRecordMetrics.countIngested(metrics, input.getRowCount());
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
    statistics = Native.boundedSorterExecutionStats(handle);
  }

  public void endInput() {
    if (finished) return;
    finished = true;
    if (handle == 0) {
      Schema empty = emptySchema.get();
      if (empty == null) return;
      initialize(empty, NativeAllocator.SHARED);
    }
    Native.finishBoundedSorter(handle);
    drain(true);
    statistics = Native.boundedSorterExecutionStats(handle);
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
        emit.accept(BoundedArrowOutput.importBatch(handle, array, resultSchema));
      }
    }
  }

  public long[] statistics() {
    return statistics;
  }

  @Override
  public void close() {
    if (handle != 0) {
      long closing = handle;
      handle = 0;
      Native.closeBoundedSorter(closing);
    }
  }
}

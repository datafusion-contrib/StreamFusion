package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Stateless windowing table function, columnar in and out: the Arrow-batch analog of Flink's {@link
 * org.apache.flink.table.runtime.operators.window.tvf.unslicing.UnsliceWindowAggProcessor}-fed {@code
 * WindowTableFunctionOperator}. Each incoming batch is assigned to its window(s) natively and emitted
 * with {@code window_start}/{@code window_end}/{@code window_time} appended (rows fanned out, one copy
 * per window, for hopping/cumulative). It does no event-time buffering — the downstream window join
 * or aggregate does — so it forwards watermarks unchanged (the default {@link AbstractStreamOperator}
 * behavior); carrying Arrow lets it chain with the native window operators without transposing.
 *
 * <p>An event-time TVF assigns each row by its rowtime column. A **proctime** TVF assigns every row
 * in a batch to the window(s) covering the operator's current processing-time clock (Flink's
 * processing-time assigner uses the clock, not a row value); the downstream window join/rank closes
 * those windows on a processing-time timer. The TVF itself stays stateless either way.
 */
public class NativeWindowTableFunctionOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int timeColumn;
  private final long windowMillis;
  private final long slideMillis;
  private final boolean cumulative;
  private final boolean proctime;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;

  public NativeWindowTableFunctionOperator(
      int timeColumn, long windowMillis, long slideMillis, boolean cumulative, boolean proctime) {
    this.timeColumn = timeColumn;
    this.windowMillis = windowMillis;
    this.slideMillis = slideMillis;
    this.cumulative = cumulative;
    this.proctime = proctime;
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    VectorSchemaRoot in = element.getValue().root();
    // The input batch's buffers belong to the upstream operator's allocator; the C Data export must
    // use that same allocator. The operator's own allocator owns only the imported result.
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    VectorSchemaRoot assigned;
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      long now = proctime ? getProcessingTimeService().getCurrentProcessingTime() : 0;
      Native.assignWindows(
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          outArray.memoryAddress(),
          outSchema.memoryAddress(),
          timeColumn,
          windowMillis,
          slideMillis,
          cumulative,
          proctime,
          now);
      assigned = Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
    } finally {
      in.close(); // the input batch is consumed
    }
    output.collect(new StreamRecord<>(new ArrowBatch(assigned)));
  }
}

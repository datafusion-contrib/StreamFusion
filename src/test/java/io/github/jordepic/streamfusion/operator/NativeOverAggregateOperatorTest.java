package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

/**
 * The columnar OVER operator passes the input columns through and appends the running aggregate,
 * emitting each row (as an Arrow batch) once the watermark passes its rowtime.
 */
class NativeOverAggregateOperatorTest {

  // Input schema [v BIGINT, rt TIMESTAMP_LTZ(3)]; output appends the running SUM (BIGINT).
  private static final RowType INPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new LocalZonedTimestampType(3)},
          new String[] {"v", "rt"});

  @Test
  void emitsRunningSumWithPassthrough() throws Exception {
    NativeOverAggregateOperator operator =
        new NativeOverAggregateOperator(1, 0, new int[0], 0, new int[] {0}, 0, 0);
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(operator, new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.processElement(new StreamRecord<>(batch(allocator, event(10, 0), event(20, 500))));
      harness.processElement(new StreamRecord<>(batch(allocator, event(30, 1000))));
      assertEquals(List.of(), collect(harness)); // nothing before the watermark

      harness.processWatermark(new Watermark(1000));
      // Each input row [v, rt] is passed through with the running SUM appended.
      assertEquals(
          List.of(List.of(10L, 0L, 10L), List.of(20L, 500L, 30L), List.of(30L, 1000L, 60L)),
          collect(harness));
      closeForwarded(harness);
    }
  }

  private static RowData event(long v, long rtMillis) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, v);
    row.setField(1, TimestampData.fromEpochMillis(rtMillis));
    return row;
  }

  private static ArrowBatch batch(BufferAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), INPUT, allocator));
  }

  /** Drains the output as [v, rt-millis, sum] triples. */
  private static List<List<Long>> collect(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> rows = new ArrayList<>();
    for (Object event : harness.getOutput()) {
      if (event instanceof StreamRecord) {
        VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root();
        var v = (org.apache.arrow.vector.BigIntVector) root.getVector(0);
        var rt = (org.apache.arrow.vector.TimeStampNanoVector) root.getVector(1);
        var sum = (org.apache.arrow.vector.BigIntVector) root.getVector(2);
        for (int i = 0; i < root.getRowCount(); i++) {
          rows.add(List.of(v.get(i), rt.get(i) / 1_000_000L, sum.get(i)));
        }
      }
    }
    return rows;
  }

  private static void closeForwarded(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    for (Object event : harness.getOutput()) {
      if (event instanceof StreamRecord) {
        ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root().close();
      }
    }
  }
}

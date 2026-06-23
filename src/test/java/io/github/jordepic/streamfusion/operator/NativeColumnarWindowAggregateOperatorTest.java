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
 * The columnar window operator (Arrow-batch input) produces the same window aggregates the row-fed
 * operator does — proving the columnar input extraction (time nanos→millis, value, keys) is correct.
 */
class NativeColumnarWindowAggregateOperatorTest {

  // Input schema [value BIGINT, rt TIMESTAMP_LTZ(3)]; output [total, window_start, window_end].
  private static final RowType SCHEMA =
      RowType.of(
          new LogicalType[] {new BigIntType(), new LocalZonedTimestampType(3)},
          new String[] {"value", "rt"});

  @Test
  void emitsWindowAggregatesFromArrowBatches() throws Exception {
    NativeColumnarWindowAggregateOperator operator =
        new NativeColumnarWindowAggregateOperator(
            false, 1000, 1000, 1, new int[] {0}, new int[0], new int[0], new int[] {0}, new int[] {0},
            "UTC");
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, RowData> harness =
            new OneInputStreamOperatorTestHarness<>(operator, new ArrowBatchSerializer())) {
      harness.setup();
      harness.open();

      harness.processElement(new StreamRecord<>(batch(allocator, event(1, 0), event(2, 500), event(3, 1000))));
      harness.processWatermark(new Watermark(1000));
      assertEquals(List.of(row(3, 0, 1000)), collect(harness));

      harness.processElement(new StreamRecord<>(batch(allocator, event(4, 1500), event(5, 2500))));
      harness.processWatermark(new Watermark(3000));
      assertEquals(List.of(row(7, 1000, 2000), row(5, 2000, 3000)), collect(harness));
    }
  }

  private static RowData event(long value, long eventTimeMillis) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, value);
    row.setField(1, TimestampData.fromEpochMillis(eventTimeMillis));
    return row;
  }

  private static ArrowBatch batch(BufferAllocator allocator, RowData... rows) {
    VectorSchemaRoot root = RowDataArrowConverter.write(List.of(rows), SCHEMA, allocator);
    return new ArrowBatch(root);
  }

  private static List<Long> row(long total, long start, long end) {
    return List.of(total, start, end);
  }

  private static List<List<Long>> collect(
      OneInputStreamOperatorTestHarness<ArrowBatch, RowData> harness) {
    List<List<Long>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        RowData r = (RowData) ((StreamRecord<?>) event).getValue();
        rows.add(
            List.of(
                r.getLong(0),
                r.getTimestamp(1, 3).getMillisecond(),
                r.getTimestamp(2, 3).getMillisecond()));
      }
    }
    return rows;
  }
}

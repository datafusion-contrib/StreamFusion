package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Comparator;
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
 * The keep-first deduplicate operator emits, per partition key, the minimum-rowtime row once the
 * watermark reaches that rowtime — not the first to arrive. After a key emits, later rows for it are
 * ignored, and a row arriving with a rowtime already below the watermark is dropped as late.
 */
class NativeColumnarDeduplicateOperatorTest {

  // [k BIGINT, v BIGINT, rt TIMESTAMP_LTZ(3)]; partition key column 0, rowtime column 2.
  private static final RowType SCHEMA =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType(), new LocalZonedTimestampType(3)},
          new String[] {"k", "v", "rt"});

  @Test
  void emitsMinimumRowtimeRowPerKeyOnWatermark() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(
                new NativeColumnarDeduplicateOperator(new int[] {0}, 2), new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      // key 1: rows at rt 2000, 0, 800 -> min-rowtime row is (v=20, rt=0). key 2: single (v=40, rt=1000).
      harness.processElement(
          new StreamRecord<>(
              batch(allocator, row(1, 30, 2000), row(2, 40, 1000), row(1, 20, 0), row(1, 25, 800))));

      // Watermark 1000 releases both keys' first rows (rt 0 and 1000 are <= 1000).
      harness.processWatermark(new Watermark(1000));
      assertEquals(List.of(emitted(1, 20), emitted(2, 40)), collect(harness));

      // A later row for the already-emitted key 1 is ignored; a row for key 3 below the watermark is
      // dropped as late; key 3's in-time row becomes its candidate.
      harness.processElement(
          new StreamRecord<>(
              batch(allocator, row(1, 99, 1500), row(3, 7, 300), row(3, 8, 1200))));
      harness.processWatermark(new Watermark(3000));
      assertEquals(List.of(emitted(3, 8)), collect(harness));
    }
  }

  private static RowData row(long k, long v, long rtMillis) {
    GenericRowData row = new GenericRowData(3);
    row.setField(0, k);
    row.setField(1, v);
    row.setField(2, TimestampData.fromEpochMillis(rtMillis));
    return row;
  }

  private static ArrowBatch batch(BufferAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), SCHEMA, allocator));
  }

  private static List<Long> emitted(long k, long v) {
    return List.of(k, v);
  }

  private static List<List<Long>> collect(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData r : RowDataArrowConverter.read(root, SCHEMA)) {
            rows.add(List.of(r.getLong(0), r.getLong(1)));
          }
        }
      }
    }
    rows.sort(Comparator.comparingLong(r -> r.get(0)));
    return rows;
  }
}

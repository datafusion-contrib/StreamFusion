package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

/**
 * The columnar interval-join operator buffers each side per equi-join key and emits a matched pair
 * (left columns then right columns, as an Arrow batch) when the second of its two rows arrives.
 */
class NativeIntervalJoinOperatorTest {

  // Both inputs share the schema [k BIGINT, v BIGINT, rt TIMESTAMP_LTZ(3)].
  private static final RowType INPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType(), new LocalZonedTimestampType(3)},
          new String[] {"k", "v", "rt"});

  @Test
  void emitsPairsWithinTheInterval() throws Exception {
    // a.rt BETWEEN b.rt - 1000 AND b.rt + 1000, equi-key on column 0, rt is column 2.
    NativeIntervalJoinOperator operator =
        new NativeIntervalJoinOperator(new int[] {0}, new int[] {0}, 2, 2, -1000L, 1000L, EncodedPredicate.NONE);
    try (BufferAllocator allocator = new RootAllocator();
        TwoInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch, ArrowBatch> harness =
            new TwoInputStreamOperatorTestHarness<>(operator)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      // Two right rows for key 1; one within range of the coming left row, one out of range.
      harness.processElement2(
          new StreamRecord<>(batch(allocator, row(1, 100, 5500), row(1, 200, 7000))));
      assertEquals(List.of(), collect(harness)); // no left buffered yet

      // A left row (k=1, rt=5000) matches the rt=5500 right row only (delta -500 in [-1000, 1000]).
      harness.processElement1(new StreamRecord<>(batch(allocator, row(1, 10, 5000))));
      assertEquals(
          List.of(List.of(1L, 10L, 5000L, 1L, 100L, 5500L)), collect(harness));

      harness.processBothWatermarks(new Watermark(Long.MAX_VALUE));
      closeForwarded(harness);
    }
  }

  @Test
  void doesNotMatchAcrossKeys() throws Exception {
    NativeIntervalJoinOperator operator =
        new NativeIntervalJoinOperator(new int[] {0}, new int[] {0}, 2, 2, -1000L, 1000L, EncodedPredicate.NONE);
    try (BufferAllocator allocator = new RootAllocator();
        TwoInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch, ArrowBatch> harness =
            new TwoInputStreamOperatorTestHarness<>(operator)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.processElement1(new StreamRecord<>(batch(allocator, row(1, 10, 5000))));
      // Same rowtime, different key — no match.
      harness.processElement2(new StreamRecord<>(batch(allocator, row(2, 100, 5000))));
      assertEquals(List.of(), collect(harness));

      harness.processBothWatermarks(new Watermark(Long.MAX_VALUE));
      closeForwarded(harness);
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
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), INPUT, allocator));
  }

  /** Drains the output as [lk, lv, lrt-millis, rk, rv, rrt-millis] rows. */
  private static List<List<Long>> collect(
      TwoInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> rows = new ArrayList<>();
    for (Object event : harness.getOutput()) {
      if (event instanceof StreamRecord) {
        VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root();
        BigIntVector lk = (BigIntVector) root.getVector(0);
        BigIntVector lv = (BigIntVector) root.getVector(1);
        TimeStampNanoVector lrt = (TimeStampNanoVector) root.getVector(2);
        BigIntVector rk = (BigIntVector) root.getVector(3);
        BigIntVector rv = (BigIntVector) root.getVector(4);
        TimeStampNanoVector rrt = (TimeStampNanoVector) root.getVector(5);
        for (int i = 0; i < root.getRowCount(); i++) {
          rows.add(
              List.of(
                  lk.get(i),
                  lv.get(i),
                  lrt.get(i) / 1_000_000L,
                  rk.get(i),
                  rv.get(i),
                  rrt.get(i) / 1_000_000L));
        }
      }
    }
    return rows;
  }

  private static void closeForwarded(
      TwoInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch, ArrowBatch> harness) {
    for (Object event : harness.getOutput()) {
      if (event instanceof StreamRecord) {
        ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root().close();
      }
    }
  }
}

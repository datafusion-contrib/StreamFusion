package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.arrow.TimestampAccessor;

/**
 * The columnar interval-join operator buffers each side per equi-join key and emits a matched pair
 * (left columns then right columns, as an Arrow batch) when the second of its two rows arrives.
 */
class NativeIntervalJoinOperatorTest {
  private static final int MAX_PARALLELISM = 128;

  // Both inputs share the schema [k BIGINT, v BIGINT, rt TIMESTAMP_LTZ(3)].
  private static final RowType INPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType(), new LocalZonedTimestampType(3)},
          new String[] {"k", "v", "rt"});

  @Test
  void expiredArrivalAndCleanupBoundariesMatchStockFlink() throws Exception {
    for (int kind = 0; kind < 4; kind++) {
      for (boolean rocks : new boolean[] {false, true}) {
        var nativeOperator = new NativeIntervalJoinOperator(
            new int[] {0}, new int[] {0}, 2, 2, 0, 0, kind, INPUT, INPUT,
            EncodedPredicate.NONE, false, new int[] {-1}, MAX_PARALLELISM);
        try (BufferAllocator allocator = new RootAllocator();
            var nativeHarness = keyedHarness(nativeOperator);
            var stock = stockHarness(kind)) {
          if (rocks) nativeHarness.setStateBackend(
              new tech.streamfusion.state.RocksDBNativeStateBackendFactory().createFromConfig(
                  new org.apache.flink.configuration.Configuration(), getClass().getClassLoader()));
          nativeHarness.setup(new ArrowBatchSerializer());
          nativeHarness.open();
          stock.open();
          nativeHarness.processElement1(new StreamRecord<>(batch(allocator, row(1, 10, 100))));
          stock.processElement1(new StreamRecord<>(GenericRowData.of(1L, 10L, 100L)));
          nativeHarness.processBothWatermarks(new Watermark(100));
          stock.processBothWatermarks(new Watermark(100));
          assertEquals(stockRows(stock), collectNullable(nativeHarness));
          nativeHarness.processElement2(new StreamRecord<>(batch(allocator, row(1, 20, 100))));
          stock.processElement2(new StreamRecord<>(GenericRowData.of(1L, 20L, 100L)));
          assertEquals(stockRows(stock), collectNullable(nativeHarness));
          nativeHarness.processBothWatermarks(new Watermark(1000));
          stock.processBothWatermarks(new Watermark(1000));
          assertEquals(stockRows(stock), collectNullable(nativeHarness));
          nativeHarness.processElement1(new StreamRecord<>(batch(allocator, row(1, 30, 100))));
          stock.processElement1(new StreamRecord<>(GenericRowData.of(1L, 30L, 100L)));
          assertEquals(stockRows(stock), collectNullable(nativeHarness));
          nativeHarness.processElement2(new StreamRecord<>(batch(allocator, row(1, 40, 100))));
          stock.processElement2(new StreamRecord<>(GenericRowData.of(1L, 40L, 100L)));
          assertEquals(stockRows(stock), collectNullable(nativeHarness));
          nativeHarness.processElement1(new StreamRecord<>(batch(allocator, row(1, 50, 1100), row(1, 60, 1090))));
          stock.processElement1(new StreamRecord<>(GenericRowData.of(1L, 50L, 1100L)));
          stock.processElement1(new StreamRecord<>(GenericRowData.of(1L, 60L, 1090L)));
          assertEquals(stockRows(stock), collectNullable(nativeHarness));
          nativeHarness.processBothWatermarks(new Watermark(1095));
          stock.processBothWatermarks(new Watermark(1095));
          assertEquals(stockRows(stock), collectNullable(nativeHarness));
          nativeHarness.processElement2(new StreamRecord<>(batch(allocator, row(1, 70, 1090), row(1, 80, 1090))));
          stock.processElement2(new StreamRecord<>(GenericRowData.of(1L, 70L, 1090L)));
          stock.processElement2(new StreamRecord<>(GenericRowData.of(1L, 80L, 1090L)));
          assertEquals(stockRows(stock), collectNullable(nativeHarness));
        }
      }
    }
  }

  @Test
  void nonpositiveSurvivingTimestampCleanupMatchesStockFlink() throws Exception {
    for (boolean rocks : new boolean[] {false, true}) {
      var operator = new NativeIntervalJoinOperator(
          new int[] {0}, new int[] {0}, 2, 2, -100, 100, 3, INPUT, INPUT,
          EncodedPredicate.NONE, false, new int[] {-1}, MAX_PARALLELISM);
      try (BufferAllocator allocator = new RootAllocator();
          var harness = keyedHarness(operator);
          var stock = stockHarness(3, -100, 100)) {
        if (rocks) harness.setStateBackend(
            new tech.streamfusion.state.RocksDBNativeStateBackendFactory().createFromConfig(
                new org.apache.flink.configuration.Configuration(), getClass().getClassLoader()));
        harness.setup(new ArrowBatchSerializer());
        harness.open();
        stock.open();
        harness.processElement1(new StreamRecord<>(batch(allocator, row(1, 10, -50), row(1, 20, 0))));
        stock.processElement1(new StreamRecord<>(GenericRowData.of(1L, 10L, -50L)));
        stock.processElement1(new StreamRecord<>(GenericRowData.of(1L, 20L, 0L)));
        assertEquals(stockRows(stock), collectNullable(harness));
        harness.processBothWatermarks(new Watermark(51));
        stock.processBothWatermarks(new Watermark(51));
        assertEquals(stockRows(stock), collectNullable(harness));
        harness.processElement2(new StreamRecord<>(batch(allocator, row(1, 30, 0))));
        stock.processElement2(new StreamRecord<>(GenericRowData.of(1L, 30L, 0L)));
        assertEquals(stockRows(stock), collectNullable(harness));
        harness.processBothWatermarks(new Watermark(101));
        stock.processBothWatermarks(new Watermark(101));
        assertEquals(stockRows(stock), collectNullable(harness));
      }
    }
  }

  @Test
  void mixedNonpositiveSurvivorsMatchReleasedRocksCleanupInBothNativeBackends() throws Exception {
    // Stock's negative sentinel depends on MapState iteration order. Native cleanup follows
    // serialized Long ordering in released RocksDB, independently of its own storage backend.
    for (long[] times : new long[][] {{-99, -1, 10}, {-99, 0, 10}, {-99, 10, 0}}) {
      for (boolean rocks : new boolean[] {false, true}) {
        var operator = new NativeIntervalJoinOperator(
            new int[] {0}, new int[] {0}, 2, 2, -100, 100, 0, INPUT, INPUT,
            EncodedPredicate.NONE, false, new int[] {-1}, MAX_PARALLELISM);
        try (BufferAllocator allocator = new RootAllocator();
            var harness = keyedHarness(operator);
            var stock = stockHarness(0, -100, 100)) {
          if (rocks) harness.setStateBackend(
              new tech.streamfusion.state.RocksDBNativeStateBackendFactory().createFromConfig(
                  new org.apache.flink.configuration.Configuration(), getClass().getClassLoader()));
          stock.setStateBackend(releasedRocksBackend());
          harness.setup(new ArrowBatchSerializer());
          harness.open();
          stock.open();
          for (long time : times) {
            harness.processElement1(new StreamRecord<>(batch(allocator, row(1, time, time))));
            stock.processElement1(new StreamRecord<>(GenericRowData.of(1L, time, time)));
          }
          harness.processBothWatermarks(new Watermark(2));
          stock.processBothWatermarks(new Watermark(2));
          assertEquals(List.of(), stockRows(stock));
          assertEquals(List.of(), collectNullable(harness));
          long rightTime = times[1] == -1 ? -1 : 0;
          harness.processElement2(new StreamRecord<>(batch(allocator, row(1, 99, rightTime))));
          stock.processElement2(new StreamRecord<>(GenericRowData.of(1L, 99L, rightTime)));
          var expected = stockRows(stock);
          assertEquals(
              List.of(), expected, "released Rocks clears nonpositive surviving timestamps");
          assertEquals(expected, collectNullable(harness), "native rocks=" + rocks);
        }
      }
    }
  }

  @Test
  void releasedHeapAndRocksCleanupDifferForNegativeSurvivingTimestamp() throws Exception {
    for (boolean rocks : new boolean[] {false, true}) {
      try (var stock = stockHarness(0, -100, 100)) {
        if (rocks) stock.setStateBackend(releasedRocksBackend());
        stock.open();
        for (long time : new long[] {-99, -1, 10})
          stock.processElement1(new StreamRecord<>(GenericRowData.of(1L, time, time)));
        stock.processBothWatermarks(new Watermark(2));
        assertEquals(List.of(), stockRows(stock));
        stock.processElement2(new StreamRecord<>(GenericRowData.of(1L, 99L, -1L)));
        assertEquals(rocks ? 0 : 2, stockRows(stock).size());
      }
    }
  }

  private static org.apache.flink.runtime.state.StateBackend releasedRocksBackend() throws Exception {
    // The released backend moved packages between supported Flink versions.
    Class<?> backend;
    try {
      backend = Class.forName("org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend");
    } catch (ClassNotFoundException legacy) {
      backend =
          Class.forName("org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend");
    }
    return (org.apache.flink.runtime.state.StateBackend) backend.getConstructor().newInstance();
  }

  private static KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
      stockHarness(int kind) throws Exception {
    return stockHarness(kind, 0, 0);
  }

  private static KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
      stockHarness(int kind, long lower, long upper) throws Exception {
    var types = org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(
        RowType.of(new BigIntType(), new BigIntType(), new BigIntType()));
    var output = org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(
        RowType.of(new BigIntType(), new BigIntType(), new BigIntType(),
            new BigIntType(), new BigIntType(), new BigIntType()));
    String code =
        "public class AuditIntervalCondition extends"
            + " org.apache.flink.api.common.functions.AbstractRichFunction implements"
            + " org.apache.flink.table.runtime.generated.JoinCondition { public"
            + " AuditIntervalCondition(Object[] refs) {} public boolean"
            + " apply(org.apache.flink.table.data.RowData a, org.apache.flink.table.data.RowData b)"
            + " { return true; }}";
    var function =
        new org.apache.flink.table.runtime.operators.join.interval.IntervalJoinFunction(
            new org.apache.flink.table.runtime.generated.GeneratedJoinCondition(
                "AuditIntervalCondition", code, new Object[0]),
            output,
            new boolean[] {true});
    var kinds = new org.apache.flink.table.runtime.operators.join.FlinkJoinType[] {
      org.apache.flink.table.runtime.operators.join.FlinkJoinType.INNER,
      org.apache.flink.table.runtime.operators.join.FlinkJoinType.LEFT,
      org.apache.flink.table.runtime.operators.join.FlinkJoinType.RIGHT,
      org.apache.flink.table.runtime.operators.join.FlinkJoinType.FULL};
    var interval = new org.apache.flink.table.runtime.operators.join.interval.RowTimeIntervalJoin(
        kinds[kind], lower, upper, 0, 0, types, types, function, 2, 2);
    var operator = new org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator<>(interval);
    var keys = org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(RowType.of(new BigIntType()));
    var serializer = new org.apache.flink.table.runtime.typeutils.RowDataSerializer(RowType.of(new BigIntType()));
    return new KeyedTwoInputStreamOperatorTestHarness<>(operator,
        row -> serializer.toBinaryRow(GenericRowData.of(row.getLong(0))).copy(),
        row -> serializer.toBinaryRow(GenericRowData.of(row.getLong(0))).copy(), keys);
  }

  private static List<List<Long>> stockRows(
      KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> harness) {
    List<List<Long>> output = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord<?>) {
        StreamRecord<?> record = ((StreamRecord<?>) event);

        RowData row = (RowData) record.getValue();
        List<Long> fields = new ArrayList<>();
        for (int column = 0; column < row.getArity(); column++)
          fields.add(row.isNullAt(column) ? null : row.getLong(column));
        output.add(fields);
      }
    }
    return output;
  }

  @Test
  void emitsPairsWithinTheInterval() throws Exception {
    // a.rt BETWEEN b.rt - 1000 AND b.rt + 1000, equi-key on column 0, rt is column 2.
    NativeIntervalJoinOperator operator =
        new NativeIntervalJoinOperator(
            new int[] {0}, new int[] {0}, 2, 2, -1000L, 1000L, 0, INPUT, INPUT, EncodedPredicate.NONE,
            false, new int[] {-1}, MAX_PARALLELISM);
    try (BufferAllocator allocator = new RootAllocator();
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> harness =
            keyedHarness(operator)) {
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
        new NativeIntervalJoinOperator(
            new int[] {0}, new int[] {0}, 2, 2, -1000L, 1000L, 0, INPUT, INPUT, EncodedPredicate.NONE,
            false, new int[] {-1}, MAX_PARALLELISM);
    try (BufferAllocator allocator = new RootAllocator();
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> harness =
            keyedHarness(operator)) {
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

  /**
   * A proctime interval join times each row by the operator's processing-time clock, not the row's
   * time column: the rt values below are deliberately bogus (9999) and ignored. Driving the clock
   * with {@code setProcessingTime}, a right row stamped at 5000 and a left row stamped at 5500 fall
   * within {@code [-1000, 1000]} (delta 500), so they match — deterministic via the controlled clock
   * (proctime is non-deterministic in a real run — see the CLAUDE.md note). The emitted rt columns
   * carry the stamped clock values.
   */
  @Test
  void proctimeMatchesByTheClockIgnoringTheTimeColumn() throws Exception {
    NativeIntervalJoinOperator operator =
        new NativeIntervalJoinOperator(
            new int[] {0}, new int[] {0}, 2, 2, -1000L, 1000L, 0, INPUT, INPUT, EncodedPredicate.NONE,
            true, new int[] {-1}, MAX_PARALLELISM);
    try (BufferAllocator allocator = new RootAllocator();
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> harness =
            keyedHarness(operator)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.setProcessingTime(5000);
      harness.processElement2(new StreamRecord<>(batch(allocator, row(1, 100, 9999))));
      assertEquals(List.of(), collect(harness));

      harness.setProcessingTime(5500);
      harness.processElement1(new StreamRecord<>(batch(allocator, row(1, 10, 9999))));
      assertEquals(List.of(List.of(1L, 10L, 5500L, 1L, 100L, 5000L)), collect(harness));

      harness.setProcessingTime(7000); // past 5500 + max(upper,-lower); buffers drain (no-op for INNER)
      closeForwarded(harness);
    }
  }

  @Test
  void rawKeyedProctimeOuterJoinRearmsCleanupAfterRestore() throws Exception {
    OperatorSubtaskState snapshot;
    try (KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> before =
        rawKeyedProctimeOuterHarness()) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.setProcessingTime(500);
      before.processElement1(new StreamRecord<>(batch(NativeAllocator.SHARED, row(1, 10, 0))));
      snapshot = before.snapshot(1L, 1L);
      closeForwarded(before);
    }

    try (KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> restored =
        rawKeyedProctimeOuterHarness()) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      // No new input: retain the row at horizon equality, then fire its restored cleanup timer.
      restored.setProcessingTime(1500);
      assertEquals(List.of(), collectNullable(restored));
      restored.setProcessingTime(1501);
      assertEquals(
          List.of(java.util.Arrays.asList(1L, 10L, 500L, null, null, null)), collectNullable(restored));
    }
  }

  private static KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
      rawKeyedProctimeOuterHarness() throws Exception {
    return keyedHarness(
        new NativeIntervalJoinOperator(
            new int[] {0},
            new int[] {0},
            2,
            2,
            -1000L,
            1000L,
            1,
            INPUT,
            INPUT,
            EncodedPredicate.NONE,
            true,
            new int[] {-1},
            MAX_PARALLELISM));
  }

  private static KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
      keyedHarness(NativeIntervalJoinOperator operator) throws Exception {
    return new KeyedTwoInputStreamOperatorTestHarness<>(
        operator, batch -> 0, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
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
        TimestampAccessor lrt = new TimestampAccessor(root.getVector(2));
        BigIntVector rk = (BigIntVector) root.getVector(3);
        BigIntVector rv = (BigIntVector) root.getVector(4);
        TimestampAccessor rrt = new TimestampAccessor(root.getVector(5));
        for (int i = 0; i < root.getRowCount(); i++) {
          rows.add(
              List.of(
                  lk.get(i),
                  lv.get(i),
                  lrt.getMillis(i),
                  rk.get(i),
                  rv.get(i),
                  rrt.getMillis(i)));
        }
      }
    }
    return rows;
  }

  private static List<List<Long>> collectNullable(
      TwoInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          BigIntVector lk = (BigIntVector) root.getVector(0);
          BigIntVector lv = (BigIntVector) root.getVector(1);
          TimestampAccessor lrt = new TimestampAccessor(root.getVector(2));
          BigIntVector rk = (BigIntVector) root.getVector(3);
          BigIntVector rv = (BigIntVector) root.getVector(4);
          TimestampAccessor rrt = new TimestampAccessor(root.getVector(5));
          for (int i = 0; i < root.getRowCount(); i++) {
            rows.add(
                java.util.Arrays.asList(
                    nullable(lk, i),
                    nullable(lv, i),
                    nullableTimestamp(lrt, i),
                    nullable(rk, i),
                    nullable(rv, i),
                    nullableTimestamp(rrt, i)));
          }
        }
      }
    }
    return rows;
  }

  private static Long nullable(BigIntVector vector, int index) {
    return vector.isNull(index) ? null : vector.get(index);
  }

  private static Long nullableTimestamp(TimestampAccessor vector, int index) {
    return vector.isNull(index) ? null : vector.getMillis(index);
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

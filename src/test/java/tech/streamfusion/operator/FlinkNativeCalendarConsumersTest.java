package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.window.WindowJoinOperatorBuilder;
import org.apache.flink.table.runtime.operators.rank.window.WindowRankOperatorBuilder;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

class FlinkNativeCalendarConsumersTest {
  private static final RowType INPUT =
      RowType.of(new BigIntType(), new BigIntType(), new TimestampType(3), new TimestampType(3));
  private static final RowType STOCK =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType());

  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String[] calendar :
        List.of(
            new String[] {"America/Los_Angeles", "2024-03-10T02:30:00"},
            new String[] {"America/Los_Angeles", "2024-11-03T02:00:00"},
            new String[] {"America/Los_Angeles", "1883-11-18T12:00:00"},
            new String[] {"Asia/Shanghai", "1969-12-31T23:00:00"},
            new String[] {"Asia/Shanghai", "1991-09-15T02:00:00"}))
      for (boolean rocks : new boolean[] {false, true})
        for (boolean restore : new boolean[] {false, true})
          cases.add(Arguments.of(calendar[0], calendar[1], rocks, restore));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void joinsMatchReleasedCalendarFiringAndLateRows(
      String zone, String endText, boolean rocks, boolean restore) throws Exception {
    long end = local(endText), start = end - 3_600_000;
    long timer = TimeWindowUtil.toEpochMillsForTimer(end - 1, ZoneId.of(zone));
    var stock = stockJoin(zone);
    var nativeRun = nativeJoin(zone, rocks);
    try {
      stock.open();
      nativeRun.setup(new ArrowBatchSerializer());
      nativeRun.open();
      for (long value : new long[] {10, 20}) {
        stock.processElement1(new StreamRecord<>(stockRow(value, start, end)));
        nativeRun.processElement1(new StreamRecord<>(batch(value, start, end)));
      }
      stock.processElement2(new StreamRecord<>(stockRow(100, start, end)));
      nativeRun.processElement2(new StreamRecord<>(batch(100, start, end)));
      watermarks(stock, timer - 1);
      watermarks(nativeRun, timer - 1);
      assertEquals(
          collect(stock.getOutput(), false, false), collect(nativeRun.getOutput(), true, false));
      if (restore) {
        stock.prepareSnapshotPreBarrier(1);
        nativeRun.prepareSnapshotPreBarrier(1);
        OperatorSubtaskState hostSnapshot = stock.snapshot(1, 1),
            nativeSnapshot = nativeRun.snapshot(1, 1);
        stock.close();
        nativeRun.close();
        stock = stockJoin(zone);
        nativeRun = nativeJoin(zone, rocks);
        stock.initializeState(hostSnapshot);
        stock.open();
        nativeRun.setup(new ArrowBatchSerializer());
        nativeRun.initializeState(nativeSnapshot);
        nativeRun.open();
      }
      watermarks(stock, timer);
      watermarks(nativeRun, timer);
      var expected = collect(stock.getOutput(), false, false);
      assertEquals(2, expected.size());
      assertEquals(expected, collect(nativeRun.getOutput(), true, false));
      stock.processElement1(new StreamRecord<>(stockRow(30, start, end)));
      stock.processElement2(new StreamRecord<>(stockRow(300, start, end)));
      nativeRun.processElement1(new StreamRecord<>(batch(30, start, end)));
      nativeRun.processElement2(new StreamRecord<>(batch(300, start, end)));
      watermarks(stock, timer + 1);
      watermarks(nativeRun, timer + 1);
      assertEquals(
          collect(stock.getOutput(), false, false), collect(nativeRun.getOutput(), true, false));
    } finally {
      stock.close();
      nativeRun.close();
    }
  }

  @ParameterizedTest
  @MethodSource("cases")
  void ranksMatchReleasedCalendarFiringAndRecovery(
      String zone, String endText, boolean rocks, boolean restore) throws Exception {
    long end = local(endText), start = end - 3_600_000;
    long timer = TimeWindowUtil.toEpochMillsForTimer(end - 1, ZoneId.of(zone));
    var stock = stockRank(zone);
    var nativeRun = nativeRank(zone, rocks, false);
    try {
      stock.open();
      nativeRun.setup(new ArrowBatchSerializer());
      nativeRun.open();
      for (long value : new long[] {10, 30, 20}) {
        stock.processElement(new StreamRecord<>(stockRow(value, start, end)));
        nativeRun.processElement(new StreamRecord<>(batch(value, start, end)));
      }
      stock.processWatermark(new Watermark(timer - 1));
      nativeRun.processWatermark(new Watermark(timer - 1));
      assertEquals(
          collect(stock.getOutput(), false, true), collect(nativeRun.getOutput(), true, true));
      if (restore) {
        stock.prepareSnapshotPreBarrier(1);
        nativeRun.prepareSnapshotPreBarrier(1);
        var hostSnapshot = stock.snapshot(1, 1);
        var nativeSnapshot = nativeRun.snapshot(1, 1);
        stock.close();
        nativeRun.close();
        stock = stockRank(zone);
        nativeRun = nativeRank(zone, rocks, false);
        stock.initializeState(hostSnapshot);
        stock.open();
        nativeRun.setup(new ArrowBatchSerializer());
        nativeRun.initializeState(nativeSnapshot);
        nativeRun.open();
      }
      stock.processWatermark(new Watermark(timer));
      nativeRun.processWatermark(new Watermark(timer));
      var expected = collect(stock.getOutput(), false, true);
      assertEquals(2, expected.size());
      assertEquals(expected, collect(nativeRun.getOutput(), true, true));
      stock.processElement(new StreamRecord<>(stockRow(99, start, end)));
      nativeRun.processElement(new StreamRecord<>(batch(99, start, end)));
      stock.processWatermark(new Watermark(timer + 1));
      nativeRun.processWatermark(new Watermark(timer + 1));
      assertEquals(
          collect(stock.getOutput(), false, true), collect(nativeRun.getOutput(), true, true));
    } finally {
      stock.close();
      nativeRun.close();
    }
  }

  @ParameterizedTest
  @MethodSource("cases")
  void processingTimeRankRearmsCalendarDeadline(
      String zone, String endText, boolean rocks, boolean restore) throws Exception {
    long end = local(endText), start = end - 3_600_000;
    long timer = TimeWindowUtil.toEpochMillsForTimer(end - 1, ZoneId.of(zone));
    var nativeRun = nativeRank(zone, rocks, true);
    try {
      nativeRun.setup(new ArrowBatchSerializer());
      nativeRun.setProcessingTime(timer - 2);
      nativeRun.open();
      nativeRun.processElement(new StreamRecord<>(batch(30, start, end)));
      nativeRun.setProcessingTime(timer - 1);
      assertEquals(List.of(), collect(nativeRun.getOutput(), true, true));
      if (restore) {
        var snapshot = nativeRun.snapshot(1, 1);
        nativeRun.close();
        nativeRun = nativeRank(zone, rocks, true);
        nativeRun.setup(new ArrowBatchSerializer());
        nativeRun.initializeState(snapshot);
        nativeRun.setProcessingTime(timer - 1);
        nativeRun.open();
      }
      nativeRun.setProcessingTime(timer);
      assertEquals(List.of(List.of(30L, 1L)), collect(nativeRun.getOutput(), true, true));
    } finally {
      nativeRun.close();
    }
  }

  @ParameterizedTest
  @MethodSource("cases")
  void processingTimeJoinRearmsCalendarDeadline(
      String zone, String endText, boolean rocks, boolean restore) throws Exception {
    long end = local(endText), start = end - 3_600_000;
    long timer = TimeWindowUtil.toEpochMillsForTimer(end - 1, ZoneId.of(zone));
    var nativeRun = nativeJoin(zone, rocks, true);
    try {
      nativeRun.setup(new ArrowBatchSerializer());
      nativeRun.setProcessingTime(timer - 2);
      nativeRun.open();
      nativeRun.processElement1(new StreamRecord<>(batch(10, start, end)));
      nativeRun.processElement2(new StreamRecord<>(batch(100, start, end)));
      nativeRun.setProcessingTime(timer - 1);
      assertEquals(List.of(), collect(nativeRun.getOutput(), true, false));
      if (restore) {
        var snapshot = nativeRun.snapshot(1, 1);
        nativeRun.close();
        nativeRun = nativeJoin(zone, rocks, true);
        nativeRun.setup(new ArrowBatchSerializer());
        nativeRun.initializeState(snapshot);
        nativeRun.setProcessingTime(timer - 1);
        nativeRun.open();
      }
      nativeRun.setProcessingTime(timer);
      assertEquals(List.of(List.of(10L, 100L)), collect(nativeRun.getOutput(), true, false));
    } finally {
      nativeRun.close();
    }
  }

  @ParameterizedTest
  @MethodSource("cases")
  void deduplicationMatchesReleasedCalendarFiring(
      String zone, String endText, boolean rocks, boolean restore) throws Exception {
    long end = local(endText), start = end - 3_600_000;
    long timer = TimeWindowUtil.toEpochMillsForTimer(end - 1, ZoneId.of(zone));
    var stock = stockDedup(zone);
    var nativeRun = nativeDedup(zone, rocks);
    try {
      stock.open();
      nativeRun.setup(new ArrowBatchSerializer());
      nativeRun.open();
      for (long value : new long[] {10, 30, 20}) {
        stock.processElement(new StreamRecord<>(stockRow(value, start, end)));
        nativeRun.processElement(new StreamRecord<>(batch(value, start, end)));
      }
      stock.processWatermark(new Watermark(timer - 1));
      nativeRun.processWatermark(new Watermark(timer - 1));
      assertEquals(
          collectDedup(stock.getOutput(), false), collectDedup(nativeRun.getOutput(), true));
      if (restore) {
        stock.prepareSnapshotPreBarrier(1);
        nativeRun.prepareSnapshotPreBarrier(1);
        var hostSnapshot = stock.snapshot(1, 1);
        var nativeSnapshot = nativeRun.snapshot(1, 1);
        stock.close();
        nativeRun.close();
        stock = stockDedup(zone);
        nativeRun = nativeDedup(zone, rocks);
        stock.initializeState(hostSnapshot);
        stock.open();
        nativeRun.setup(new ArrowBatchSerializer());
        nativeRun.initializeState(nativeSnapshot);
        nativeRun.open();
      }
      stock.processWatermark(new Watermark(timer));
      nativeRun.processWatermark(new Watermark(timer));
      var expected = collectDedup(stock.getOutput(), false);
      assertEquals(List.of(30L), expected);
      assertEquals(expected, collectDedup(nativeRun.getOutput(), true));
    } finally {
      stock.close();
      nativeRun.close();
    }
  }

  private static List<Long> collectDedup(Queue<Object> output, boolean nativeOutput) {
    var rows = new ArrayList<Long>();
    for (Object event; (event = output.poll()) != null; )
      if (event instanceof StreamRecord<?>) {
        Object value = ((StreamRecord<?>) event).getValue();
        if (nativeOutput) {
          try (var root = ((ArrowBatch) value).root()) {
            var vector = (org.apache.arrow.vector.BigIntVector) root.getVector(1);
            for (int i = 0; i < root.getRowCount(); i++) rows.add(vector.get(i));
          }
        } else rows.add(((RowData) value).getLong(1));
      }
    rows.sort(Long::compare);
    return rows;
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stockDedup(
      String zone) throws Exception {
    var key = new LongKey(0);
    var operator =
        org.apache.flink.table.runtime.operators.deduplicate.window
            .RowTimeWindowDeduplicateOperatorBuilder.builder()
            .inputSerializer(new RowDataSerializer(STOCK))
            .keySerializer((PagedTypeSerializer<RowData>) key.getProducedType().toSerializer())
            .keepLastRow(true)
            .rowtimeIndex(1)
            .windowEndIndex(3)
            .shiftTimeZone(ZoneId.of(zone))
            .build();
    return new KeyedOneInputStreamOperatorTestHarness<>(operator, key, key.getProducedType());
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      nativeDedup(String zone, boolean rocks) throws Exception {
    var operator =
        new NativeColumnarWindowRankOperator(
                2,
                3,
                new int[] {0},
                new int[] {-1},
                new int[] {1},
                new int[] {0},
                new int[] {0},
                1,
                false,
                0L,
                false,
                3_600_000,
                3_600_000,
                false,
                INPUT,
                128,
                true)
            .withCalendarZone(zone);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            operator, batch -> 0, Types.INT, 128, 1, 0);
    if (rocks)
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), FlinkNativeCalendarConsumersTest.class.getClassLoader()));
    return harness;
  }

  private static long local(String value) {
    return LocalDateTime.parse(value).toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  private static RowData stockRow(long value, long start, long end) {
    return GenericRowData.of(1L, value, start, end);
  }

  private static ArrowBatch batch(long value, long start, long end) {
    return new ArrowBatch(
        RowDataArrowConverter.write(
            List.of(
                GenericRowData.of(
                    1L,
                    value,
                    TimestampData.fromEpochMillis(start),
                    TimestampData.fromEpochMillis(end))),
            INPUT,
            NativeAllocator.SHARED));
  }

  private static void watermarks(
      KeyedTwoInputStreamOperatorTestHarness<?, ?, ?, ?> harness, long time) throws Exception {
    harness.processWatermark1(new Watermark(time));
    harness.processWatermark2(new Watermark(time));
  }

  private static List<List<Long>> collect(
      Queue<Object> output, boolean nativeOutput, boolean rank) {
    var rows = new ArrayList<List<Long>>();
    for (Object event; (event = output.poll()) != null; )
      if (event instanceof StreamRecord<?>) {
        Object value = ((StreamRecord<?>) event).getValue();
        if (nativeOutput) {
          try (var root = ((ArrowBatch) value).root()) {
            var v = (org.apache.arrow.vector.BigIntVector) root.getVector(1);
            var r = (org.apache.arrow.vector.BigIntVector) root.getVector(rank ? 4 : 5);
            for (int i = 0; i < root.getRowCount(); i++) rows.add(List.of(v.get(i), r.get(i)));
          }
        } else {
          RowData row = (RowData) value;
          rows.add(List.of(row.getLong(1), row.getLong(rank ? 4 : 5)));
        }
      }
    rows.sort(java.util.Comparator.comparing(Object::toString));
    return rows;
  }

  private static KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
      stockJoin(String zone) throws Exception {
    String code =
        "public class CalendarJoinCondition extends"
            + " org.apache.flink.api.common.functions.AbstractRichFunction implements"
            + " org.apache.flink.table.runtime.generated.JoinCondition { public"
            + " CalendarJoinCondition(Object[] refs) {} public boolean"
            + " apply(org.apache.flink.table.data.RowData a, org.apache.flink.table.data.RowData b)"
            + " { return true; }}";
    var operator =
        WindowJoinOperatorBuilder.builder()
            .leftSerializer(new RowDataSerializer(STOCK))
            .rightSerializer(new RowDataSerializer(STOCK))
            .generatedJoinCondition(
                new GeneratedJoinCondition("CalendarJoinCondition", code, new Object[0]))
            .leftWindowEndIndex(3)
            .rightWindowEndIndex(3)
            .filterNullKeys(new boolean[] {true})
            .joinType(FlinkJoinType.INNER)
            .withShiftTimezone(ZoneId.of(zone))
            .build();
    var key = new LongKey(0);
    return new KeyedTwoInputStreamOperatorTestHarness<>(operator, key, key, key.getProducedType());
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stockRank(
      String zone) throws Exception {
    var key = new LongKey(0);
    var comparator =
        new GeneratedRecordComparator("", "", new Object[0]) {
          @Override
          public RecordComparator newInstance(ClassLoader loader) {
            return (left, right) -> Long.compare(right.getLong(0), left.getLong(0));
          }
        };
    var builder =
        WindowRankOperatorBuilder.builder()
            .inputSerializer(new RowDataSerializer(STOCK))
            .keySerializer((PagedTypeSerializer<RowData>) key.getProducedType().toSerializer())
            .sortKeySelector(new LongKey(1))
            .sortKeyComparator(comparator)
            .outputRankNumber(true)
            .rankStart(1)
            .rankEnd(2)
            .windowEndIndex(3)
            .shiftTimeZone(ZoneId.of(zone));
    try {
      builder.getClass().getMethod("withEventTime", Boolean.class).invoke(builder, true);
    } catch (NoSuchMethodException legacyEventTimeOnly) {
      // Flink 1.18's builder supports event time exclusively.
    }
    var operator = builder.build();
    return new KeyedOneInputStreamOperatorTestHarness<>(operator, key, key.getProducedType());
  }

  private static KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
      nativeJoin(String zone, boolean rocks) throws Exception {
    return nativeJoin(zone, rocks, false);
  }

  private static KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
      nativeJoin(String zone, boolean rocks, boolean proctime) throws Exception {
    var operator =
        new NativeWindowJoinOperator(
                new int[] {0},
                new int[] {0},
                2,
                3,
                2,
                3,
                0,
                INPUT,
                INPUT,
                EncodedPredicate.NONE,
                proctime,
                3_600_000,
                3_600_000,
                false,
                new int[] {-1},
                128,
                0)
            .withCalendarZone(zone);
    var harness =
        new KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>(
            operator, batch -> 0, batch -> 0, Types.INT, 128, 1, 0);
    if (rocks)
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), FlinkNativeCalendarConsumersTest.class.getClassLoader()));
    return harness;
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeRank(
      String zone, boolean rocks, boolean proctime) throws Exception {
    var operator =
        new NativeColumnarWindowRankOperator(
            2,
            3,
            new int[] {0},
            new int[] {-1},
            new int[] {1},
            new int[] {0},
            new int[] {0},
            2,
            true,
            zone,
            proctime,
            3_600_000,
            3_600_000,
            false,
            INPUT,
            128);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            operator, batch -> 0, Types.INT, 128, 1, 0);
    if (rocks)
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), FlinkNativeCalendarConsumersTest.class.getClassLoader()));
    return harness;
  }

  private static class LongKey implements RowDataKeySelector {
    private final int index;
    private final RowDataSerializer serializer = new RowDataSerializer(new BigIntType());

    LongKey(int index) {
      this.index = index;
    }

    @Override
    public RowData getKey(RowData row) {
      return serializer.toBinaryRow(GenericRowData.of(row.getLong(index))).copy();
    }

    @Override
    public InternalTypeInfo<RowData> getProducedType() {
      return InternalTypeInfo.ofFields(new BigIntType());
    }

    public RowDataKeySelector copy() {
      return new LongKey(index);
    }
  }
}

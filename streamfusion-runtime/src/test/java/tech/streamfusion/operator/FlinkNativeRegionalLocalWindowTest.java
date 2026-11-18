package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static tech.streamfusion.operator.FlinkNativeFixedCalendarWindowTest.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkNativeRegionalLocalWindowTest {
  static final RowType PARTIAL = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());

  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String scenario : List.of("gap", "overlap", "negative", "history", "utc", "fixed"))
      for (String shape : List.of("tumble", "hop", "cumulate"))
        for (boolean barrier : new boolean[] {false, true})
          cases.add(Arguments.of(scenario, shape, barrier));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void localPartialDeliveryMatchesReleasedBuffer(String scenario, String shape, boolean barrier)
      throws Exception {
    String zone =
        scenario.equals("negative")
            ? "Asia/Shanghai"
            : scenario.equals("utc")
                ? "UTC"
                : scenario.equals("fixed") ? "GMT+05:45" : "America/Los_Angeles";
    long size =
        scenario.equals("history")
            ? (shape.equals("cumulate") ? 3 : shape.equals("hop") ? 2 : 1)
            : 86_400_000;
    long slide = scenario.equals("history") ? 1 : shape.equals("tumble") ? size : size / 4;
    long[] times =
        scenario.equals("history")
            ? times("1883-11-18T19:59:59.999Z", "1883-11-18T20:07:02Z")
            : scenario.equals("negative")
                ? times("1969-12-31T23:30:00Z", "1970-01-01T05:30:00Z")
                : scenario.equals("overlap")
                    ? times("2024-11-03T08:30:00Z", "2024-11-03T15:30:00Z")
                    : times("2024-03-10T09:30:00Z", "2024-03-11T10:30:00Z");
    try (var allocator = new RootAllocator();
        var stock = stockLocal(zone, shape, size, slide);
        var nativeLocal = localHarness(zone, slide)) {
      stock.setup(new RowDataSerializer(PARTIAL));
      stock.open();
      nativeLocal.setup(new ArrowBatchSerializer());
      nativeLocal.open();
      for (long time : times) {
        var row = GenericRowData.of(1L, TimestampData.fromEpochMillis(time));
        stock.processElement(new StreamRecord<>(row));
        nativeLocal.processElement(
            new StreamRecord<>(
                new ArrowBatch(RowDataArrowConverter.write(List.of(row), INPUT, allocator))));
      }
      if (barrier) {
        stock.prepareSnapshotPreBarrier(1);
        nativeLocal.prepareSnapshotPreBarrier(1);
      }
      compare(zone, stock.getOutput(), nativeLocal.getOutput(), "input/barrier");
      long[] ticks =
          scenario.equals("history")
              ? times(
                  "1883-11-18T20:07:02.001Z",
                  "1883-11-18T20:59:59.999Z",
                  "1970-01-01T00:00:00Z",
                  "1970-01-01T00:00:00.001Z")
              : new long[] {
                times[0] - 1,
                times[0],
                times[0],
                times[1],
                times[1] + 86_400_000,
                times[1] + 86_400_001
              };
      for (long tick : ticks) {
        stock.processWatermark(new Watermark(tick));
        nativeLocal.processWatermark(new Watermark(tick));
        compare(zone, stock.getOutput(), nativeLocal.getOutput(), "watermark=" + tick);
      }
      stock.prepareSnapshotPreBarrier(2);
      nativeLocal.prepareSnapshotPreBarrier(2);
      compare(zone, stock.getOutput(), nativeLocal.getOutput(), "final barrier");
    }
  }

  @ParameterizedTest
  @MethodSource("cases")
  void independentLocalGlobalPipelinesPreserveWatermarkDelivery(
      String scenario, String shape, boolean barrier) throws Exception {
    String zone =
        scenario.equals("negative")
            ? "Asia/Shanghai"
            : scenario.equals("utc")
                ? "UTC"
                : scenario.equals("fixed") ? "GMT+05:45" : "America/Los_Angeles";
    long size =
        scenario.equals("history")
            ? (shape.equals("cumulate") ? 3 : shape.equals("hop") ? 2 : 1)
            : 86_400_000;
    long slide = scenario.equals("history") ? 1 : shape.equals("tumble") ? size : size / 4;
    long[] arrivals =
        scenario.equals("history")
            ? times("1883-11-18T19:59:59.999Z", "1883-11-18T20:07:02Z")
            : scenario.equals("negative")
                ? times("1969-12-31T23:30:00Z", "1970-01-01T05:30:00Z")
                : scenario.equals("overlap")
                    ? times("2024-11-03T08:30:00Z", "2024-11-03T15:30:00Z")
                    : times("2024-03-10T09:30:00Z", "2024-03-11T10:30:00Z");
    for (boolean rocks : new boolean[] {false, true}) {
      try (var allocator = new RootAllocator();
          var stockLocal = stockLocal(zone, shape, size, slide);
          var stockGlobal = stockGlobal(zone, shape, size, slide);
          var nativeLocal = localHarness(zone, slide);
          var nativeGlobal = globalHarness(zone, shape, rocks, size, slide)) {
        stockLocal.setup(new RowDataSerializer(PARTIAL));
        stockLocal.open();
        stockGlobal.setup(new RowDataSerializer(OUTPUT));
        stockGlobal.open();
        nativeLocal.setup(new ArrowBatchSerializer());
        nativeLocal.open();
        nativeGlobal.setup(new ArrowBatchSerializer());
        nativeGlobal.open();
        for (long arrival : arrivals) {
          var row = GenericRowData.of(1L, TimestampData.fromEpochMillis(arrival));
          stockLocal.processElement(new StreamRecord<>(row));
          nativeLocal.processElement(
              new StreamRecord<>(
                  new ArrowBatch(RowDataArrowConverter.write(List.of(row), INPUT, allocator))));
        }
        if (barrier) {
          stockLocal.prepareSnapshotPreBarrier(1);
          nativeLocal.prepareSnapshotPreBarrier(1);
          forwardPipeline(
              stockLocal.getOutput(), stockGlobal, nativeLocal.getOutput(), nativeGlobal);
          stockGlobal.prepareSnapshotPreBarrier(1);
          nativeGlobal.prepareSnapshotPreBarrier(1);
        }
        long[] ticks =
            scenario.equals("history")
                ? times(
                    "1883-11-18T20:07:02.001Z",
                    "1883-11-18T20:59:59.999Z",
                    "1970-01-01T00:00:00.001Z")
                : new long[] {arrivals[0] - 1, arrivals[0], arrivals[1], arrivals[1] + 86_400_000};
        for (long tick : ticks) {
          stockLocal.processWatermark(new Watermark(tick));
          nativeLocal.processWatermark(new Watermark(tick));
          forwardPipeline(
              stockLocal.getOutput(), stockGlobal, nativeLocal.getOutput(), nativeGlobal);
          var expected = drainStock(stockGlobal);
          var actual = drainNative(nativeGlobal);
          expected.sort(FlinkNativeFixedCalendarWindowTest::compareRows);
          actual.sort(FlinkNativeFixedCalendarWindowTest::compareRows);
          assertEquals(expected, actual, "pipeline watermark=" + tick + " rocks=" + rocks);
        }
        stockLocal.prepareSnapshotPreBarrier(2);
        nativeLocal.prepareSnapshotPreBarrier(2);
        forwardPipeline(stockLocal.getOutput(), stockGlobal, nativeLocal.getOutput(), nativeGlobal);
        stockGlobal.prepareSnapshotPreBarrier(2);
        nativeGlobal.prepareSnapshotPreBarrier(2);
        stockLocal.processWatermark(Watermark.MAX_WATERMARK);
        nativeLocal.processWatermark(Watermark.MAX_WATERMARK);
        forwardPipeline(stockLocal.getOutput(), stockGlobal, nativeLocal.getOutput(), nativeGlobal);
        var expected = drainStock(stockGlobal);
        var actual = drainNative(nativeGlobal);
        expected.sort(FlinkNativeFixedCalendarWindowTest::compareRows);
        actual.sort(FlinkNativeFixedCalendarWindowTest::compareRows);
        assertEquals(expected, actual, "pipeline final watermark rocks=" + rocks);
      }
    }
  }

  private static void forwardPipeline(
      java.util.Queue<Object> stockOutput,
      org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness<
              RowData, RowData, RowData>
          stockGlobal,
      java.util.Queue<Object> nativeOutput,
      org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness<
              Integer, ArrowBatch, ArrowBatch>
          nativeGlobal)
      throws Exception {
    Object event;
    while ((event = stockOutput.poll()) != null) {
      if (event instanceof StreamRecord) {
        var row = (RowData) ((StreamRecord<?>) event).getValue();
        stockGlobal.processElement(
            new StreamRecord<>(
                GenericRowData.of(
                    row.getLong(0),
                    row.getLong(1),
                    TimestampData.fromEpochMillis(row.getLong(2)))));
      } else stockGlobal.processWatermark((Watermark) event);
    }
    while ((event = nativeOutput.poll()) != null) {
      if (event instanceof StreamRecord)
        nativeGlobal.processElement(
            new StreamRecord<>((ArrowBatch) ((StreamRecord<?>) event).getValue()));
      else nativeGlobal.processWatermark((Watermark) event);
    }
  }

  private static void compare(
      String zone,
      java.util.Queue<Object> stock,
      java.util.Queue<Object> nativeOutput,
      String phase) {
    var expected = new ArrayList<List<Long>>();
    var actual = new ArrayList<List<Long>>();
    Object event;
    while ((event = stock.poll()) != null) {
      if (event instanceof StreamRecord) {
        var row = (RowData) ((StreamRecord<?>) event).getValue();
        expected.add(Arrays.asList(row.getLong(0), row.getLong(1), row.getLong(2)));
      } else expected.add(List.of(((Watermark) event).getTimestamp()));
    }
    while ((event = nativeOutput.poll()) != null) {
      if (event instanceof StreamRecord) {
        try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (var row : RowDataArrowConverter.read(root, PARTIAL))
            actual.add(
                Arrays.asList(
                    row.getLong(0),
                    row.getLong(1),
                    java.time.ZoneId.of(zone).getRules().isFixedOffset()
                        ? org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills(
                            row.getLong(2), java.time.ZoneId.of(zone))
                        : row.getLong(2)));
        }
      } else actual.add(List.of(((Watermark) event).getTimestamp()));
    }
    // Record order across keys/slices is unspecified; every record must precede the forwarded
    // watermark.
    int expectedWatermark = expected.size() - 1;
    int actualWatermark = actual.size() - 1;
    var expectedRecords = new ArrayList<>(expected);
    var actualRecords = new ArrayList<>(actual);
    if (expectedWatermark >= 0 && expected.get(expectedWatermark).size() == 1)
      expectedRecords.remove(expectedWatermark);
    if (actualWatermark >= 0 && actual.get(actualWatermark).size() == 1)
      actualRecords.remove(actualWatermark);
    expectedRecords.sort(FlinkNativeFixedCalendarWindowTest::compareRows);
    actualRecords.sort(FlinkNativeFixedCalendarWindowTest::compareRows);
    assertEquals(expectedRecords, actualRecords, phase);
    assertEquals(
        expected.stream()
            .filter(row -> row.size() == 1)
            .collect(java.util.stream.Collectors.toList()),
        actual.stream()
            .filter(row -> row.size() == 1)
            .collect(java.util.stream.Collectors.toList()),
        "forwarded watermarks");
  }

  private static long[] times(String... values) {
    return Arrays.stream(values).mapToLong(value -> Instant.parse(value).toEpochMilli()).toArray();
  }
}

package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static tech.streamfusion.operator.FlinkNativeFixedCalendarWindowTest.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkNativeRegionalFixedWindowTest {
  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String scenario : List.of("gap", "overlap", "negative", "history", "history_shared"))
      for (String shape : List.of("tumble", "hop", "cumulate"))
        for (boolean writer : new boolean[] {false, true})
          for (boolean reader : new boolean[] {false, true}) {
            cases.add(Arguments.of(scenario, shape, writer, reader, false));
            if (!scenario.equals("negative") && !scenario.startsWith("history"))
              cases.add(Arguments.of(scenario, shape, writer, reader, true));
          }
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void regionalFixedAssignmentFiringAndRecoveryMatchReleasedFlink(
      String scenario, String shape, boolean writer, boolean reader, boolean proctime)
      throws Exception {
    String zone = scenario.equals("negative") ? "Asia/Shanghai" : "America/Los_Angeles";
    long size =
        scenario.equals("history_shared")
            ? (shape.equals("cumulate") ? 3 : shape.equals("hop") ? 2 : 1)
            : scenario.equals("history") ? 1 : 86_400_000;
    long slide = scenario.startsWith("history") ? 1 : shape.equals("tumble") ? size : size / 4;
    long[] times =
        scenario.startsWith("history")
            ? times("1883-11-18T19:59:59.999Z", "1883-11-18T20:07:02Z")
            : scenario.equals("gap")
                ? times("2024-03-10T09:30:00Z", "2024-03-10T10:30:00Z")
                : scenario.equals("overlap")
                    ? times("2024-11-03T08:30:00Z", "2024-11-03T09:30:00Z")
                    : times("1969-12-31T23:30:00Z", "1969-12-31T23:45:00Z");
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState stockSnapshot, nativeSnapshot;
    try (var allocator = new RootAllocator();
        var stock = stock(zone, shape, proctime, size, slide);
        var nativeOperator = nativeHarness(zone, shape, proctime, writer, size, slide)) {
      stock.setup(new RowDataSerializer(OUTPUT));
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.open();
      for (long time : times) {
        if (proctime) {
          stock.setProcessingTime(time);
          nativeOperator.setProcessingTime(time);
        }
        var row = GenericRowData.of(1L, TimestampData.fromEpochMillis(time));
        stock.processElement(new StreamRecord<>(row));
        nativeOperator.processElement(
            new StreamRecord<>(
                new ArrowBatch(RowDataArrowConverter.write(List.of(row), INPUT, allocator))));
      }
      compareOutput(stock, nativeOperator, "input");
      stock.prepareSnapshotPreBarrier(1);
      stockSnapshot =
          writer == reader
              ? stock.snapshot(1, 1)
              : stock
                  .snapshotWithLocalState(
                      1,
                      1,
                      org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                          org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
                  .getJobManagerOwnedState();
      nativeSnapshot =
          writer == reader
              ? nativeOperator.snapshot(1, 1)
              : nativeOperator
                  .snapshotWithLocalState(
                      1,
                      1,
                      org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                          org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
                  .getJobManagerOwnedState();
      progress(stock, nativeOperator, scenario, times[1], proctime);
    }
    try (var stock = stock(zone, shape, proctime, size, slide);
        var nativeOperator = nativeHarness(zone, shape, proctime, reader, size, slide)) {
      stock.setup(new RowDataSerializer(OUTPUT));
      stock.initializeState(stockSnapshot);
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.initializeState(nativeSnapshot);
      if (proctime) nativeOperator.setProcessingTime(times[1]);
      nativeOperator.open();
      progress(stock, nativeOperator, scenario, times[1], proctime);
    }
  }

  private static long[] times(String... values) {
    return java.util.Arrays.stream(values)
        .mapToLong(value -> Instant.parse(value).toEpochMilli())
        .toArray();
  }

  private static void progress(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock,
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeOperator,
      String scenario,
      long last,
      boolean proctime)
      throws Exception {
    long[] ticks;
    if (scenario.startsWith("history"))
      ticks =
          times(
              "1883-11-18T20:07:01.999Z",
              "1883-11-18T20:07:02Z",
              "1883-11-18T20:07:02.001Z",
              "1883-11-18T20:59:59.999Z");
    else {
      ticks = new long[49];
      for (int i = 0; i < ticks.length; i++) ticks[i] = last + i * 3_600_000;
    }
    for (long tick : ticks) {
      if (proctime) {
        stock.setProcessingTime(tick);
        nativeOperator.setProcessingTime(tick);
      } else {
        stock.processWatermark(new Watermark(tick));
        nativeOperator.processWatermark(new Watermark(tick));
      }
      compareOutput(stock, nativeOperator, "progress=" + tick);
    }
  }

  private static void compareOutput(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock,
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeOperator,
      String stage) {
    var expected = drainStock(stock);
    var actual = drainNative(nativeOperator);
    expected.sort(FlinkNativeFixedCalendarWindowTest::compareRows);
    actual.sort(FlinkNativeFixedCalendarWindowTest::compareRows);
    assertEquals(expected, actual, stage);
  }
}

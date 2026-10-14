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

class FlinkNativeRegionalTwoPhaseWindowTest {
  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String scenario : List.of("gap", "overlap", "negative", "history"))
      for (String shape : List.of("tumble", "hop", "cumulate"))
        for (boolean writer : new boolean[] {false, true})
          for (boolean reader : new boolean[] {false, true})
            for (boolean afterFiring : new boolean[] {false, true})
              cases.add(Arguments.of(scenario, shape, writer, reader, afterFiring));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void regionalPartialDeliveryAndGlobalRecoveryMatchReleasedFlink(
      String scenario, String shape, boolean writer, boolean reader, boolean afterFiring)
      throws Exception {
    String zone = scenario.equals("negative") ? "Asia/Shanghai" : "America/Los_Angeles";
    long size =
        scenario.equals("history")
            ? (shape.equals("cumulate") ? 3 : shape.equals("hop") ? 2 : 1)
            : 86_400_000;
    long slide = scenario.equals("history") ? 1 : shape.equals("tumble") ? size : size / 4;
    long[] times =
        scenario.equals("history")
            ? times("1883-11-18T19:59:59.999Z", "1883-11-18T20:07:02Z")
            : scenario.equals("gap")
                ? times("2024-03-10T09:30:00Z", "2024-03-10T10:30:00Z")
                : scenario.equals("overlap")
                    ? times("2024-11-03T08:30:00Z", "2024-11-03T09:30:00Z")
                    : times("1969-12-31T23:30:00Z", "1969-12-31T23:45:00Z");
    long checkpointProgress =
        scenario.equals("history")
            ? Instant.parse("1883-11-18T20:07:02.001Z").toEpochMilli()
            : times[1] + 3_600_000 * 3;
    // Live watermarks deliberately precede any barrier drain.
    try (var allocator = new RootAllocator();
        var stock = stockGlobal(zone, shape, size, slide);
        var local = localHarness(zone, slide);
        var global = globalHarness(zone, shape, writer, size, slide)) {
      stock.setup(new RowDataSerializer(OUTPUT));
      stock.open();
      local.setup(new ArrowBatchSerializer());
      local.open();
      global.setup(new ArrowBatchSerializer());
      global.open();
      for (long time : times) push(stock, local, allocator, time);
      stock.prepareSnapshotPreBarrier(1);
      progress(stock, local, global, scenario, times[1], Long.MIN_VALUE);
    }
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState stockSnapshot, nativeSnapshot;
    try (var allocator = new RootAllocator();
        var stock = stockGlobal(zone, shape, size, slide);
        var local = localHarness(zone, slide);
        var global = globalHarness(zone, shape, writer, size, slide)) {
      stock.setup(new RowDataSerializer(OUTPUT));
      stock.open();
      local.setup(new ArrowBatchSerializer());
      local.open();
      global.setup(new ArrowBatchSerializer());
      global.open();
      push(stock, local, allocator, times[0]);
      if (afterFiring) {
        push(stock, local, allocator, times[1]);
        local.processWatermark(new Watermark(checkpointProgress));
        forward(stock, local, global);
        stock.prepareSnapshotPreBarrier(1);
        stock.processWatermark(new Watermark(checkpointProgress));
        global.processWatermark(new Watermark(checkpointProgress));
        var expected = drainStock(stock);
        var actual = drainNative(global);
        expected.sort(FlinkNativeFixedCalendarWindowTest::compareRows);
        actual.sort(FlinkNativeFixedCalendarWindowTest::compareRows);
        assertEquals(expected, actual, "before recovery after firing");
      } else {
        local.prepareSnapshotPreBarrier(1);
        forward(stock, local, global);
      }
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
              ? global.snapshot(1, 1)
              : global
                  .snapshotWithLocalState(
                      1,
                      1,
                      org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                          org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
                  .getJobManagerOwnedState();
    }
    try (var allocator = new RootAllocator();
        var stock = stockGlobal(zone, shape, size, slide);
        var local = localHarness(zone, slide);
        var global = globalHarness(zone, shape, reader, size, slide)) {
      stock.setup(new RowDataSerializer(OUTPUT));
      stock.initializeState(stockSnapshot);
      stock.open();
      local.setup(new ArrowBatchSerializer());
      local.open();
      global.setup(new ArrowBatchSerializer());
      global.initializeState(nativeSnapshot);
      global.open();
      if (!afterFiring) push(stock, local, allocator, times[1]);
      stock.prepareSnapshotPreBarrier(2);
      progress(
          stock,
          local,
          global,
          scenario,
          times[1],
          afterFiring ? checkpointProgress : Long.MIN_VALUE);
    }
  }

  private static void push(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock,
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> local,
      RootAllocator allocator,
      long time)
      throws Exception {
    var row = GenericRowData.of(1L, TimestampData.fromEpochMillis(time));

    local.processElement(
        new StreamRecord<>(
            new ArrowBatch(RowDataArrowConverter.write(List.of(row), INPUT, allocator))));
  }

  private static void forward(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock,
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> local,
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> global)
      throws Exception {
    for (var record : local.extractOutputStreamRecords()) {
      for (var row :
          RowDataArrowConverter.read(
              record.getValue().root(),
              org.apache.flink.table.types.logical.RowType.of(
                  new org.apache.flink.table.types.logical.BigIntType(),
                  new org.apache.flink.table.types.logical.BigIntType(),
                  new org.apache.flink.table.types.logical.BigIntType())))
        stock.processElement(
            new StreamRecord<>(
                GenericRowData.of(
                    row.getLong(0),
                    row.getLong(1),
                    TimestampData.fromEpochMillis(row.getLong(2)))));
      global.processElement(new StreamRecord<>(record.getValue()));
    }
    local.getOutput().clear();
  }

  private static void progress(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock,
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> local,
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> global,
      String scenario,
      long last,
      long skipBefore)
      throws Exception {
    long[] ticks;
    if (scenario.equals("history"))
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
      if (tick < skipBefore) continue;
      local.processWatermark(new Watermark(tick));
      forward(stock, local, global);
      stock.prepareSnapshotPreBarrier(98);
      stock.processWatermark(new Watermark(tick));
      global.processWatermark(new Watermark(tick));
      var expected = drainStock(stock);
      var actual = drainNative(global);
      expected.sort(FlinkNativeFixedCalendarWindowTest::compareRows);
      actual.sort(FlinkNativeFixedCalendarWindowTest::compareRows);
      assertEquals(expected, actual, "progress=" + tick);
    }
    try (var allocator = new RootAllocator()) {
      push(stock, local, allocator, last);
      stock.prepareSnapshotPreBarrier(99);
      long finalTick = ticks[ticks.length - 1];
      local.processWatermark(new Watermark(finalTick));
      forward(stock, local, global);
      stock.prepareSnapshotPreBarrier(99);
      stock.processWatermark(new Watermark(finalTick));
      global.processWatermark(new Watermark(finalTick));
      assertEquals(List.of(), drainStock(stock), "stock late input");
      assertEquals(List.of(), drainNative(global), "late partial must not recreate a fired window");
    }
  }

  private static long[] times(String... values) {
    return java.util.Arrays.stream(values)
        .mapToLong(value -> Instant.parse(value).toEpochMilli())
        .toArray();
  }
}

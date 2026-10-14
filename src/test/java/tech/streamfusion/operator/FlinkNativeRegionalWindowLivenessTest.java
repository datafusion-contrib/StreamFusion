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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkNativeRegionalWindowLivenessTest {
  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String shape : List.of("hop", "cumulate"))
      for (boolean writer : new boolean[] {false, true})
        for (boolean reader : new boolean[] {false, true})
          cases.add(Arguments.of(shape, writer, reader));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void zeroLiveCountStopsHoppingTimerContinuationAfterRestore(
      String shape, boolean writer, boolean reader) throws Exception {
    String zone = "America/Los_Angeles";
    long slide = 6 * 3_600_000L;
    long size = (shape.equals("hop") ? 2 : 3) * slide;
    long firstDeadline = Instant.parse("2024-03-10T13:00:00Z").toEpochMilli() - 1;
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState stockSnapshot, nativeSnapshot;
    try (var allocator = new RootAllocator();
        var stock = stock(zone, shape, false, size, slide);
        var nativeOperator = nativeCountHarness(zone, shape, false, writer, size, slide, 11)) {
      stock.setup(new RowDataSerializer(OUTPUT));
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.open();
      long[] times = {
        Instant.parse("2024-03-10T09:00:00Z").toEpochMilli(),
        Instant.parse("2024-03-10T14:00:00Z").toEpochMilli()
      };
      for (int i = 0; i < times.length; i++) {
        var row = GenericRowData.of(1L, TimestampData.fromEpochMillis(times[i]));
        row.setRowKind(i == 0 ? RowKind.INSERT : RowKind.DELETE);
        stock.processElement(new StreamRecord<>(row));
        nativeOperator.processElement(
            new StreamRecord<>(
                new ArrowBatch(RowDataArrowConverter.write(List.of(row), INPUT, allocator, true))));
      }
      stock.prepareSnapshotPreBarrier(1);
      stock.processWatermark(new Watermark(firstDeadline));
      nativeOperator.processWatermark(new Watermark(firstDeadline));
      assertEquals(drainStock(stock), drainNative(nativeOperator), "first nonempty window");
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
    }
    try (var stock = stock(zone, shape, false, size, slide);
        var nativeOperator = nativeCountHarness(zone, shape, false, reader, size, slide, 11)) {
      stock.setup(new RowDataSerializer(OUTPUT));
      stock.initializeState(stockSnapshot);
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.initializeState(nativeSnapshot);
      nativeOperator.open();
      for (String time :
          List.of("2024-03-10T19:00:00Z", "2024-03-11T01:00:00Z", "2024-03-11T07:00:00Z")) {
        var watermark = new Watermark(Instant.parse(time).toEpochMilli() - 1);
        stock.processWatermark(watermark);
        nativeOperator.processWatermark(watermark);
        assertEquals(drainStock(stock), drainNative(nativeOperator), "after restore at " + time);
      }
    }
  }
}

package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static tech.streamfusion.operator.FlinkNativeFixedCalendarWindowTest.*;

import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.configuration.*;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkNativeUpdatingProctimeWindowTest {
  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> cases() {
    return Stream.of("tumble", "hop", "cumulate")
        .flatMap(
            shape ->
                Stream.of(false, true)
                    .flatMap(
                        writer ->
                            Stream.of(false, true)
                                .flatMap(
                                    reader ->
                                        Stream.of(false, true)
                                            .map(
                                                attached ->
                                                    Arguments.of(
                                                        shape, writer, reader, attached)))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void signedCountsRetainTimersAndEmptyWindowsAcrossBackendRestore(
      String shape, boolean writer, boolean reader, boolean attached) throws Exception {
    long size = shape.equals("tumble") ? 1000 : 3000;
    OperatorSubtaskState snapshot;
    var expected = new java.util.ArrayList<List<List<Long>>>();
    long[] deadlines = {999, 1999, 2999, 5000};
    try (var allocator = new RootAllocator();
        var host = stock("UTC", shape, true, size, 1000);
        var before = consumer(shape, writer, size, attached);
        var assignment =
            attached
                ? new org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness<
                    ArrowBatch, ArrowBatch>(
                    new NativeWindowTableFunctionOperator(
                        1, size, 1000, shape.equals("cumulate"), true, 0))
                : null) {
      host.setup(new RowDataSerializer(OUTPUT));
      host.open();
      before.setup(new ArrowBatchSerializer());
      before.open();
      if (assignment != null) {
        assignment.setup(new ArrowBatchSerializer());
        assignment.open();
      }
      for (int batch = 0; batch < 2; batch++) {
        long now = 100 + batch * 100;
        host.setProcessingTime(now);
        before.setProcessingTime(now);
        if (assignment != null) assignment.setProcessingTime(now);
        var rows = new java.util.ArrayList<org.apache.flink.table.data.RowData>();
        for (var kind :
            batch == 0
                ? List.of(RowKind.INSERT, RowKind.DELETE)
                : List.of(RowKind.INSERT, RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER)) {
          var row = GenericRowData.of(1L, TimestampData.fromEpochMillis(0));
          row.setRowKind(kind);
          var hostRow = GenericRowData.of(1L, TimestampData.fromEpochMillis(0));
          hostRow.setRowKind(attached && !tech.streamfusion.compat.RuntimeCompat.windowTvfPreservesRowKinds() ? RowKind.INSERT : kind);
          host.processElement(new StreamRecord<>(hostRow));
          rows.add(row);
        }
        var batchRecord =
            new StreamRecord<>(
                new ArrowBatch(RowDataArrowConverter.write(rows, INPUT, allocator, true)));
        if (assignment == null) before.processElement(batchRecord);
        else {
          assignment.processElement(batchRecord);
          for (var record : assignment.extractOutputStreamRecords()) before.processElement(new StreamRecord<>(record.getValue()));
          assignment.getOutput().clear();
        }
      }
      host.prepareSnapshotPreBarrier(1);
      snapshot =
          writer == reader
              ? before.snapshot(1, 200)
              : before
                  .snapshotWithLocalState(
                      1,
                      200,
                      org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                          org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
                  .getJobManagerOwnedState();
      for (long deadline : deadlines) {
        host.setProcessingTime(deadline);
        expected.add(drainStock(host));
      }
    }
    try (var after = consumer(shape, reader, size, attached)) {
      after.setup(new ArrowBatchSerializer());
      after.initializeState(snapshot);
      after.open();
      for (int index = 0; index < deadlines.length; index++) {
        after.setProcessingTime(deadlines[index]);
        assertEquals(expected.get(index), drainNative(after), "clock=" + deadlines[index]);
      }
    }
  }

  private static org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness<
          Integer, ArrowBatch, ArrowBatch>
      consumer(String shape, boolean rocks, long size, boolean attached) throws Exception {
    if (!attached) return nativeCountHarness("UTC", shape, true, rocks, size, 1000, 11);
    var operator =
        new NativeColumnarWindowAggregateOperator(
            false,
            size,
            size,
            -1,
            new int[] {-1},
            null,
            new int[] {0},
            new int[] {0},
            new int[] {0},
            new int[] {11},
            "UTC",
            true,
            "UTC",
            OUTPUT,
            true,
            new int[] {-1},
            128,
            2,
            3);
    var harness =
        new org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness<
            Integer, ArrowBatch, ArrowBatch>(
            operator, ignored -> 0, org.apache.flink.api.common.typeinfo.Types.INT, 128, 1, 0);
    if (rocks)
      harness.setStateBackend(
          new tech.streamfusion.state.RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(),
                  FlinkNativeUpdatingProctimeWindowTest.class.getClassLoader()));
    return harness;
  }
}

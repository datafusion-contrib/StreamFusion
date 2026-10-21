package tech.streamfusion.operator;

import static tech.streamfusion.operator.FlinkNativeRegionalSessionWindowTest.*;

import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.configuration.*;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
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

class FlinkNativeUpdatingSessionWindowTest {
  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            proctime ->
                Stream.of(false, true)
                    .flatMap(
                        writer ->
                            Stream.of(false, true)
                                .map(reader -> Arguments.of(proctime, writer, reader))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void signedSessionsKeepEmptyNamespacesAndClockTimersAcrossBackendRestore(
      boolean proctime, boolean writer, boolean reader) throws Exception {
    OperatorSubtaskState nativeSnapshot, hostSnapshot;
    try (var allocator = new RootAllocator();
        var host = stock("UTC", 5000, writer, proctime, true);
        var before = nativeHarness("UTC", 5000, writer, proctime, 11)) {
      host.setup(new RowDataSerializer(OUTPUT));
      host.open();
      before.setup(new ArrowBatchSerializer());
      before.open();
      long[][] rows = {{0, 0}, {3, 0}, {0, 10000}, {3, 10000}, {0, 3000}, {0, 6000}, {3, 3000}};
      for (int i = 0; i < rows.length; i++) {
        if (proctime) {
          host.setProcessingTime(100 + i);
          before.setProcessingTime(100 + i);
        }
        var kind = RowKind.fromByteValue((byte) rows[i][0]);
        var stockRow = GenericRowData.of(1L, rows[i][1]);
        stockRow.setRowKind(kind);
        host.processElement(new StreamRecord<>(stockRow));
        var nativeRow = GenericRowData.of(1L, TimestampData.fromEpochMillis(rows[i][1]));
        nativeRow.setRowKind(kind);
        before.processElement(
            new StreamRecord<>(
                new ArrowBatch(
                    RowDataArrowConverter.write(List.of(nativeRow), INPUT, allocator, true))));
      }
      hostSnapshot =
          writer == reader
              ? host.snapshot(1, 200)
              : host.snapshotWithLocalState(
                      1,
                      200,
                      org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                          org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
                  .getJobManagerOwnedState();
      nativeSnapshot =
          writer == reader
              ? before.snapshot(1, 200)
              : before
                  .snapshotWithLocalState(
                      1,
                      200,
                      org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                          org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
                  .getJobManagerOwnedState();
    }
    try (var host = stock("UTC", 5000, reader, proctime, true);
        var after = nativeHarness("UTC", 5000, reader, proctime, 11)) {
      host.setup(new RowDataSerializer(OUTPUT));
      host.initializeState(hostSnapshot);
      host.open();
      after.setup(new ArrowBatchSerializer());
      after.initializeState(nativeSnapshot);
      after.open();
      if (proctime) {
        host.setProcessingTime(20000);
        after.setProcessingTime(20000);
      } else {
        host.processWatermark(new Watermark(20000));
        after.processWatermark(new Watermark(20000));
      }
      compareOutput(host, after, "after signed session restore");
    }
  }
}

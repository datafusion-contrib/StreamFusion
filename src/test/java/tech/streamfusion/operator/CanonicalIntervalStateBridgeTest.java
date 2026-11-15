package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.Native;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

class CanonicalIntervalStateBridgeTest {
  private static final RowType INPUT =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType());

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (int kind = 0; kind < 4; kind++)
      for (boolean sourceDisk : new boolean[] {false, true})
        for (boolean targetDisk : new boolean[] {false, true})
          cases.add(Arguments.of(kind, sourceDisk, targetDisk));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void directRowsAndTimersContinueWithoutReplayingInput(
      int kind, boolean sourceDisk, boolean targetDisk) throws Exception {
    var source = operator(kind);
    var target = operator(kind);
    try (BufferAllocator allocator = new RootAllocator();
        var before = harness(source, sourceDisk);
        var after = harness(target, targetDisk)) {
      before.processElement1(
          new StreamRecord<>(
              batch(
                  allocator,
                  GenericRowData.of(1L, 10L, 100L),
                  GenericRowData.of(1L, 11L, 100L),
                  GenericRowData.of(2L, null, 100L),
                  GenericRowData.of(3L, 30L, 100L))));
      before.processElement2(
          new StreamRecord<>(batch(allocator, GenericRowData.of(1L, 20L, 150L))));
      assertEquals(2, output(before).size());
      for (boolean left : new boolean[] {true, false}) {
        Native.exportIntervalJoinCanonicalRows(
            source.handle,
            left,
            (array, schema) ->
                Native.importIntervalJoinCanonicalRows(target.handle, left, array, schema));
        Native.exportIntervalJoinCanonicalTimers(
            source.handle,
            left,
            (array, schema) ->
                Native.importIntervalJoinCanonicalTimers(target.handle, left, array, schema));
      }
      assertTrue(output(after).isEmpty(), "import must not join or emit");
      // Export immediately after import, with no new input or native snapshot.
      assertEquals(capture(source.handle, allocator), capture(target.handle, allocator));
      before.processElement2(
          new StreamRecord<>(batch(allocator, GenericRowData.of(2L, 40L, 120L))));
      after.processElement2(new StreamRecord<>(batch(allocator, GenericRowData.of(2L, 40L, 120L))));
      assertEquals(output(before), output(after));
      before.processBothWatermarks(new Watermark(400));
      after.processBothWatermarks(new Watermark(400));
      assertEquals(output(before), output(after));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void failedReceiverReleasesCDataAndLeavesStateExportable(boolean disk) throws Exception {
    var source = operator(3);
    try (BufferAllocator allocator = new RootAllocator();
        var before = harness(source, disk)) {
      var rows = new ArrayList<RowData>();
      for (long value = 0; value < 2051; value++) rows.add(GenericRowData.of(1L, value, 100L));
      before.processElement1(new StreamRecord<>(batch(allocator, rows.toArray(new RowData[0]))));
      assertThrows(
          IllegalStateException.class,
          () ->
              Native.exportIntervalJoinCanonicalRows(
                  source.handle,
                  true,
                  (array, schema) -> {
                    throw new IllegalStateException("receiver failed");
                  }));
      var sizes = new ArrayList<Integer>();
      try (var dictionaries = new CDataDictionaryProvider()) {
        Native.exportIntervalJoinCanonicalRows(
            source.handle,
            true,
            (array, schema) -> {
              try (var a = ArrowArray.wrap(array);
                  var s = ArrowSchema.wrap(schema);
                  var root = Data.importVectorSchemaRoot(allocator, a, s, dictionaries)) {
                assertTrue(root.getRowCount() <= 1024);
                sizes.add(root.getRowCount());
              }
            });
      }
      assertEquals(List.of(1024, 1024, 3), sizes);
      assertEquals(
          2051,
          capture(source.handle, allocator).stream().filter(row -> row.startsWith("row:")).count());
    }
  }

  private static NativeIntervalJoinOperator operator(int kind) {
    return new NativeIntervalJoinOperator(
        new int[] {0},
        new int[] {0},
        2,
        2,
        -100,
        100,
        kind,
        INPUT,
        INPUT,
        EncodedPredicate.NONE,
        false,
        new int[] {-1},
        128);
  }

  private static KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
      harness(NativeIntervalJoinOperator operator, boolean disk) throws Exception {
    var harness =
        new KeyedTwoInputStreamOperatorTestHarness<>(
            operator, batch -> 0, batch -> 0, Types.INT, 128, 1, 0);
    if (disk)
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), CanonicalIntervalStateBridgeTest.class.getClassLoader()));
    harness.setup(new ArrowBatchSerializer());
    harness.open();
    assertEquals(disk, operator.directRocksDBState());
    Native.bindIntervalJoinCanonicalKeys(
        operator.handle, new int[] {Native.STOCK_KEY_PROJECTION, -1});
    return harness;
  }

  private static ArrowBatch batch(BufferAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(Arrays.asList(rows), INPUT, allocator));
  }

  private static List<String> capture(long handle, BufferAllocator allocator) {
    var rows = new ArrayList<String>();
    try (var dictionaries = new CDataDictionaryProvider()) {
      for (boolean left : new boolean[] {true, false}) {
        Native.BatchReceiver receiveRows =
            (array, schema) -> {
              try (var a = ArrowArray.wrap(array);
                  var s = ArrowSchema.wrap(schema);
                  var root = Data.importVectorSchemaRoot(allocator, a, s, dictionaries)) {
                for (int row = 0; row < root.getRowCount(); row++) {
                  var values = new ArrayList<String>();
                  // INNER match bits have no effect on future output; native memory normalizes
                  // them.
                  for (int col = 0; col < 3; col++)
                    values.add(String.valueOf(root.getVector(col).getObject(row)));
                  rows.add("row:" + left + ":" + values);
                }
              }
            };
        Native.exportIntervalJoinCanonicalRows(handle, left, receiveRows);
        Native.exportIntervalJoinCanonicalTimers(
            handle,
            left,
            (array, schema) -> {
              try (var a = ArrowArray.wrap(array);
                  var s = ArrowSchema.wrap(schema);
                  var root = Data.importVectorSchemaRoot(allocator, a, s, dictionaries)) {
                for (int row = 0; row < root.getRowCount(); row++)
                  rows.add(
                      "timer:"
                          + left
                          + ":"
                          + Arrays.toString((byte[]) root.getVector(0).getObject(row))
                          + ":"
                          + root.getVector(1).getObject(row));
              }
            });
      }
    }
    Collections.sort(rows);
    return rows;
  }

  private static List<String> output(
      KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> harness) {
    var rows = new ArrayList<String>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (!(event instanceof StreamRecord)) continue;
      try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
        for (int row = 0; row < root.getRowCount(); row++) {
          var values = new ArrayList<String>();
          for (var vector : root.getFieldVectors())
            values.add(String.valueOf(vector.getObject(row)));
          rows.add(values.toString());
        }
      }
    }
    Collections.sort(rows);
    return rows;
  }
}

package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.over.ProcTimeRowsBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRangeBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRowsBoundedPrecedingFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

class FlinkNativeBoundedDecimalOverTest {
  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  @ParameterizedTest
  @CsvSource({
    "1,false,false",
    "1,false,true",
    "1,true,false",
    "1,true,true",
    "2,false,false",
    "2,false,true"
  })
  void actualReleasedBoundedOperatorsMatchNativePeersNullsAndEviction(
      int frame, boolean proctime, boolean rocks) throws Exception {
    compare(
        frame,
        proctime,
        rocks,
        12,
        2,
        1,
        new String[] {null, "1.25", null, "2.50", "-0.25", null},
        new long[] {100, 200, 200, 300, 400, 500});
  }

  @ParameterizedTest
  @CsvSource({
    "1,false,false",
    "1,false,true",
    "1,true,false",
    "1,true,true",
    "2,false,false",
    "2,false,true"
  })
  void actualReleasedOperatorsRetainOverflowHistoryAfterTheOffendingValueLeaves(
      int frame, boolean proctime, boolean rocks) throws Exception {
    compare(
        frame,
        proctime,
        rocks,
        38,
        0,
        1,
        new String[] {null, "99999999999999999999999999999999999999", "1", "7", "2"},
        new long[] {50, 100, 200, 300, 400});
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void releasedRangeSumRetractionOrderDependsOnBackendWhenDecimalOverflows(boolean rocks)
      throws Exception {
    var input = RowType.of(new DecimalType(38, 0), new BigIntType());
    var output =
        RowType.of(
            new DecimalType(38, 0),
            new BigIntType(),
            new DecimalType(38, 0),
            new DecimalType(38, 0),
            new DecimalType(38, 6));
    try (var allocator = new RootAllocator();
        var stock = stock(2, false, rocks, 38, 0, 1000, input);
        var nativeHarness = nativeHarness(2, false, rocks, 38, 0, 1000, input)) {
      stock.setup(new RowDataSerializer(output));
      stock.open();
      nativeHarness.setup(new ArrowBatchSerializer());
      nativeHarness.open();
      String[] values = {"99999999999999999999999999999999999999", "-1", "2", "7"};
      long[] times = {100, 200, 400, 2000};
      List<List<BigDecimal>> last = List.of();
      for (int i = 0; i < times.length; i++) {
        var row =
            GenericRowData.of(
                DecimalData.fromBigDecimal(new BigDecimal(values[i]), 38, 0), times[i]);
        stock.processElement(new StreamRecord<>(row));
        nativeHarness.processElement(
            new StreamRecord<>(
                new ArrowBatch(RowDataArrowConverter.write(List.of(row), input, allocator))));
        stock.processWatermark(new Watermark(times[i]));
        nativeHarness.processWatermark(new Watermark(times[i]));
        last = drainStock(stock, 0);
        assertEquals(last, drainNative(nativeHarness, output, 0));
      }
      assertEquals(List.of(Arrays.asList(BigDecimal.valueOf(rocks ? 7 : 8), null, null)), last);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void releasedRangeRetractionOrderAlsoChangesAfterHeapCheckpoint(boolean rocks) throws Exception {
    var input = RowType.of(new DecimalType(38, 0), new BigIntType());
    var output =
        RowType.of(
            new DecimalType(38, 0),
            new BigIntType(),
            new DecimalType(38, 0),
            new DecimalType(38, 0),
            new DecimalType(38, 6));
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState snapshot;
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState nativeSnapshot;
    try (var allocator = new RootAllocator();
        var before = stock(2, false, rocks, 38, 0, 1000, input);
        var nativeBefore = nativeHarness(2, false, rocks, 38, 0, 1000, input)) {
      before.setup(new RowDataSerializer(output));
      before.open();
      nativeBefore.setup(new ArrowBatchSerializer());
      nativeBefore.open();
      String[] values = {"99999999999999999999999999999999999999", "-1", "2"};
      long[] times = {100, 200, 400};
      for (int i = 0; i < times.length; i++) {
        var row =
            GenericRowData.of(
                DecimalData.fromBigDecimal(new BigDecimal(values[i]), 38, 0), times[i]);
        before.processElement(new StreamRecord<>(row));
        nativeBefore.processElement(
            new StreamRecord<>(
                new ArrowBatch(RowDataArrowConverter.write(List.of(row), input, allocator))));
        before.processWatermark(new Watermark(times[i]));
        nativeBefore.processWatermark(new Watermark(times[i]));
        assertEquals(drainStock(before, 0), drainNative(nativeBefore, output, 0));
      }
      snapshot = before.snapshot(1, 1);
      nativeSnapshot = nativeBefore.snapshot(1, 1);
    }
    try (var allocator = new RootAllocator();
        var after = stock(2, false, rocks, 38, 0, 1000, input);
        var nativeAfter = nativeHarness(2, false, rocks, 38, 0, 1000, input)) {
      after.setup(new RowDataSerializer(output));
      after.initializeState(snapshot);
      after.open();
      nativeAfter.setup(new ArrowBatchSerializer());
      nativeAfter.initializeState(nativeSnapshot);
      nativeAfter.open();
      var row = GenericRowData.of(DecimalData.fromBigDecimal(BigDecimal.valueOf(7), 38, 0), 2000L);
      after.processElement(new StreamRecord<>(row));
      nativeAfter.processElement(
          new StreamRecord<>(
              new ArrowBatch(RowDataArrowConverter.write(List.of(row), input, allocator))));
      after.processWatermark(new Watermark(2000));
      nativeAfter.processWatermark(new Watermark(2000));
      var expected = List.of(Arrays.asList(BigDecimal.valueOf(7), null, null));
      assertEquals(expected, drainStock(after, 0));
      assertEquals(expected, drainNative(nativeAfter, output, 0));
    }
  }

  @ParameterizedTest
  @CsvSource({
    "false,false,false",
    "false,true,false",
    "true,false,false",
    "true,true,false",
    "false,false,true",
    "false,true,true",
    "true,false,true",
    "true,true,true"
  })
  void rangePendingTimestampMapSurvivesCanonicalBackendTransitions(
      boolean writer, boolean reader, boolean swapped) throws Exception {
    var input = RowType.of(new DecimalType(38, 0), new BigIntType());
    var output =
        RowType.of(
            new DecimalType(38, 0),
            new BigIntType(),
            new DecimalType(38, 0),
            new DecimalType(38, 0),
            new DecimalType(38, 6));
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState stockSnapshot;
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState nativeSnapshot;
    var canonical =
        org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
            org.apache.flink.core.execution.SavepointFormatType.CANONICAL);
    try (var allocator = new RootAllocator();
        var before = stock(2, false, writer, 38, 0, 1000, input);
        var nativeBefore = nativeHarness(2, false, writer, 38, 0, 1000, input)) {
      before.setup(new RowDataSerializer(output));
      before.open();
      nativeBefore.setup(new ArrowBatchSerializer());
      nativeBefore.open();
      long[] times = {100, 200, 400, 2000};
      String maximum = "99999999999999999999999999999999999999";
      String[] values =
          swapped ? new String[] {"-1", maximum, "2", "7"} : new String[] {maximum, "-1", "2", "7"};
      for (int i = 0; i < times.length; i++) {
        var row =
            GenericRowData.of(
                DecimalData.fromBigDecimal(new BigDecimal(values[i]), 38, 0), times[i]);
        before.processElement(new StreamRecord<>(row));
        nativeBefore.processElement(
            new StreamRecord<>(
                new ArrowBatch(RowDataArrowConverter.write(List.of(row), input, allocator))));
        if (i < 3) {
          before.processWatermark(new Watermark(times[i]));
          nativeBefore.processWatermark(new Watermark(times[i]));
          assertEquals(drainStock(before, 0), drainNative(nativeBefore, output, 0));
        }
      }
      stockSnapshot = before.snapshotWithLocalState(1, 1, canonical).getJobManagerOwnedState();
      nativeSnapshot =
          nativeBefore.snapshotWithLocalState(1, 1, canonical).getJobManagerOwnedState();
    }
    try (var after = stock(2, false, reader, 38, 0, 1000, input);
        var nativeAfter = nativeHarness(2, false, reader, 38, 0, 1000, input)) {
      after.setup(new RowDataSerializer(output));
      after.initializeState(stockSnapshot);
      after.open();
      nativeAfter.setup(new ArrowBatchSerializer());
      nativeAfter.initializeState(nativeSnapshot);
      nativeAfter.open();
      after.processWatermark(new Watermark(2000));
      nativeAfter.processWatermark(new Watermark(2000));
      var expected =
          List.of(Arrays.asList(BigDecimal.valueOf(reader || swapped ? 7 : 8), null, null));
      assertEquals(expected, drainStock(after, 0));
      assertEquals(expected, drainNative(nativeAfter, output, 0));
    }
  }

  @ParameterizedTest
  @CsvSource({
    "false,false,false",
    "false,false,true",
    "false,true,false",
    "false,true,true",
    "true,false,false",
    "true,false,true",
    "true,true,false",
    "true,true,true"
  })
  void collidingRangeTimestampsRetainTreeOrderWithPendingGroupsAndCheckpoint(
      boolean rocks, boolean checkpoint, boolean pending) throws Exception {
    var input = RowType.of(new DecimalType(38, 0), new BigIntType());
    var output =
        RowType.of(
            new DecimalType(38, 0),
            new BigIntType(),
            new DecimalType(38, 0),
            new DecimalType(38, 0),
            new DecimalType(38, 6));
    long interval = 24L << 32;
    long finalTime = ((24L << 32) | (24 ^ 7)) + interval + 1;
    List<RowData> rows = new ArrayList<>();
    for (int key = 1; key <= 24; key++) {
      String value =
          key == 1
              ? "99999999999999999999999999999999999999"
              : key == 2 ? "-1" : key == 8 ? "2" : null;
      rows.add(
          GenericRowData.of(
              value == null ? null : DecimalData.fromBigDecimal(new BigDecimal(value), 38, 0),
              ((long) key << 32) | (key ^ 7)));
    }
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState stockSnapshot = null;
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState nativeSnapshot = null;
    try (var allocator = new RootAllocator();
        var before = stock(2, false, rocks, 38, 0, interval, input);
        var nativeBefore = nativeHarness(2, false, rocks, 38, 0, interval, input)) {
      before.setup(new RowDataSerializer(output));
      before.open();
      nativeBefore.setup(new ArrowBatchSerializer());
      nativeBefore.open();
      if (pending) {
        for (var row : rows) before.processElement(new StreamRecord<>(row));
        nativeBefore.processElement(
            new StreamRecord<>(
                new ArrowBatch(RowDataArrowConverter.write(rows, input, allocator))));
        long time = rows.get(rows.size() - 1).getLong(1);
        before.processWatermark(new Watermark(time));
        nativeBefore.processWatermark(new Watermark(time));
        assertEquals(drainStock(before, 0), drainNative(nativeBefore, output, 0));
      } else {
        for (var row : rows) {
          before.processElement(new StreamRecord<>(row));
          nativeBefore.processElement(
              new StreamRecord<>(
                  new ArrowBatch(RowDataArrowConverter.write(List.of(row), input, allocator))));
          before.processWatermark(new Watermark(row.getLong(1)));
          nativeBefore.processWatermark(new Watermark(row.getLong(1)));
          assertEquals(drainStock(before, 0), drainNative(nativeBefore, output, 0));
        }
      }
      if (checkpoint) {
        stockSnapshot = before.snapshot(1, 1);
        nativeSnapshot = nativeBefore.snapshot(1, 1);
      } else compareFinalRange(before, nativeBefore, allocator, input, output, finalTime, rocks);
    }
    if (checkpoint) {
      try (var allocator = new RootAllocator();
          var after = stock(2, false, rocks, 38, 0, interval, input);
          var nativeAfter = nativeHarness(2, false, rocks, 38, 0, interval, input)) {
        after.setup(new RowDataSerializer(output));
        after.initializeState(stockSnapshot);
        after.open();
        nativeAfter.setup(new ArrowBatchSerializer());
        nativeAfter.initializeState(nativeSnapshot);
        nativeAfter.open();
        compareFinalRange(after, nativeAfter, allocator, input, output, finalTime, rocks);
      }
    }
  }

  private static void compareFinalRange(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock,
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness,
      RootAllocator allocator,
      RowType input,
      RowType output,
      long time,
      boolean rocks)
      throws Exception {
    var row = GenericRowData.of(DecimalData.fromBigDecimal(BigDecimal.valueOf(7), 38, 0), time);
    stock.processElement(new StreamRecord<>(row));
    nativeHarness.processElement(
        new StreamRecord<>(
            new ArrowBatch(RowDataArrowConverter.write(List.of(row), input, allocator))));
    stock.processWatermark(new Watermark(time));
    nativeHarness.processWatermark(new Watermark(time));
    var actual = drainStock(stock, 0);
    assertEquals(1, actual.size());
    assertEquals(BigDecimal.valueOf(rocks ? 7 : 8), actual.get(0).get(0));
    assertEquals(actual, drainNative(nativeHarness, output, 0));
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void liveHeapRangeOrderDependsOnAnOutstandingSnapshot(boolean outstanding, boolean pendingInput)
      throws Exception {
    var input = RowType.of(new DecimalType(38, 0), new BigIntType());
    var output =
        RowType.of(
            new DecimalType(38, 0),
            new BigIntType(),
            new DecimalType(38, 0),
            new DecimalType(38, 0),
            new DecimalType(38, 6));
    try (var allocator = new RootAllocator();
        var stock = stock(2, false, false, 38, 0, 1000, input);
        var nativeHarness = nativeHarness(2, false, false, 38, 0, 1000, input)) {
      stock.setup(new RowDataSerializer(output));
      stock.open();
      nativeHarness.setup(new ArrowBatchSerializer());
      nativeHarness.open();
      String[] values = {"99999999999999999999999999999999999999", "-1", "2"};
      long[] times = {100, 200, 400};
      for (int i = 0; i < times.length; i++) {
        var row =
            GenericRowData.of(
                DecimalData.fromBigDecimal(new BigDecimal(values[i]), 38, 0), times[i]);
        stock.processElement(new StreamRecord<>(row));
        nativeHarness.processElement(
            new StreamRecord<>(
                new ArrowBatch(RowDataArrowConverter.write(List.of(row), input, allocator))));
        stock.processWatermark(new Watermark(times[i]));
        nativeHarness.processWatermark(new Watermark(times[i]));
        assertEquals(drainStock(stock, 0), drainNative(nativeHarness, output, 0));
      }
      var next = GenericRowData.of(DecimalData.fromBigDecimal(BigDecimal.valueOf(7), 38, 0), 2000L);
      if (pendingInput) {
        stock.processElement(new StreamRecord<>(next));
        nativeHarness.processElement(
            new StreamRecord<>(
                new ArrowBatch(RowDataArrowConverter.write(List.of(next), input, allocator))));
      }
      var operator = (KeyedProcessOperator<RowData, RowData, RowData>) stock.getOperator();
      var backend =
          (org.apache.flink.runtime.state.CheckpointableKeyedStateBackend<?>)
              operator.getKeyedStateBackend();
      var future =
          backend.snapshot(
              1,
              1,
              new org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory(1 << 20),
              org.apache.flink.runtime.checkpoint.CheckpointOptions
                  .forCheckpointWithDefaultLocation());
      var nativeBackend =
          (org.apache.flink.runtime.state.CheckpointableKeyedStateBackend<?>)
              ((NativeOverAggregateOperator) nativeHarness.getOperator()).getKeyedStateBackend();
      var nativeFuture =
          nativeBackend.snapshot(
              1,
              1,
              new org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory(1 << 20),
              org.apache.flink.runtime.checkpoint.CheckpointOptions
                  .forCheckpointWithDefaultLocation());
      org.junit.jupiter.api.Assertions.assertFalse(future.isDone());
      org.junit.jupiter.api.Assertions.assertFalse(nativeFuture.isDone());
      if (!outstanding) {
        future.run();
        future.get().discardState();
        nativeFuture.run();
        nativeFuture.get().discardState();
      }
      try {
        if (!pendingInput) {
          stock.processElement(new StreamRecord<>(next));
          nativeHarness.processElement(
              new StreamRecord<>(
                  new ArrowBatch(RowDataArrowConverter.write(List.of(next), input, allocator))));
        }
        stock.processWatermark(new Watermark(2000));
        nativeHarness.processWatermark(new Watermark(2000));
        var expected = List.of(Arrays.asList(BigDecimal.valueOf(outstanding ? 7 : 8), null, null));
        assertEquals(expected, drainStock(stock, 0));
        assertEquals(expected, drainNative(nativeHarness, output, 0));
      } finally {
        if (outstanding) {
          future.run();
          future.get().discardState();
          nativeFuture.run();
          nativeFuture.get().discardState();
        }
      }
    }
  }

  @org.junit.jupiter.api.Test
  void heapCopyProbeClearsWithRangeStateAndRestoresWithoutStaleCopyFlags() throws Exception {
    var input = RowType.of(new DecimalType(38, 0), new BigIntType());
    var output =
        RowType.of(
            new DecimalType(38, 0),
            new BigIntType(),
            new DecimalType(38, 0),
            new DecimalType(38, 0),
            new DecimalType(38, 6));
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState snapshot;
    String probeState = "__streamfusion_over_heap_copy_probe_v1";
    try (var allocator = new RootAllocator();
        var before = nativeHarness(2, false, false, 38, 0, 1000, input)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      var row = GenericRowData.of(DecimalData.fromBigDecimal(BigDecimal.ONE, 38, 0), 100L);
      before.processElement(
          new StreamRecord<>(
              new ArrowBatch(RowDataArrowConverter.write(List.of(row), input, allocator))));
      before.processWatermark(new Watermark(100));
      drainNative(before, output, 0);
      var backend = ((NativeOverAggregateOperator) before.getOperator()).getKeyedStateBackend();
      try (var keys = backend.getKeysAndNamespaces(probeState)) {
        assertEquals(1, keys.count());
      }
      snapshot = before.snapshot(1, 1);
      before.processWatermark(new Watermark(1601));
      try (var keys = backend.getKeysAndNamespaces(probeState)) {
        assertEquals(0, keys.count());
      }
    }
    try (var allocator = new RootAllocator();
        var after = nativeHarness(2, false, false, 38, 0, 1000, input)) {
      after.setup(new ArrowBatchSerializer());
      after.initializeState(snapshot);
      after.open();
      var row = GenericRowData.of(DecimalData.fromBigDecimal(BigDecimal.ONE, 38, 0), 200L);
      after.processElement(
          new StreamRecord<>(
              new ArrowBatch(RowDataArrowConverter.write(List.of(row), input, allocator))));
      after.processWatermark(new Watermark(200));
      assertEquals(BigDecimal.valueOf(2), drainNative(after, output, 0).get(0).get(0));
      after.processWatermark(new Watermark(1701));
      var backend = ((NativeOverAggregateOperator) after.getOperator()).getKeyedStateBackend();
      try (var keys = backend.getKeysAndNamespaces(probeState)) {
        assertEquals(0, keys.count());
      }
    }
  }

  private static void compare(
      int frame,
      boolean proctime,
      boolean rocks,
      int precision,
      int scale,
      long precedingRows,
      String[] values,
      long[] times)
      throws Exception {
    var input = RowType.of(new DecimalType(precision, scale), new BigIntType());
    var output =
        RowType.of(
            new DecimalType(precision, scale),
            new BigIntType(),
            new DecimalType(38, scale),
            new DecimalType(38, scale),
            new DecimalType(38, Math.max(6, scale)));
    var rows = new ArrayList<RowData>();
    for (int i = 0; i < values.length; i++)
      rows.add(
          GenericRowData.of(
              values[i] == null
                  ? null
                  : DecimalData.fromBigDecimal(new BigDecimal(values[i]), precision, scale),
              times[i]));
    try (var allocator = new RootAllocator();
        var stock = stock(frame, proctime, rocks, precision, scale, precedingRows, input);
        var nativeHarness =
            nativeHarness(frame, proctime, rocks, precision, scale, precedingRows, input)) {
      stock.setup(new RowDataSerializer(output));
      stock.open();
      nativeHarness.setup(new ArrowBatchSerializer());
      nativeHarness.open();
      // Complete one timestamp group at a time; ROWS retains peer arrival order and RANGE
      // folds all peers before either result. Both use the released operator timer machinery.
      for (int start = 0; start < rows.size(); ) {
        int end = start + 1;
        while (end < rows.size() && times[end] == times[start]) end++;
        for (int i = start; i < end; i++) stock.processElement(new StreamRecord<>(rows.get(i)));
        nativeHarness.processElement(
            new StreamRecord<>(
                new ArrowBatch(
                    RowDataArrowConverter.write(rows.subList(start, end), input, allocator))));
        if (!proctime) {
          stock.processWatermark(new Watermark(times[start]));
          nativeHarness.processWatermark(new Watermark(times[start]));
        }
        assertEquals(
            drainStock(stock, scale),
            drainNative(nativeHarness, output, scale),
            "frame="
                + frame
                + " proctime="
                + proctime
                + " rocks="
                + rocks
                + " timestamp="
                + times[start]);
        start = end;
      }
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(
      int frame,
      boolean proctime,
      boolean rocks,
      int precision,
      int scale,
      long precedingRows,
      RowType input)
      throws Exception {
    var generated = FlinkOverDecimalAccumulatorContractTest.generated(precision, scale, true);
    List<LogicalType> stateTypes = new ArrayList<>();
    for (var function : FlinkOverDecimalAccumulatorContractTest.functions(precision, scale, true)) {
      for (var type : function.getAggBufferTypes()) stateTypes.add(type.getLogicalType());
    }
    var state = stateTypes.toArray(new LogicalType[0]);
    var fields = input.getChildren().toArray(new LogicalType[0]);
    org.apache.flink.streaming.api.functions.KeyedProcessFunction<RowData, RowData, RowData>
        function;
    if (proctime)
      function =
          new ProcTimeRowsBoundedPrecedingFunction(
              0, 0, generated, state, fields, precedingRows + 1);
    else if (frame == 1)
      function =
          new RowTimeRowsBoundedPrecedingFunction(
              0, 0, generated, state, fields, precedingRows + 1, 1);
    else
      function =
          new RowTimeRangeBoundedPrecedingFunction(
              generated, state, fields, precedingRows == 1 ? 100 : precedingRows, 1);
    var keyType = RowType.of(new BigIntType());
    var serializer = new RowDataSerializer(keyType);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(function),
            row -> serializer.toBinaryRow(GenericRowData.of(0L)).copy(),
            InternalTypeInfo.of(keyType));
    if (rocks) {
      Class<?> backend;
      try {
        backend = Class.forName("org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend");
      } catch (ClassNotFoundException older) {
        backend =
            Class.forName("org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend");
      }
      harness.setStateBackend((StateBackend) backend.getConstructor().newInstance());
    }
    return harness;
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      nativeHarness(
          int frame,
          boolean proctime,
          boolean rocks,
          int precision,
          int scale,
          long precedingRows,
          RowType input)
          throws Exception {
    int type = 2000 + precision * 100 + scale;
    var operator =
        new NativeOverAggregateOperator(
            1,
            new int[] {0, 0, 0},
            new int[0],
            new int[] {type, type, type},
            new int[] {0, 22, 4},
            frame,
            frame == 1 ? precedingRows : precedingRows == 1 ? 100 : precedingRows,
            proctime,
            new int[0],
            0,
            input,
            128);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(operator, ignored -> 0, Types.INT, 128, 1, 0);
    if (rocks)
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), FlinkNativeBoundedDecimalOverTest.class.getClassLoader()));
    return harness;
  }

  private static List<List<BigDecimal>> drainStock(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness, int scale) {
    List<List<BigDecimal>> rows = new ArrayList<>();
    Object event;
    while ((event = harness.getOutput().poll()) != null) {
      if (event instanceof StreamRecord)
        rows.add(values((RowData) ((StreamRecord<?>) event).getValue(), scale));
    }
    return rows;
  }

  private static List<List<BigDecimal>> drainNative(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      RowType output,
      int scale) {
    List<List<BigDecimal>> rows = new ArrayList<>();
    Object event;
    while ((event = harness.getOutput().poll()) != null) {
      if (event instanceof StreamRecord) {
        try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (var row : RowDataArrowConverter.read(root, output)) rows.add(values(row, scale));
        }
      }
    }
    return rows;
  }

  private static List<BigDecimal> values(RowData row, int scale) {
    return Arrays.asList(
        row.isNullAt(2) ? null : row.getDecimal(2, 38, scale).toBigDecimal(),
        row.isNullAt(3) ? null : row.getDecimal(3, 38, scale).toBigDecimal(),
        row.isNullAt(4) ? null : row.getDecimal(4, 38, Math.max(6, scale)).toBigDecimal());
  }
}

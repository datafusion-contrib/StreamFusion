package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.planner.FlinkKeyGroupUtils;
import tech.streamfusion.state.*;

class CanonicalTemporalJoinInteropTest {
  private static final RowType INPUT =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
  private static final RowType KEY = RowType.of(new BigIntType());
  private static final RowType OUTPUT =
      RowType.of(
          new BigIntType(),
          new BigIntType(),
          new BigIntType(),
          new BigIntType(),
          new BigIntType(),
          new BigIntType());
  private static final SavepointType CANONICAL =
      SavepointType.savepoint(SavepointFormatType.CANONICAL);

  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> cases() throws Exception {
    return tech.streamfusion.CanonicalTestMatrix.select(allCases());
  }

  private static Stream<Arguments> allCases() throws Exception {
    var result = new ArrayList<Arguments>();
    for (String scenario : CanonicalTemporalMetadataFixtureTest.traces().keySet())
      for (boolean stock : new boolean[] {false, true})
        for (boolean nativeStore : new boolean[] {false, true})
          for (boolean outer : new boolean[] {false, true})
            result.add(Arguments.of(scenario, stock, nativeStore, outer));
    return Boolean.getBoolean("streamfusion.test.temporalJoinSmoke")
        ? result.stream()
            .skip(Integer.getInteger("streamfusion.test.temporalJoinSmokeCase", 0))
            .limit(1)
        : result.stream();
  }

  @ParameterizedTest(name = "{0} stock={1} native={2} outer={3}")
  @MethodSource("cases")
  void releasedTraceSurvivesDirectStockTransferAndNativeRecoveryAfterEveryTransition(
      String scenario, boolean stockStore, boolean nativeStore, boolean outer) throws Exception {
    var trace = CanonicalTemporalMetadataFixtureTest.traces().get(scenario);
    long retention = Long.parseLong(trace.get(0)[1]);
    KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> nativeRun =
        null;
    long clock = 0, ordinal = 0, id = 0;
    List<List<Object>> continuedStock = null;
    try (var reference =
        CanonicalTemporalMetadataFixtureTest.harness(stockStore, retention, outer)) {
      reference.open();
      try {
        for (int step = 0; step < trace.size(); step++) {
          var row = trace.get(step);
          long argument = Long.parseLong(row[3]);
          String label = String.join("/", row);
          switch (row[2]) {
            case "L":
              var left = CanonicalTemporalMetadataFixtureTest.data(++ordinal, argument);
              reference.processElement1(new StreamRecord<>(left));
              if (nativeRun != null) push(nativeRun, true, left);
              break;
            case "R":
              var right = CanonicalTemporalMetadataFixtureTest.data(argument, argument);
              reference.processElement2(new StreamRecord<>(right));
              if (nativeRun != null) push(nativeRun, false, right);
              break;
            case "W":
              reference.processBothWatermarks(new Watermark(argument));
              if (nativeRun != null) nativeRun.processBothWatermarks(new Watermark(argument));
              break;
            case "T":
            case "C":
              clock = argument;
              reference.setProcessingTime(clock);
              if (nativeRun != null) nativeRun.setProcessingTime(clock);
              break;
            case "S":
              assertNull(nativeRun, "Seed is the first transition");
              var backend =
                  (CheckpointableKeyedStateBackend<RowData>)
                      reference.getOperator().<RowData>getKeyedStateBackend();
              backend.setCurrentKey(
                  new RowDataSerializer(KEY).toBinaryRow(GenericRowData.of(1L)).copy());
              backend
                  .getPartitionedState(
                      VoidNamespace.INSTANCE,
                      VoidNamespaceSerializer.INSTANCE,
                      new ValueStateDescriptor<>("next-index", LongSerializer.INSTANCE))
                  .update(argument);
              break;
            default:
              fail(label);
          }
          CanonicalTemporalMetadataFixtureTest.check(reference, row, retention, false);
          if (nativeRun == null) {
            var source = snapshot(reference, ++id);
            try {
              nativeRun = nativeHarness(nativeStore, retention, outer);
              nativeRun.initializeState(source);
              nativeRun.setProcessingTime(clock);
              nativeRun.open();
            } finally {
              source.discardState();
            }
          }
          var expected = stockOutput(reference);
          assertEquals(expected, nativeOutput(nativeRun), label);
          if (continuedStock != null)
            assertEquals(
                expected,
                continuedStock,
                "Continued stock execution after native export: " + label);
          continuedStock = null;
          assertEquals(
              nativeStore,
              ((NativeTemporalJoinOperator) nativeRun.getOperator()).directRocksDBState());
          long allocated = NativeAllocator.SHARED.getAllocatedMemory();
          var saved = snapshot(nativeRun, ++id);
          assertEquals(
              allocated,
              NativeAllocator.SHARED.getAllocatedMemory(),
              "Direct export retained Java Arrow buffers");
          try (var restored =
              CanonicalTemporalMetadataFixtureTest.harness(!stockStore, retention, outer)) {
            restored.initializeState(saved);
            restored.setProcessingTime(clock);
            restored.open();
            CanonicalTemporalMetadataFixtureTest.check(restored, row, retention, true);
            verifyMetadata(restored);
            if (step + 1 < trace.size()) {
              var next = trace.get(step + 1);
              continueStock(restored, next, ordinal);
              CanonicalTemporalMetadataFixtureTest.check(restored, next, retention, false);
              continuedStock = stockOutput(restored);
            }
          }
          // Alternate ordinary native recovery with a stock-schema re-import. Both must preserve
          // state used by the next native action, including callbacks without cached rows.
          var recovery = id % 4 == 0 ? nativeRun.snapshot(++id, 0) : saved;
          closeNative(nativeRun);
          nativeRun = null;
          try {
            nativeRun = nativeHarness(nativeStore, retention, outer);
            nativeRun.initializeState(recovery);
            nativeRun.setProcessingTime(clock);
            nativeRun.open();
            assertEquals(
                allocated,
                NativeAllocator.SHARED.getAllocatedMemory(),
                "Import retained Java Arrow buffers");
          } finally {
            if (recovery != saved) recovery.discardState();
            saved.discardState();
          }
        }
      } finally {
        if (nativeRun != null) closeNative(nativeRun);
      }
    }
  }

  static Stream<Arguments> stores() {
    var result = new ArrayList<Arguments>();
    for (boolean stock : new boolean[] {false, true})
      for (boolean nativeStore : new boolean[] {false, true})
        for (boolean outer : new boolean[] {false, true})
          result.add(Arguments.of(stock, nativeStore, outer));
    return result.stream();
  }

  @ParameterizedTest
  @MethodSource("stores")
  void continuedRowsAndRetractionsSurviveBothEngineOrders(
      boolean stockStore, boolean nativeStore, boolean outer) throws Exception {
    try (var reference = CanonicalTemporalMetadataFixtureTest.harness(stockStore, 0, outer);
        var nativeRun = nativeHarness(nativeStore, 0, outer)) {
      reference.open();
      nativeRun.open();
      var prefixRight =
          List.of(
              changed(org.apache.flink.types.RowKind.INSERT, 1L, 100L, 100),
              changed(org.apache.flink.types.RowKind.UPDATE_AFTER, 1L, 200L, 200),
              changed(org.apache.flink.types.RowKind.DELETE, 2L, null, 100));
      var prefixLeft =
          List.of(
              changed(org.apache.flink.types.RowKind.INSERT, 1L, null, 150),
              changed(org.apache.flink.types.RowKind.UPDATE_AFTER, 1L, 20L, 250),
              changed(org.apache.flink.types.RowKind.DELETE, 2L, 30L, 150),
              changed(org.apache.flink.types.RowKind.UPDATE_BEFORE, 3L, null, 50));
      for (var row : prefixRight) {
        reference.processElement2(new StreamRecord<>(row));
        push(nativeRun, false, row);
      }
      for (var row : prefixLeft) {
        reference.processElement1(new StreamRecord<>(row));
        push(nativeRun, true, row);
      }
      var saved = snapshot(nativeRun, 1);
      try (var stock = CanonicalTemporalMetadataFixtureTest.harness(!stockStore, 0, outer)) {
        stock.initializeState(saved);
        stock.open();
        verifyMetadata(stock);
        for (var row :
            List.of(
                changed(org.apache.flink.types.RowKind.UPDATE_BEFORE, 1L, 400L, 300),
                changed(org.apache.flink.types.RowKind.UPDATE_AFTER, 2L, 22L, 250),
                changed(org.apache.flink.types.RowKind.UPDATE_AFTER, 1L, 222L, 200))) {
          reference.processElement2(new StreamRecord<>(row));
          stock.processElement2(new StreamRecord<>(row));
          push(nativeRun, false, row);
        }
        for (var row :
            List.of(
                changed(org.apache.flink.types.RowKind.INSERT, 1L, 31L, 350),
                changed(org.apache.flink.types.RowKind.DELETE, 2L, 40L, 350),
                changed(org.apache.flink.types.RowKind.UPDATE_AFTER, 4L, null, 400))) {
          reference.processElement1(new StreamRecord<>(row));
          stock.processElement1(new StreamRecord<>(row));
          push(nativeRun, true, row);
        }
        reference.processBothWatermarks(new Watermark(1000));
        stock.processBothWatermarks(new Watermark(1000));
        nativeRun.processBothWatermarks(new Watermark(1000));
        var expected = stockOutput(reference);
        var nativeRows = nativeOutput(nativeRun);
        var stockRows = stockOutput(stock);
        expected.sort(Comparator.comparing(Object::toString));
        nativeRows.sort(Comparator.comparing(Object::toString));
        stockRows.sort(Comparator.comparing(Object::toString));
        assertEquals(expected, nativeRows);
        assertEquals(expected, stockRows);
        var stockSaved = snapshot(reference, 2);
        try (var imported = nativeHarness(!nativeStore, 0, outer)) {
          imported.initializeState(stockSaved);
          imported.open();
          var late = changed(org.apache.flink.types.RowKind.DELETE, 1L, 77L, 150);
          reference.processElement1(new StreamRecord<>(late));
          stock.processElement1(new StreamRecord<>(late));
          push(nativeRun, true, late);
          push(imported, true, late);
          reference.processBothWatermarks(new Watermark(2000));
          stock.processBothWatermarks(new Watermark(2000));
          nativeRun.processBothWatermarks(new Watermark(2000));
          imported.processBothWatermarks(new Watermark(2000));
          expected = stockOutput(reference);
          assertEquals(expected, stockOutput(stock));
          assertEquals(expected, nativeOutput(nativeRun));
          assertEquals(expected, nativeOutput(imported));
        } finally {
          stockSaved.discardState();
        }
      } finally {
        saved.discardState();
      }
    }
  }

  private static RowData changed(
      org.apache.flink.types.RowKind kind, Long key, Long value, long time) {
    var row = GenericRowData.of(key, value, time);
    row.setRowKind(kind);
    return new RowDataSerializer(INPUT).toBinaryRow(row).copy();
  }

  private static void closeNative(
      KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> harness)
      throws Exception {
    try {
      harness.close();
    } finally {
      // The harness calls finish() during close, after the state handoff has already completed.
      // Release those teardown-only batches as well as ordinary consumed output.
      while (!harness.getOutput().isEmpty()) {
        var event = harness.getOutput().poll();
        if (event instanceof StreamRecord)
          ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root().close();
      }
    }
  }

  private static void continueStock(
      KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> harness,
      String[] row,
      long ordinal)
      throws Exception {
    long argument = Long.parseLong(row[3]);
    switch (row[2]) {
      case "L":
        harness.processElement1(
            new StreamRecord<>(CanonicalTemporalMetadataFixtureTest.data(ordinal + 1, argument)));
        break;
      case "R":
        harness.processElement2(
            new StreamRecord<>(CanonicalTemporalMetadataFixtureTest.data(argument, argument)));
        break;
      case "W":
        harness.processBothWatermarks(new Watermark(argument));
        break;
      case "T":
      case "C":
        harness.setProcessingTime(argument);
        break;
      default:
        fail("Unexpected continuation action " + row[2]);
    }
  }

  private static OperatorSubtaskState snapshot(
      org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness<?> harness, long id)
      throws Exception {
    harness.prepareSnapshotPreBarrier(id);
    return harness.snapshotWithLocalState(id, 0, CANONICAL).getJobManagerOwnedState();
  }

  private static void push(
      KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> harness,
      boolean left,
      RowData row)
      throws Exception {
    var record =
        new StreamRecord<>(
            new ArrowBatch(
                RowDataArrowConverter.write(List.of(row), INPUT, NativeAllocator.SHARED, true)));
    if (left) harness.processElement1(record);
    else harness.processElement2(record);
  }

  private static List<Object> formatted(RowData row) {
    var result = new ArrayList<Object>();
    result.add(row.getRowKind());
    for (int i = 0; i < 6; i++) result.add(row.isNullAt(i) ? null : row.getLong(i));
    return result;
  }

  private static List<List<Object>> stockOutput(
      KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> harness) {
    var result = new ArrayList<List<Object>>();
    while (!harness.getOutput().isEmpty()) {
      var event = harness.getOutput().poll();
      if (event instanceof StreamRecord)
        result.add(formatted((RowData) ((StreamRecord<?>) event).getValue()));
    }
    return result;
  }

  private static List<List<Object>> nativeOutput(
      KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> harness) {
    var result = new ArrayList<List<Object>>();
    while (!harness.getOutput().isEmpty()) {
      var event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (var row : RowDataArrowConverter.read(root, OUTPUT)) result.add(formatted(row));
        }
      }
    }
    return result;
  }

  static KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
      nativeHarness(boolean rocks, long retention, boolean outer) throws Exception {
    return nativeHarness(rocks, retention, outer, INPUT);
  }

  static KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
      nativeHarness(boolean rocks, long retention, boolean outer, RowType input) throws Exception {
    var operator = new CanonicalTemporal(retention, outer, input);
    var contract =
        new StockTemporalJoinStateContract(
            new RowDataSerializer(KEY),
            KEY,
            input,
            input,
            new int[] {0},
            new int[] {0},
            2,
            2,
            retention);
    ClassLoader runtimeLoader =
        new ClassLoader(CanonicalTemporalJoinInteropTest.class.getClassLoader()) {
          @Override
          protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith("org.apache.flink.table.planner."))
              throw new ClassNotFoundException(name);
            return super.loadClass(name, resolve);
          }
        };
    contract =
        org.apache.flink.util.InstantiationUtil.deserializeObject(
            org.apache.flink.util.InstantiationUtil.serializeObject(contract), runtimeLoader);
    operator.setStockStateContract(contract);
    int key = FlinkKeyGroupUtils.stateKeysForSubtasks(128, 1)[0];
    var harness =
        new KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>(
            operator, ignored -> key, ignored -> key, Types.INT, 128, 1, 0);
    var config = new Configuration();
    if (rocks)
      config.setString(
          org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND.key(),
          RocksDBNativeStateBackendFactory.class.getName());
    CanonicalStateBackend.install(
        StreamExecutionEnvironment.getExecutionEnvironment(),
        config,
        harness.getStreamConfig().getOperatorID().toHexString(),
        operator.getClass().getSimpleName(),
        contract,
        CanonicalTemporalJoinInteropTest.class.getClassLoader());
    harness.setStateBackend(
        new CanonicalStateBackendFactory()
            .createFromConfig(config, CanonicalTemporalJoinInteropTest.class.getClassLoader()));
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }

  private static void verifyMetadata(
      KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> harness)
      throws Exception {
    var backend =
        (CheckpointableKeyedStateBackend<RowData>)
            harness.getOperator().<RowData>getKeyedStateBackend();
    var resources = backend.savepoint().getSnapshotResources();
    var clean =
        new ClassLoader(CanonicalTemporalJoinInteropTest.class.getClassLoader()) {
          @Override
          protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith("tech.streamfusion.")
                || name.startsWith("org.apache.flink.table.planner."))
              throw new ClassNotFoundException(name);
            return super.loadClass(name, resolve);
          }
        };
    try (AutoCloseable release = resources::release) {
      for (var meta : resources.getMetaInfoSnapshots())
        for (var serializer : meta.getSerializerSnapshotsImmutable().values()) {
          var output = new org.apache.flink.core.memory.DataOutputSerializer(128);
          org.apache.flink.api.common.typeutils.TypeSerializerSnapshot.writeVersionedSnapshot(
              output, serializer);
          var input =
              new org.apache.flink.core.memory.DataInputDeserializer(output.getCopyOfBuffer());
          assertNotNull(
              org.apache.flink.api.common.typeutils.TypeSerializerSnapshot.readVersionedSnapshot(
                      input, clean)
                  .restoreSerializer());
        }
    }
  }

  private static final class CanonicalTemporal extends NativeTemporalJoinOperator {
    private boolean canonical;

    CanonicalTemporal(long retention, boolean outer, RowType input) {
      super(
          new int[] {0},
          new int[] {0},
          2,
          2,
          outer ? 1 : 0,
          input,
          input,
          EncodedPredicate.NONE,
          new int[] {-1},
          retention,
          128);
    }

    @Override
    protected org.apache.flink.streaming.api.operators.OperatorSnapshotFutures
        snapshotCanonicalState(
            long id,
            long timestamp,
            org.apache.flink.runtime.checkpoint.CheckpointOptions options,
            org.apache.flink.runtime.state.CheckpointStreamFactory streams)
            throws Exception {
      canonical = true;
      try {
        return super.snapshotCanonicalState(id, timestamp, options, streams);
      } finally {
        canonical = false;
      }
    }

    @Override
    protected byte[][] snapshotCanonicalPartitions() {
      if (canonical)
        throw new AssertionError(
            "Temporal canonical export must visit live state without a native snapshot");
      return super.snapshotCanonicalPartitions();
    }
  }
}

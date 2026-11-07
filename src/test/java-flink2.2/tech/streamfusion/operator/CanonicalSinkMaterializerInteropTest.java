package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.*;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.planner.FlinkKeyGroupUtils;
import tech.streamfusion.state.*;
import tech.streamfusion.state.StockSinkMaterializerStateContract.Strategy;

class CanonicalSinkMaterializerInteropTest {
  static Stream<Arguments> modes() {
    var values = new ArrayList<Arguments>();
    for (String strategy : List.of("LEGACY", "VALUE", "MAP", "ADAPTIVE_SMALL", "ADAPTIVE_LARGE"))
      for (boolean inputKey : new boolean[] {false, true})
        for (boolean stockStore : new boolean[] {false, true})
          for (boolean nativeStore : new boolean[] {false, true})
            for (boolean older : new boolean[] {false, true})
              values.add(Arguments.of(strategy, inputKey, stockStore, nativeStore, older));
    return values.stream();
  }

  @ParameterizedTest
  @MethodSource("modes")
  void stockStateAndItsContinuationSurviveRepeatedCanonicalHandoffs(
      String strategy, boolean inputKey, boolean stockStore, boolean nativeStore, boolean older)
      throws Exception {
    OperatorSubtaskState original, exported;
    List<String> expectedState, expectedOutput;
    try (var stock =
        CanonicalSinkMaterializerProtocolTest.harness(strategy, inputKey, stockStore, false)) {
      stock.open();
      CanonicalSinkMaterializerProtocolTest.prefix(stock, older);
      expectedState = logicalCandidates(stock, strategy);
      original = CanonicalSinkMaterializerProtocolTest.snapshot(stock);
      stock.getOutput().clear();
      CanonicalSinkMaterializerProtocolTest.suffix(stock, strategy, inputKey, older);
      expectedOutput = CanonicalSinkMaterializerProtocolTest.output(stock);
    }
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var nativeOp = nativeHarness(strategy, inputKey, nativeStore)) {
      nativeOp.initializeState(original);
      nativeOp.open();
      exported = CanonicalTemporalSortInteropTest.snapshot(nativeOp);
    }
    try (var stock =
        CanonicalSinkMaterializerProtocolTest.harness(strategy, inputKey, stockStore, false)) {
      stock.initializeState(exported);
      stock.open();
      assertEquals(expectedState, logicalCandidates(stock, strategy));
      original = CanonicalSinkMaterializerProtocolTest.snapshot(stock);
      CanonicalSinkMaterializerProtocolTest.suffix(stock, strategy, inputKey, older);
      assertEquals(expectedOutput, CanonicalSinkMaterializerProtocolTest.output(stock));
    }
    try (var nativeOp = nativeHarness(strategy, inputKey, nativeStore)) {
      nativeOp.initializeState(original);
      nativeOp.open();
      exported = CanonicalTemporalSortInteropTest.snapshot(nativeOp);
    }
    try (var stock =
        CanonicalSinkMaterializerProtocolTest.harness(strategy, inputKey, stockStore, false)) {
      stock.initializeState(exported);
      stock.open();
      assertEquals(expectedState, logicalCandidates(stock, strategy));
      CanonicalSinkMaterializerProtocolTest.suffix(stock, strategy, inputKey, older);
      assertEquals(expectedOutput, CanonicalSinkMaterializerProtocolTest.output(stock));
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  static Stream<Arguments> nativeModes() {
    return modes().filter(arguments -> !(boolean) arguments.get()[4]);
  }

  @ParameterizedTest
  @MethodSource("nativeModes")
  void nativeCandidatesAndActualResumedInputContinueIntoTheReleasedStockOperator(
      String strategy, boolean inputKey, boolean stockStore, boolean nativeStore, boolean older)
      throws Exception {
    OperatorSubtaskState saved;
    List<String> expected;
    try (var stock =
        CanonicalSinkMaterializerProtocolTest.harness(strategy, inputKey, stockStore, false)) {
      stock.open();
      for (RowData row : prefix()) stock.processElement(new StreamRecord<>(row, 100));
      stock.getOutput().clear();
      CanonicalSinkMaterializerProtocolTest.suffix(stock, strategy, inputKey, false);
      expected = CanonicalSinkMaterializerProtocolTest.output(stock);
    }
    try (var nativeOp = nativeHarness(strategy, inputKey, nativeStore)) {
      nativeOp.open();
      nativeOp.setProcessingTime(100);
      for (RowData row : prefix()) push(nativeOp, row);
      output(nativeOp);
      saved = CanonicalTemporalSortInteropTest.snapshot(nativeOp);
    }
    try (var stock =
        CanonicalSinkMaterializerProtocolTest.harness(strategy, inputKey, stockStore, false)) {
      stock.initializeState(saved);
      stock.open();
      CanonicalSinkMaterializerProtocolTest.suffix(stock, strategy, inputKey, false);
      assertEquals(expected, CanonicalSinkMaterializerProtocolTest.output(stock));
    }
    try (var restored = nativeHarness(strategy, inputKey, nativeStore)) {
      restored.initializeState(saved);
      restored.open();
      var actual = new ArrayList<String>();
      for (RowData row : suffix()) {
        push(restored, row);
        actual.addAll(output(restored));
      }
      assertEquals(expected, actual);
    }
  }

  static Stream<Arguments> ttlModes() {
    var result = new ArrayList<Arguments>();
    for (boolean stockStore : new boolean[] {false, true})
      for (boolean nativeStore : new boolean[] {false, true})
        for (boolean inputKey : new boolean[] {false, true})
          for (boolean checkpoint : new boolean[] {false, true})
            result.add(Arguments.of(stockStore, nativeStore, inputKey, checkpoint));
    return result.stream();
  }

  @ParameterizedTest
  @MethodSource("ttlModes")
  void legacyTtlAgeSurvivesCanonicalHandoffsAndOrdinaryNativeRecovery(
      boolean stockStore, boolean nativeStore, boolean inputKey, boolean checkpoint)
      throws Exception {
    OperatorSubtaskState saved;
    try (var stock =
        CanonicalSinkMaterializerProtocolTest.harness("LEGACY", inputKey, stockStore, true)) {
      stock.open();
      stock.setStateTtlProcessingTime(100);
      stock.processElement(new StreamRecord<>(row(RowKind.INSERT, 1, 10, 100)));
      saved = CanonicalSinkMaterializerProtocolTest.snapshot(stock);
    }
    try (var nativeOp = nativeHarness("LEGACY", inputKey, nativeStore, 1000)) {
      nativeOp.initializeState(saved);
      nativeOp.open();
      nativeOp.setProcessingTime(1099);
      saved =
          checkpoint
              ? nativeOp.snapshot(1, 1099)
              : CanonicalTemporalSortInteropTest.snapshot(nativeOp);
    }
    if (checkpoint) {
      try (var nativeOp = nativeHarness("LEGACY", inputKey, nativeStore, 1000)) {
        nativeOp.initializeState(saved);
        nativeOp.open();
        nativeOp.setProcessingTime(1099);
        saved = CanonicalTemporalSortInteropTest.snapshot(nativeOp);
      }
    }
    try (var stock =
        CanonicalSinkMaterializerProtocolTest.harness("LEGACY", inputKey, stockStore, true)) {
      stock.initializeState(saved);
      stock.open();
      stock.setStateTtlProcessingTime(1099);
      saved = CanonicalSinkMaterializerProtocolTest.snapshot(stock);
    }
    try (var nativeOp = nativeHarness("LEGACY", inputKey, nativeStore, 1000)) {
      nativeOp.initializeState(saved);
      nativeOp.open();
      nativeOp.setProcessingTime(1100);
      push(nativeOp, row(RowKind.DELETE, 1, 10, 100));
      assertEquals(List.of(), output(nativeOp));
      push(nativeOp, row(RowKind.INSERT, 1, 20, 200));
      assertEquals(List.of("+I:1:20:200"), output(nativeOp));
    }
    try (var stock =
        CanonicalSinkMaterializerProtocolTest.harness("LEGACY", inputKey, stockStore, true)) {
      stock.initializeState(saved);
      stock.open();
      stock.setStateTtlProcessingTime(1100);
      stock.processElement(new StreamRecord<>(row(RowKind.DELETE, 1, 10, 100)));
      assertEquals(List.of(), CanonicalSinkMaterializerProtocolTest.output(stock));
      stock.processElement(new StreamRecord<>(row(RowKind.INSERT, 1, 20, 200)));
      assertEquals(List.of("+I:1:20:200"), CanonicalSinkMaterializerProtocolTest.output(stock));
    }
  }

  static Stream<Arguments> corruptions() {
    return Stream.of(
        Arguments.of("unknown", false),
        Arguments.of("unknown", true),
        Arguments.of("key", false),
        Arguments.of("key", true));
  }

  @ParameterizedTest
  @MethodSource("corruptions")
  void malformedStockStateFailsWithoutRetainingNativeOrArrowOwnership(String mode, boolean rocks)
      throws Exception {
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    OperatorSubtaskState state;
    try (var stock = CanonicalSinkMaterializerProtocolTest.harness("LEGACY", true, false, false)) {
      stock.open();
      stock.processElement(new StreamRecord<>(row(RowKind.INSERT, 1, 10, 100)));
      var backend = stock.getOperator().<RowData>getKeyedStateBackend();
      backend.setCurrentKey(
          new RowDataSerializer(CanonicalSinkMaterializerProtocolTest.KEY)
              .toBinaryRow(GenericRowData.of(1L))
              .copy());
      if (mode.equals("unknown")) {
        backend
            .getPartitionedState(
                org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                org.apache.flink.runtime.state.VoidNamespaceSerializer.INSTANCE,
                new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                    "unowned", org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE))
            .update(7L);
      } else {
        backend
            .getPartitionedState(
                org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                org.apache.flink.runtime.state.VoidNamespaceSerializer.INSTANCE,
                new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                    "values",
                    new org.apache.flink.api.common.typeutils.base.ListSerializer<>(
                        new RowDataSerializer(CanonicalSinkMaterializerProtocolTest.TYPE))))
            .update(List.of(row(RowKind.INSERT, 2, 10, 100)));
      }
      state = CanonicalSinkMaterializerProtocolTest.snapshot(stock);
    }
    try (var target = nativeHarness("LEGACY", true, rocks)) {
      var error = assertThrows(Exception.class, () -> target.initializeState(state));
      var text = new StringBuilder();
      for (Throwable c = error; c != null; c = c.getCause()) text.append(c.getMessage());
      assertTrue(
          text.toString().contains(mode.equals("unknown") ? "not owned" : "candidate key changed"),
          text.toString());
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @ParameterizedTest
  @MethodSource("nativeModes")
  void emptyCanonicalSavepointsRestoreInEitherEngine(
      String strategy, boolean inputKey, boolean stockStore, boolean nativeStore, boolean older)
      throws Exception {
    OperatorSubtaskState saved;
    try (var source = nativeHarness(strategy, inputKey, nativeStore)) {
      source.open();
      saved = CanonicalTemporalSortInteropTest.snapshot(source);
    }
    try (var stock =
        CanonicalSinkMaterializerProtocolTest.harness(strategy, inputKey, stockStore, false)) {
      stock.initializeState(saved);
      stock.open();
      assertEquals(List.of(), logicalCandidates(stock, strategy));
      verifyStockSerializers(stock);
      saved = CanonicalSinkMaterializerProtocolTest.snapshot(stock);
    }
    try (var target = nativeHarness(strategy, inputKey, nativeStore)) {
      target.initializeState(saved);
      target.open();
      push(target, row(RowKind.INSERT, 1, 10, 100));
      assertEquals(List.of("+I:1:10:100"), output(target));
    }
  }

  static Stream<Arguments> nestedModes() {
    var result = new ArrayList<Arguments>();
    for (String strategy : List.of("MAP", "ADAPTIVE_LARGE"))
      for (boolean key : new boolean[] {false, true})
        for (boolean rocks : new boolean[] {false, true})
          result.add(Arguments.of(strategy, key, rocks));
    return result.stream();
  }

  @ParameterizedTest
  @MethodSource("nestedModes")
  void nestedMatchingIndexesOwnTheirImportedDataAfterJavaBuffersClose(
      String strategy, boolean inputKey, boolean rocks) throws Exception {
    var input =
        org.apache.flink.table.types.logical.RowType.of(
            new org.apache.flink.table.types.logical.BigIntType(),
            new org.apache.flink.table.types.logical.ArrayType(
                new org.apache.flink.table.types.logical.BigIntType()),
            new org.apache.flink.table.types.logical.BigIntType());
    OperatorSubtaskState saved;
    try (var stock =
        CanonicalSinkMaterializerProtocolTest.harness(strategy, inputKey, false, false, input)) {
      stock.open();
      for (RowData value :
          List.of(
              GenericRowData.of(1L, new GenericArrayData(new Long[] {1L, null}), 10L),
              GenericRowData.of(1L, new GenericArrayData(new Long[] {2L, 3L}), 20L),
              GenericRowData.of(1L, new GenericArrayData(new Long[] {4L}), 30L)))
        stock.processElement(
            new StreamRecord<>(new RowDataSerializer(input).toBinaryRow(value).copy(), 123));
      saved = CanonicalSinkMaterializerProtocolTest.snapshot(stock);
    }
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var nativeOp =
        nativeHarness(
            strategy,
            inputKey,
            rocks,
            0,
            input,
            inputKey ? new int[] {-1, -1} : new int[] {-1, -1, -1, -1})) {
      nativeOp.initializeState(saved);
      nativeOp.open();
      assertEquals(
          allocated,
          NativeAllocator.SHARED.getAllocatedMemory(),
          "Native state retained Java import buffers");
      saved = CanonicalTemporalSortInteropTest.snapshot(nativeOp);
    }
    try (var stock =
        CanonicalSinkMaterializerProtocolTest.harness(strategy, inputKey, false, false, input)) {
      stock.initializeState(saved);
      stock.open();
      verifyStockSerializers(stock);
      stock.processElement(
          new StreamRecord<>(
              new RowDataSerializer(input)
                  .toBinaryRow(
                      GenericRowData.ofKind(
                          RowKind.DELETE, 1L, new GenericArrayData(new Long[] {4L}), 30L))
                  .copy(),
              999));
      var output = stock.extractOutputValues();
      assertEquals(1, output.size());
      assertEquals(RowKind.UPDATE_AFTER, output.get(0).getRowKind());
      assertEquals(20L, output.get(0).getLong(2));
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private static final ClassLoader STOCK_CLASSES =
      new ClassLoader(CanonicalSinkMaterializerInteropTest.class.getClassLoader()) {
        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
          if (name.startsWith("tech.streamfusion.")
              || name.startsWith("org.apache.flink.table.planner."))
            throw new ClassNotFoundException(name);
          return super.loadClass(name, resolve);
        }
      };

  private static void verifyStockSerializers(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock) throws Exception {
    var backend =
        (org.apache.flink.runtime.state.CheckpointableKeyedStateBackend<RowData>)
            stock.getOperator().<RowData>getKeyedStateBackend();
    var resources = backend.savepoint().getSnapshotResources();
    try (AutoCloseable release = resources::release) {
      for (var meta : resources.getMetaInfoSnapshots())
        for (var snapshot : meta.getSerializerSnapshotsImmutable().values()) {
          var output = new org.apache.flink.core.memory.DataOutputSerializer(128);
          org.apache.flink.api.common.typeutils.TypeSerializerSnapshot.writeVersionedSnapshot(
              output, snapshot);
          var restored =
              org.apache.flink.api.common.typeutils.TypeSerializerSnapshot.readVersionedSnapshot(
                  new org.apache.flink.core.memory.DataInputDeserializer(output.getCopyOfBuffer()),
                  STOCK_CLASSES);
          assertTrue(
              restored.restoreSerializer().getClass().getName().startsWith("org.apache.flink."));
        }
    }
  }

  private static List<RowData> prefix() {
    return List.of(
        row(RowKind.INSERT, 1, 10, 100),
        row(RowKind.INSERT, 1, 20, 200),
        row(RowKind.INSERT, 1, 30, 300),
        row(RowKind.UPDATE_AFTER, 1, 30, 101),
        row(RowKind.INSERT, 2, 40, null));
  }

  private static List<RowData> suffix() {
    return List.of(
        row(RowKind.DELETE, 1, 99, 999),
        row(RowKind.DELETE, 1, 20, 200),
        row(RowKind.INSERT, 1, 50, 500),
        row(RowKind.DELETE, 1, 50, 500),
        row(RowKind.DELETE, 1, 10, 100),
        row(RowKind.DELETE, 1, 30, 101),
        row(RowKind.DELETE, 2, 40, null));
  }

  private static RowData row(RowKind kind, long key, long input, Integer value) {
    return GenericRowData.ofKind(kind, key, input, value == null ? null : value.longValue());
  }

  private static List<String> logicalCandidates(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h, String strategy)
      throws Exception {
    return CanonicalSinkMaterializerProtocolTest.candidates(h, strategy).stream()
        .map(row -> row.substring(row.indexOf(':')))
        .collect(java.util.stream.Collectors.toList());
  }

  static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness(
      String strategy, boolean inputKey, boolean rocks) throws Exception {
    return nativeHarness(strategy, inputKey, rocks, 0);
  }

  static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness(
      String strategy, boolean inputKey, boolean rocks, long ttl) throws Exception {
    return nativeHarness(
        strategy,
        inputKey,
        rocks,
        ttl,
        CanonicalSinkMaterializerProtocolTest.TYPE,
        inputKey ? new int[] {-1} : new int[] {-1, -1, -1});
  }

  static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness(
      String strategy,
      boolean inputKey,
      boolean rocks,
      long ttl,
      org.apache.flink.table.types.logical.RowType input,
      int[] matchPrecisions)
      throws Exception {
    var operator = new Materializer(strategy, inputKey, rocks, ttl, matchPrecisions);
    var contract =
        new StockSinkMaterializerStateContract(
            new RowDataSerializer(CanonicalSinkMaterializerProtocolTest.KEY),
            CanonicalSinkMaterializerProtocolTest.KEY,
            input,
            inputKey ? new int[] {1} : null,
            strategy.startsWith("ADAPTIVE") ? Strategy.ADAPTIVE : Strategy.valueOf(strategy),
            ttl);
    operator.setStockStateContract(contract);
    int stateKey = FlinkKeyGroupUtils.stateKeysForSubtasks(128, 1)[0];
    var h =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            operator, ignored -> stateKey, Types.INT, 128, 1, 0);
    var config = new Configuration();
    if (rocks)
      config.setString("state.backend.type", RocksDBNativeStateBackendFactory.class.getName());
    CanonicalStateBackend.install(
        StreamExecutionEnvironment.getExecutionEnvironment(),
        config,
        h.getStreamConfig().getOperatorID().toHexString(),
        operator.getClass().getSimpleName(),
        contract,
        CanonicalSinkMaterializerInteropTest.class.getClassLoader());
    h.setStateBackend(
        new CanonicalStateBackendFactory()
            .createFromConfig(config, CanonicalSinkMaterializerInteropTest.class.getClassLoader()));
    h.setup(new ArrowBatchSerializer());
    return h;
  }

  private static final class Materializer extends NativeSinkMaterializer {
    private final int mode;
    private final int[] matchPrecisions;
    private final long high;

    Materializer(
        String strategy, boolean inputKey, boolean rocks, long ttl, int[] matchPrecisions) {
      super(new int[] {0}, new int[] {-1}, inputKey ? new int[] {1} : null, ttl, 128);
      this.matchPrecisions = matchPrecisions.clone();
      mode =
          strategy.equals("LEGACY")
              ? 0
              : strategy.equals("VALUE")
                  ? 1
                  : strategy.equals("MAP") ? (rocks ? 4 : 2) : (rocks ? 5 : 3);
      high = strategy.equals("ADAPTIVE_SMALL") ? 100 : 3;
    }

    @Override
    protected int strategyMode() {
      return mode;
    }

    @Override
    protected int[] matchTimestampPrecisions() {
      return matchPrecisions;
    }

    @Override
    protected long adaptiveHighThreshold() {
      return high;
    }

    @Override
    protected long adaptiveLowThreshold() {
      return 1;
    }
  }

  private static void push(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h, RowData row)
      throws Exception {
    h.processElement(
        new StreamRecord<>(
            new ArrowBatch(
                RowDataArrowConverter.write(
                    List.of(row),
                    CanonicalSinkMaterializerProtocolTest.TYPE,
                    NativeAllocator.SHARED,
                    true))));
  }

  private static List<String> output(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h) {
    var values = new ArrayList<String>();
    while (!h.getOutput().isEmpty()) {
      Object event = h.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (var row :
              RowDataArrowConverter.read(root, CanonicalSinkMaterializerProtocolTest.TYPE))
            values.add(
                row.getRowKind().shortString()
                    + ":"
                    + row.getLong(0)
                    + ":"
                    + row.getLong(1)
                    + ":"
                    + (row.isNullAt(2) ? "null" : row.getLong(2)));
        }
      }
    }
    return values;
  }
}

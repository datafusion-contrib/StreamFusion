package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.ttl.TtlStateFactory.TtlSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.AppendOnlyTopNFunction;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.operators.rank.VariableRankRange;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.StockAppendTopNStateContract;

class CanonicalAppendRankTtlInteropTest {
  static final RowType INPUT = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
  static final RowType OUTPUT =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType());
  static final RowType KEY = RowType.of(new BigIntType());

  @org.junit.jupiter.api.Test
  void projectedRankRetainsTheReleasedHiddenOverflowGroup() throws Exception {
    OperatorSubtaskState prefix;
    List<String> expectedOutput;
    OperatorSubtaskState expectedState;
    try (var stock = harness(true, false)) {
      stock.open();
      stock.setStateTtlProcessingTime(1000);
      for (long score : new long[] {10, 20, 30})
        stock.processElement(new StreamRecord<>(GenericRowData.of(1L, score, 3L)));
      stock.setStateTtlProcessingTime(1500);
      stock.processElement(new StreamRecord<>(GenericRowData.of(1L, 15L, 3L)));
      prefix = snapshot(stock);
      stock.getOutput().clear();
      stock.setStateTtlProcessingTime(1600);
      stock.processElement(new StreamRecord<>(GenericRowData.of(1L, 12L, 3L)));
      expectedOutput =
          stock.extractOutputValues().stream()
              .map(CanonicalAppendRankTtlInteropTest::value)
              .collect(java.util.stream.Collectors.toList());
      expectedState = snapshot(stock);
    }
    java.util.Map<Long, Long> expectedAges;
    try (var view = harness(false, false)) {
      view.initializeState(expectedState);
      view.open();
      expectedAges = ages(backend(view));
    }
    assertEquals(java.util.Map.of(10L, 1000L, 12L, 1600L, 15L, 1500L, 20L, 1000L), expectedAges);
    long handle =
        Native.createTopNRanker(
            new int[] {0},
            new int[] {Native.STOCK_KEY_PROJECTION, -1},
            new int[] {1},
            new int[] {1},
            new int[] {1},
            0,
            3,
            -1,
            true,
            false,
            true,
            false,
            1000,
            1 << 24);
    try (var view = harness(false, false)) {
      view.initializeState(prefix);
      view.open();
      var contract =
          new StockAppendTopNStateContract(
              new RowDataSerializer(KEY),
              selector(),
              sortSelector(),
              KEY,
              INPUT,
              new int[] {1},
              false,
              false,
              true);
      contract.importState(backend(view), handle, false);
      assertEquals(expectedOutput, push(handle, List.of(GenericRowData.of(1L, 12L, 3L)), 1600));
      contract.exportState(backend(view), handle, false);
      assertEquals(expectedAges, ages(backend(view)));
    } finally {
      Native.closeTopNRanker(handle);
    }
  }

  private static java.util.Map<Long, Long> ages(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    backend.setCurrentKey(selector().getKey(GenericRowData.of(1L, 0L, 0L)));
    var state =
        backend.getPartitionedState(
            VoidNamespace.INSTANCE,
            VoidNamespaceSerializer.INSTANCE,
            new MapStateDescriptor<>(
                "data-state-with-append",
                new RowDataSerializer(KEY),
                new TtlSerializer<>(
                    LongSerializer.INSTANCE, new ListSerializer<>(new RowDataSerializer(INPUT)))));
    var ages = new java.util.TreeMap<Long, Long>();
    for (var entry : state.entries())
      ages.put(entry.getKey().getLong(0), entry.getValue().getLastAccessTimestamp());
    return ages;
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void listAgesAndIndependentBoundOnlyStateSurviveLateConversion(boolean variable)
      throws Exception {
    OperatorSubtaskState prefix;
    try (var stock = harness(true, variable)) {
      stock.open();
      stock.setStateTtlProcessingTime(1000);
      stock.processElement(new StreamRecord<>(GenericRowData.of(1L, 10L, 3L)));
      if (variable) stock.processElement(new StreamRecord<>(GenericRowData.of(2L, 10L, -1L)));
      stock.setStateTtlProcessingTime(1500);
      stock.processElement(new StreamRecord<>(GenericRowData.of(1L, 20L, 3L)));
      prefix = snapshot(stock);
    }
    long handle =
        Native.createTopNRanker(
            new int[] {0},
            new int[] {Native.STOCK_KEY_PROJECTION, -1},
            new int[] {1},
            new int[] {1},
            new int[] {1},
            0,
            3,
            variable ? 2 : -1,
            true,
            false,
            true,
            false,
            1000,
            1 << 24);
    if (variable) Native.enableTopNFirstBound(handle, false, true);
    try {
      OperatorSubtaskState exported;
      try (var view = harness(false, variable)) {
        view.initializeState(prefix);
        view.open();
        view.setStateTtlProcessingTime(10_000);
        var backend = backend(view);
        var contract =
            new StockAppendTopNStateContract(
                new RowDataSerializer(KEY),
                selector(),
                sortSelector(),
                KEY,
                INPUT,
                new int[] {1},
                false,
                variable,
                true);
        contract.importState(backend, handle, false);
        contract.exportState(backend, handle, false);
        backend.setCurrentKey(selector().getKey(GenericRowData.of(1L, 0L, 0L)));
        var state =
            backend.getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new MapStateDescriptor<>(
                    "data-state-with-append",
                    new RowDataSerializer(KEY),
                    new TtlSerializer<>(
                        LongSerializer.INSTANCE,
                        new ListSerializer<>(new RowDataSerializer(INPUT)))));
        for (var entry : state.entries())
          assertEquals(
              entry.getKey().getLong(0) == 10 ? 1000 : 1500,
              entry.getValue().getLastAccessTimestamp());
        exported = snapshot(view);
      }
      try (var stock = harness(true, variable)) {
        stock.initializeState(exported);
        stock.open();
        stock.setStateTtlProcessingTime(2200);
        var additions = new ArrayList<RowData>();
        additions.add(GenericRowData.of(1L, 15L, 2L));
        if (variable) additions.add(GenericRowData.of(2L, 12L, 2L));
        for (var row : additions) stock.processElement(new StreamRecord<>(row));
        var expected =
            stock.extractOutputValues().stream()
                .map(CanonicalAppendRankTtlInteropTest::value)
                .collect(java.util.stream.Collectors.toList());
        assertEquals(expected, push(handle, additions, 2200));
        assertTrue(expected.stream().anyMatch(row -> row.contains(":15:")), expected.toString());
        if (variable)
          assertTrue(
              expected.stream().anyMatch(row -> row.startsWith("0:2:12:")), expected.toString());
      }
    } finally {
      Native.closeTopNRanker(handle);
    }
  }

  private static List<String> push(long handle, List<RowData> rows, long time) {
    try (VectorSchemaRoot root =
            RowDataArrowConverter.write(rows, INPUT, NativeAllocator.SHARED, true);
        ArrowArray in = ArrowArray.allocateNew(NativeAllocator.SHARED);
        ArrowSchema inSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        ArrowArray out = ArrowArray.allocateNew(NativeAllocator.SHARED);
        ArrowSchema outSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, in, inSchema);
      Native.pushTopNRanker(
          handle,
          in.memoryAddress(),
          inSchema.memoryAddress(),
          time,
          out.memoryAddress(),
          outSchema.memoryAddress());
      try (VectorSchemaRoot result =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, out, outSchema, NativeAllocator.DICTIONARIES)) {
        var reader = ArrowConversion.createArrowReader(result, OUTPUT);
        var kinds =
            (org.apache.arrow.vector.TinyIntVector)
                result.getVector(result.getFieldVectors().size() - 1);
        var values = new ArrayList<String>();
        for (int index = 0; index < result.getRowCount(); index++) {
          var row = reader.read(index);
          row.setRowKind(org.apache.flink.types.RowKind.fromByteValue(kinds.get(index)));
          values.add(value(row));
        }
        return values;
      }
    }
  }

  private static String value(RowData row) {
    return row.getRowKind().toByteValue()
        + ":"
        + row.getLong(0)
        + ":"
        + row.getLong(1)
        + ":"
        + row.getLong(2)
        + ":"
        + row.getLong(3);
  }

  static RowDataKeySelector sortSelector() {
    return KeySelectorUtil.getRowDataSelector(
        CanonicalAppendRankTtlInteropTest.class.getClassLoader(),
        new int[] {1},
        InternalTypeInfo.of(INPUT));
  }

  static RowDataKeySelector selector() {
    return KeySelectorUtil.getRowDataSelector(
        CanonicalAppendRankTtlInteropTest.class.getClassLoader(),
        new int[] {0},
        InternalTypeInfo.of(INPUT));
  }

  static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      boolean stock, boolean variable) throws Exception {
    OneInputStreamOperator<RowData, RowData> operator;
    if (stock) {
      var comparator =
          new GeneratedRecordComparator("", "", new Object[0]) {
            @Override
            public RecordComparator newInstance(ClassLoader loader) {
              return (left, right) -> Long.compare(left.getLong(0), right.getLong(0));
            }
          };
      var function =
          new AppendOnlyTopNFunction(
              StateConfigUtil.createTtlConfig(1000),
              InternalTypeInfo.of(INPUT),
              comparator,
              KeySelectorUtil.getRowDataSelector(
                  CanonicalAppendRankTtlInteropTest.class.getClassLoader(),
                  new int[] {1},
                  InternalTypeInfo.of(INPUT)),
              RankType.ROW_NUMBER,
              variable ? new VariableRankRange(2) : new ConstantRankRange(1, 3),
              true,
              true,
              10000);
      var process = new KeyedProcessOperator<RowData, RowData, RowData>(function);
      function.setKeyContext(process);
      operator = process;
    } else operator = new ProjectionOperator();
    var selector = selector();
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            operator, selector, selector.getProducedType(), 128, 1, 0);
    harness.setStateBackend(new HashMapStateBackend());
    harness.setup(new RowDataSerializer(OUTPUT));
    return harness;
  }

  @SuppressWarnings("unchecked")
  static CheckpointableKeyedStateBackend<RowData> backend(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
    return (CheckpointableKeyedStateBackend<RowData>)
        harness.getOperator().<RowData>getKeyedStateBackend();
  }

  private static OperatorSubtaskState snapshot(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) throws Exception {
    return harness
        .snapshotWithLocalState(1, 1500, SavepointType.savepoint(SavepointFormatType.CANONICAL))
        .getJobManagerOwnedState();
  }

  private static final class ProjectionOperator extends AbstractStreamOperator<RowData>
      implements OneInputStreamOperator<RowData, RowData> {
    @Override
    public void processElement(StreamRecord<RowData> record) {}
  }
}

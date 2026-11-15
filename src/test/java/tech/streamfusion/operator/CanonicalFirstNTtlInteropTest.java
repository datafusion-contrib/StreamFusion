package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
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
import org.apache.flink.table.runtime.operators.rank.AppendOnlyFirstNFunction;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.FlinkRocksDBOptions;
import tech.streamfusion.state.StockFirstNStateContract;

class CanonicalFirstNTtlInteropTest {
  private static final RowType INPUT = RowType.of(new BigIntType(), new BigIntType());
  private static final RowType OUTPUT =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
  private static final RowType KEY = RowType.of(new BigIntType());

  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rejectedRowsDoNotRefreshCountersAndConversionDoesNotExpireThem(boolean persistent)
      throws Exception {
    OperatorSubtaskState original;
    try (var stock = harness(true)) {
      stock.open();
      stock.setStateTtlProcessingTime(1000);
      stock.processElement(new StreamRecord<>(GenericRowData.of(1L, 10L)));
      stock.processElement(new StreamRecord<>(GenericRowData.of(2L, 20L)));
      stock.setStateTtlProcessingTime(1500);
      stock.processElement(new StreamRecord<>(GenericRowData.of(1L, 15L)));
      stock.setStateTtlProcessingTime(1800);
      stock.processElement(new StreamRecord<>(GenericRowData.of(1L, 18L)));
      original = snapshot(stock);
    }
    int[] precision = {Native.STOCK_KEY_PROJECTION, -1};
    long handle =
        persistent
            ? Native.createRocksDBFirstN(
                new int[] {0},
                precision,
                2,
                true,
                1000,
                10000,
                1 << 24,
                directory.resolve("state").toString(),
                128,
                FlinkRocksDBOptions.from(new Configuration()).json(),
                0,
                new String[0],
                new String[0],
                0,
                127,
                false,
                new byte[0][])
            : Native.createFirstN(
                new int[] {0}, precision, 2, true, 1000, 10000, new byte[0][], 1 << 24);
    try {
      OperatorSubtaskState converted;
      try (var view = harness(false)) {
        view.initializeState(original);
        view.open();
        view.setStateTtlProcessingTime(10000);
        var backend = backend(view);
        var contract = new StockFirstNStateContract(new RowDataSerializer(KEY), KEY, true);
        contract.importState(backend, handle, persistent);
        contract.exportState(backend, handle, persistent);
        var state =
            backend.getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new ValueStateDescriptor<>(
                    "counterState",
                    new TtlSerializer<>(LongSerializer.INSTANCE, IntSerializer.INSTANCE)));
        backend.setCurrentKey(selector().getKey(GenericRowData.of(1L, 0L)));
        assertEquals(2, state.value().getUserValue());
        assertEquals(1500, state.value().getLastAccessTimestamp());
        backend.setCurrentKey(selector().getKey(GenericRowData.of(2L, 0L)));
        assertEquals(1, state.value().getUserValue());
        assertEquals(1000, state.value().getLastAccessTimestamp());
        converted = snapshot(view);
      }
      try (var stock = harness(true)) {
        stock.initializeState(converted);
        stock.open();
        for (long time : new long[] {2200, 2500}) {
          stock.setStateTtlProcessingTime(time);
          stock.getOutput().clear();
          var rows = List.<RowData>of(GenericRowData.of(1L, time), GenericRowData.of(2L, time));
          for (var row : rows) stock.processElement(new StreamRecord<>(row));
          var expected =
              stock.extractOutputValues().stream()
                  .map(CanonicalFirstNTtlInteropTest::value)
                  .collect(java.util.stream.Collectors.toList());
          assertEquals(expected, push(handle, rows, time));
          assertEquals(
              time == 2200 ? List.of("2:2200:1") : List.of("1:2500:1", "2:2500:2"), expected);
        }
      }
    } finally {
      Native.closeFirstN(handle);
    }
  }

  private static List<String> push(long handle, List<RowData> rows, long time) {
    try (VectorSchemaRoot root = RowDataArrowConverter.write(rows, INPUT, NativeAllocator.SHARED);
        ArrowArray in = ArrowArray.allocateNew(NativeAllocator.SHARED);
        ArrowSchema inSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        ArrowArray out = ArrowArray.allocateNew(NativeAllocator.SHARED);
        ArrowSchema outSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, in, inSchema);
      Native.pushFirstN(
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
        var values = new java.util.ArrayList<String>();
        for (int index = 0; index < result.getRowCount(); index++)
          values.add(value(reader.read(index)));
        return values;
      }
    }
  }

  private static String value(RowData row) {
    return row.getLong(0) + ":" + row.getLong(1) + ":" + row.getLong(2);
  }

  private static RowDataKeySelector selector() {
    return KeySelectorUtil.getRowDataSelector(
        CanonicalFirstNTtlInteropTest.class.getClassLoader(),
        new int[] {0},
        InternalTypeInfo.of(INPUT));
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      boolean stock) throws Exception {
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
          new AppendOnlyFirstNFunction(
              StateConfigUtil.createTtlConfig(1000),
              InternalTypeInfo.of(INPUT),
              comparator,
              KeySelectorUtil.getRowDataSelector(
                  CanonicalFirstNTtlInteropTest.class.getClassLoader(),
                  new int[] {1},
                  InternalTypeInfo.of(INPUT)),
              RankType.ROW_NUMBER,
              new ConstantRankRange(1, 2),
              true,
              true);
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
  private static CheckpointableKeyedStateBackend<RowData> backend(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
    return (CheckpointableKeyedStateBackend<RowData>)
        harness.getOperator().<RowData>getKeyedStateBackend();
  }

  private static OperatorSubtaskState snapshot(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) throws Exception {
    return harness
        .snapshotWithLocalState(1, 1800, SavepointType.savepoint(SavepointFormatType.CANONICAL))
        .getJobManagerOwnedState();
  }

  private static final class ProjectionOperator extends AbstractStreamOperator<RowData>
      implements OneInputStreamOperator<RowData, RowData> {
    @Override
    public void processElement(StreamRecord<RowData> row) {}
  }
}

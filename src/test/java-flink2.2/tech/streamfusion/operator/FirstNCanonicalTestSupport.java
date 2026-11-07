package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.rank.AppendOnlyFirstNFunction;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.FlinkRocksDBOptions;
import tech.streamfusion.state.StockFirstNStateContract;

final class FirstNCanonicalTestSupport {
  static final RowType INPUT = RowType.of(new BigIntType(), new BigIntType());

  static RowType keyType(boolean keyless) {
    return keyless ? RowType.of() : RowType.of(new BigIntType());
  }

  static RowType outputType(boolean rank) {
    return rank ? RowType.of(new BigIntType(), new BigIntType(), new BigIntType()) : INPUT;
  }

  static long create(Path directory, boolean rocks, boolean keyless, boolean rank, long ttl) {
    int[] keys = keyless ? new int[0] : new int[] {0};
    int[] precision =
        keyless
            ? new int[] {Native.STOCK_KEY_PROJECTION}
            : new int[] {Native.STOCK_KEY_PROJECTION, -1};
    if (!rocks)
      return Native.createFirstN(keys, precision, 3, rank, ttl, 0, new byte[0][], 1 << 26);
    return Native.createRocksDBFirstN(
        keys,
        precision,
        3,
        rank,
        ttl,
        0,
        1 << 26,
        directory.resolve("state").toString(),
        keyless ? 1 : 128,
        FlinkRocksDBOptions.from(new Configuration()).json(),
        0,
        new String[0],
        new String[0],
        0,
        keyless ? 0 : 127,
        false,
        new byte[0][]);
  }

  static StockFirstNStateContract contract(boolean keyless, long ttl) {
    return new StockFirstNStateContract(
        new RowDataSerializer(keyType(keyless)), keyType(keyless), ttl > 0);
  }

  static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      boolean stock, boolean keyless, boolean rank, long ttl) throws Exception {
    OneInputStreamOperator<RowData, RowData> operator;
    if (stock) {
      var comparator =
          new GeneratedRecordComparator("", "", new Object[0]) {
            @Override
            public RecordComparator newInstance(ClassLoader loader) {
              return (left, right) -> Long.compare(left.getLong(0), right.getLong(0));
            }
          };
      var sort =
          KeySelectorUtil.getRowDataSelector(
              FirstNCanonicalTestSupport.class.getClassLoader(),
              new int[] {1},
              InternalTypeInfo.of(INPUT));
      var function =
          new AppendOnlyFirstNFunction(
              StateConfigUtil.createTtlConfig(ttl),
              InternalTypeInfo.of(INPUT),
              comparator,
              sort,
              RankType.ROW_NUMBER,
              new ConstantRankRange(1, 3),
              true,
              rank);
      var process = new KeyedProcessOperator<RowData, RowData, RowData>(function);
      function.setKeyContext(process);
      operator = process;
    } else operator = new Projection();
    var selector =
        KeySelectorUtil.getRowDataSelector(
            FirstNCanonicalTestSupport.class.getClassLoader(),
            keyless ? new int[0] : new int[] {0},
            InternalTypeInfo.of(INPUT));
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            operator, selector, selector.getProducedType(), keyless ? 1 : 128, 1, 0);
    harness.setStateBackend(new HashMapStateBackend());
    harness.setup(new RowDataSerializer(outputType(rank)));
    return harness;
  }

  static List<String> stockPush(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness, List<RowData> rows)
      throws Exception {
    harness.getOutput().clear();
    for (RowData row : rows)
      harness.processElement(
          new StreamRecord<>(new RowDataSerializer(INPUT).toBinaryRow(row).copy()));
    var values = new ArrayList<String>();
    for (RowData row : harness.extractOutputValues()) values.add(value(row));
    return values;
  }

  static List<String> nativePush(long handle, List<RowData> rows, boolean rank, long time) {
    try (var root = RowDataArrowConverter.write(rows, INPUT, NativeAllocator.SHARED);
        var input = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var inputSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        var output = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var outputSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, input, inputSchema);
      Native.pushFirstN(
          handle,
          input.memoryAddress(),
          inputSchema.memoryAddress(),
          time,
          output.memoryAddress(),
          outputSchema.memoryAddress());
      try (var result =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, output, outputSchema, NativeAllocator.DICTIONARIES)) {
        var reader = ArrowConversion.createArrowReader(result, outputType(rank));
        var values = new ArrayList<String>();
        for (int i = 0; i < result.getRowCount(); i++) values.add(value(reader.read(i)));
        return values;
      }
    }
  }

  private static String value(RowData row) {
    assertEquals(RowKind.INSERT, row.getRowKind());
    var value = new StringBuilder();
    for (int i = 0; i < row.getArity(); i++) {
      if (i > 0) value.append('|');
      value.append(row.isNullAt(i) ? "null" : row.getLong(i));
    }
    return value.toString();
  }

  @SuppressWarnings("unchecked")
  static CheckpointableKeyedStateBackend<RowData> backend(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
    return (CheckpointableKeyedStateBackend<RowData>)
        harness.getOperator().<RowData>getKeyedStateBackend();
  }

  static OperatorSubtaskState snapshot(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) throws Exception {
    return harness
        .snapshotWithLocalState(1, 1800, SavepointType.savepoint(SavepointFormatType.CANONICAL))
        .getJobManagerOwnedState();
  }

  private static final class Projection extends AbstractStreamOperator<RowData>
      implements OneInputStreamOperator<RowData, RowData> {
    @Override
    public void processElement(StreamRecord<RowData> row) {}
  }
}

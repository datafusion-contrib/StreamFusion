package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.CanonicalAppendRankTypedTest.bytes;
import static tech.streamfusion.operator.CanonicalAppendRankTypedTest.collect;
import static tech.streamfusion.operator.CanonicalAppendRankTypedTest.feed;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.*;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.state.CanonicalStateBackend;
import tech.streamfusion.state.CanonicalStateBackendFactory;
import tech.streamfusion.state.StockUpdatingTopNStateContract;

class CanonicalUpdateRankTypedTest {
  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String shape : List.of("int", "string", "long", "not-null", "collision", "composite"))
      for (boolean fast : new boolean[] {false, true})
        for (boolean rocks : new boolean[] {false, true})
          for (boolean before : new boolean[] {false, true})
            for (boolean rank : new boolean[] {false, true})
              for (boolean twoSorts : new boolean[] {false, true})
                cases.add(Arguments.of(shape, fast, rocks, before, rank, twoSorts));
    return tech.streamfusion.CanonicalTestMatrix.select(cases.stream());
  }

  @ParameterizedTest
  @MethodSource("cases")
  void typedUniqueKeysAndStoredRowsContinueAcrossCanonicalSwitches(
      String shape, boolean fast, boolean rocks, boolean before, boolean rank, boolean twoSorts)
      throws Exception {
    var layout = new Layout(shape, twoSorts);
    RowType output = layout.type;
    if (rank) {
      var fields = new ArrayList<>(output.getFields());
      fields.add(new RowType.RowField("rank", new BigIntType()));
      output = new RowType(fields);
    }
    try (var stock = stock(layout, output, fast, rocks, before, rank);
        var nativeHarness = nativeHarness(layout, fast, rocks, before, rank)) {
      stock.open();
      var prefix =
          List.of(
              layout.row(1, 20, 1),
              layout.row(1, 10, 2),
              layout.row(1, 10, 3),
              layout.row(1, 30, 4),
              layout.row(2, 30, 1),
              layout.row(null, 40, 1));
      feed(stock, layout.type, prefix);
      nativeHarness.initializeState(snapshot(stock));
      nativeHarness.open();
      assertEquals(
          rocks ? "rocksdb" : "memory", ((CanonicalRank) nativeHarness.getOperator()).storage());
      var suffix =
          List.of(
              layout.update(1, 5, 4),
              layout.update(1, 8, 3),
              layout.update(2, 25, 1),
              layout.update(null, 35, 1),
              layout.row(1, 8, 5));
      feed(stock, layout.type, suffix);
      push(nativeHarness, layout.type, suffix);
      var expected = bytes(output, stock.extractOutputValues());
      assertFalse(expected.isEmpty());
      assertEquals(expected, bytes(output, collect(nativeHarness, output)));
      try (var restored = stock(layout, output, fast, rocks, before, rank)) {
        restored.initializeState(snapshot(nativeHarness));
        restored.open();
        var future =
            List.of(
                layout.update(1, 4, 2),
                layout.update(2, shape.equals("not-null") ? 0 : null, 1),
                layout.update(null, 34, 1),
                layout.row(1, 3, 6));
        feed(stock, layout.type, future);
        feed(restored, layout.type, future);
        push(nativeHarness, layout.type, future);
        expected = bytes(output, stock.extractOutputValues());
        assertFalse(expected.isEmpty());
        assertEquals(expected, bytes(output, restored.extractOutputValues()));
        assertEquals(expected, bytes(output, collect(nativeHarness, output)));
      }
    }
  }

  private static final class Layout {
    final String shape;
    final RowType type;
    final int[] keys;
    final int[] rowKeys;
    final int sort;
    final int[] sortColumns;

    Layout(String shape, boolean twoSorts) {
      this.shape = shape;
      if (shape.equals("composite")) {
        type = RowType.of(new IntType(), new VarCharType(100), new IntType(), new VarCharType(100));
        keys = new int[] {3, 2};
        rowKeys = new int[] {3, 1, 2};
        sort = 0;
      } else {
        LogicalType key =
            shape.equals("string")
                ? new VarCharType(100)
                : shape.equals("long") ? new BigIntType() : new IntType();
        LogicalType score =
            shape.equals("long")
                ? new VarCharType(100)
                : shape.equals("string")
                    ? new IntType()
                    : new BigIntType(!shape.equals("not-null"));
        type =
            shape.equals("collision")
                ? RowType.of(
                    new LogicalType[] {key, score, new VarCharType(100)},
                    new String[] {"binary_key", "row_key", "inner_rank"})
                : RowType.of(key, score, new VarCharType(100));
        keys = new int[] {0};
        rowKeys = new int[] {0, 2};
        sort = 1;
      }
      sortColumns = twoSorts ? new int[] {sort, keys[0]} : new int[] {sort};
    }

    RowData row(Integer key, Integer score, int id) {
      var label = StringData.fromString("row-😀-" + id);
      if (shape.equals("composite"))
        return GenericRowData.of(
            score, label, key, key == null ? null : StringData.fromString("分区-" + key));
      Object partition =
          key == null
              ? null
              : shape.equals("string")
                  ? StringData.fromString("分区-" + key)
                  : shape.equals("long") ? (Object) key.longValue() : key;
      Object value =
          score == null
              ? null
              : shape.equals("long")
                  ? StringData.fromString(String.format(java.util.Locale.ROOT, "%03d", score))
                  : shape.equals("string") ? (Object) score : score.longValue();
      return GenericRowData.of(partition, value, label);
    }

    RowData update(Integer key, Integer score, int id) {
      var row = row(key, score, id);
      row.setRowKind(RowKind.UPDATE_AFTER);
      return row;
    }
  }

  private static RowDataKeySelector selector(RowType type, int... columns) {
    return KeySelectorUtil.getRowDataSelector(
        CanonicalUpdateRankTypedTest.class.getClassLoader(), columns, InternalTypeInfo.of(type));
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(
      Layout layout, RowType output, boolean fast, boolean rocks, boolean before, boolean rank)
      throws Exception {
    var selector = selector(layout.type, layout.keys);
    var sort = selector(layout.type, layout.sortColumns);
    var sortSpec = SortSpec.builder();
    for (int field = 0; field < layout.sortColumns.length; field++)
      sortSpec.addField(field, true, false);
    var comparator =
        ComparatorCodeGenerator.gen(
            new Configuration(),
            CanonicalUpdateRankTypedTest.class.getClassLoader(),
            "CanonicalTypedUpdateComparator",
            (RowType) sort.getProducedType().toLogicalType(),
            sortSpec.build());
    AbstractTopNFunction function =
        fast
            ? new FastTop1Function(
                StateConfigUtil.createTtlConfig(0),
                InternalTypeInfo.of(layout.type),
                comparator,
                sort,
                RankType.ROW_NUMBER,
                new ConstantRankRange(1, 1),
                before,
                rank,
                10000)
            : new UpdatableTopNFunction(
                StateConfigUtil.createTtlConfig(0),
                InternalTypeInfo.of(layout.type),
                selector(layout.type, layout.rowKeys),
                comparator,
                sort,
                RankType.ROW_NUMBER,
                new ConstantRankRange(1, 3),
                before,
                rank,
                10000);
    var operator = new KeyedProcessOperator<RowData, RowData, RowData>(function);
    function.setKeyContext(operator);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            operator, selector, selector.getProducedType(), 128, 1, 0);
    harness.setStateBackend(
        rocks
            ? tech.streamfusion.compat.CanonicalTestCompat.rocksBackend()
            : new HashMapStateBackend());
    harness.setup(new RowDataSerializer(output));
    return harness;
  }

  private KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness(
      Layout layout, boolean fast, boolean rocks, boolean before, boolean rank) throws Exception {
    var selector = selector(layout.type, layout.keys);
    var keyType = (RowType) selector.getProducedType().toLogicalType();
    var contract =
        new StockUpdatingTopNStateContract(
            new RowDataSerializer(keyType),
            selector,
            selector(layout.type, layout.rowKeys),
            selector(layout.type, layout.sortColumns),
            keyType,
            layout.type,
            fast);
    var operator = new CanonicalRank(layout, fast, before, rank);
    operator.setStockStateContract(contract);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            operator, batch -> 0, Types.INT, 128, 1, 0);
    var config = new Configuration();
    if (rocks)
      config.setString(
          org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND.key(),
          tech.streamfusion.state.RocksDBNativeStateBackendFactory.class.getName());
    CanonicalStateBackend.install(
        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
            .getExecutionEnvironment(),
        config,
        harness.getStreamConfig().getOperatorID().toHexString(),
        operator.getClass().getSimpleName(),
        contract,
        getClass().getClassLoader());
    harness.setStateBackend(
        new CanonicalStateBackendFactory().createFromConfig(config, getClass().getClassLoader()));
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }

  private static int[] descriptor(int[] columns) {
    var descriptor = new int[columns.length + 1];
    java.util.Arrays.fill(descriptor, -1);
    descriptor[0] = Native.STOCK_KEY_PROJECTION;
    return descriptor;
  }

  private static void push(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      RowType type,
      List<RowData> rows)
      throws Exception {
    harness.processElement(
        new StreamRecord<>(
            new ArrowBatch(RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED, true))));
    harness.prepareSnapshotPreBarrier(0);
  }

  private static OperatorSubtaskState snapshot(
      org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness<?, ?> harness)
      throws Exception {
    harness.prepareSnapshotPreBarrier(2);
    return harness
        .snapshotWithLocalState(
            2,
            0,
            org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
        .getJobManagerOwnedState();
  }

  private static int[] ascending(int length) {
    var order = new int[length];
    java.util.Arrays.fill(order, 1);
    return order;
  }

  private static final class CanonicalRank extends NativeColumnarTopNOperator {
    private boolean canonical;

    CanonicalRank(Layout layout, boolean fast, boolean before, boolean rank) {
      super(
          layout.keys,
          descriptor(layout.keys),
          layout.type,
          layout.sortColumns,
          ascending(layout.sortColumns.length),
          ascending(layout.sortColumns.length),
          0,
          fast ? 1 : 3,
          rank,
          false,
          layout.rowKeys,
          descriptor(layout.rowKeys),
          before,
          false,
          -1,
          0,
          128);
    }

    String storage() {
      return rocksdbState() ? directRocksDBState() ? "rocksdb" : "rocksdb-snapshot" : "memory";
    }

    @Override
    protected org.apache.flink.streaming.api.operators.OperatorSnapshotFutures
        snapshotCanonicalState(
            long id,
            long time,
            org.apache.flink.runtime.checkpoint.CheckpointOptions options,
            org.apache.flink.runtime.state.CheckpointStreamFactory streams)
            throws Exception {
      canonical = true;
      try {
        return super.snapshotCanonicalState(id, time, options, streams);
      } finally {
        canonical = false;
      }
    }

    @Override
    protected byte[][] snapshotRawPartitions() {
      if (canonical)
        throw new AssertionError("Canonical update export serialized raw native state");
      return super.snapshotRawPartitions();
    }

    @Override
    protected byte[][] snapshotCanonicalPartitions() {
      if (canonical)
        throw new AssertionError("Canonical update export serialized a native snapshot");
      return super.snapshotCanonicalPartitions();
    }

    @Override
    protected String[] checkpointRocksDBHandle(String directory) {
      if (canonical && !directory.isEmpty())
        throw new AssertionError("Canonical update export created a native checkpoint");
      return super.checkpointRocksDBHandle(directory);
    }
  }
}

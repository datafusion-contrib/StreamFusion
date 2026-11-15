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
import tech.streamfusion.state.CanonicalStateBackend;
import tech.streamfusion.state.CanonicalStateBackendFactory;
import tech.streamfusion.state.StockUpdatingTopNStateContract;

class CanonicalUpdateRankTypedTest {
  private final List<OperatorSubtaskState> snapshots = new ArrayList<>();

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void independentUpstreamInterleavingsKeepTheSameReleasedRankTable(boolean rocks)
      throws Exception {
    var layout = new Layout("bound-int", false);
    var fields = new ArrayList<>(layout.type.getFields());
    fields.add(new RowType.RowField("rank", new BigIntType()));
    var output = new RowType(fields);
    var changes =
        List.of(
            layout.row(1, 20, 1), layout.row(1, 10, 2),
            layout.update(1, 5, 1), layout.update(1, 0, 2));
    for (var row : changes) ((GenericRowData) row).setField(3, 1);
    var changelogs = new ArrayList<List<String>>();
    var tables = new ArrayList<java.util.Map<String, Long>>();
    for (var order : new int[][] {{0, 1, 2, 3}, {0, 2, 1, 3}}) {
      try (var stock = stock(layout, output, false, rocks, true, true, 0)) {
        stock.open();
        var ordered = new ArrayList<RowData>();
        for (int index : order) ordered.add(changes.get(index));
        feed(stock, layout.type, ordered);
        var rows = stock.extractOutputValues();
        changelogs.add(bytes(output, rows));
        var table = new java.util.TreeMap<String, Long>();
        var serializer = new RowDataSerializer(output);
        for (var row : rows) {
          long sign =
              row.getRowKind() == RowKind.INSERT || row.getRowKind() == RowKind.UPDATE_AFTER
                  ? 1
                  : -1;
          var value = serializer.copy(row);
          value.setRowKind(RowKind.INSERT);
          table.merge(bytes(output, List.of(value)).get(0), sign, Long::sum);
        }
        table.values().removeIf(count -> count == 0);
        tables.add(table);
      }
    }
    assertNotEquals(changelogs.get(0), changelogs.get(1));
    var winner = GenericRowData.of(1, 0L, StringData.fromString("row-😀-2"), 1, 1L);
    var expected = java.util.Map.of(bytes(output, List.of(winner)).get(0), 1L);
    assertEquals(expected, tables.get(0));
    assertEquals(expected, tables.get(1));
  }

  @org.junit.jupiter.api.AfterEach
  void discardSnapshots() throws Exception {
    for (var state : snapshots) state.discardState();
  }

  static Stream<Arguments> offsetCases() {
    var cases = new ArrayList<Arguments>();
    for (String shape :
        new String[] {"int", "composite", "three-sort", "payload-row", "row-key-padded"})
      for (int offset : new int[] {1, 2})
        for (boolean rocks : new boolean[] {false, true})
          for (boolean before : new boolean[] {false, true})
            for (boolean rank : new boolean[] {false, true})
              cases.add(Arguments.of(shape, offset, rocks, before, rank));
    return tech.streamfusion.CanonicalTestMatrix.select(cases.stream());
  }

  @ParameterizedTest
  @MethodSource("offsetCases")
  void hiddenPrefixAndVisibleRowsSurviveCanonicalAndNativeRecovery(
      String shape, int offset, boolean rocks, boolean before, boolean rank) throws Exception {
    verify(shape, false, rocks, before, rank, false, offset);
  }

  static Stream<Arguments> variableCases() {
    var cases = new ArrayList<Arguments>();
    for (String shape : new String[] {"bound-int", "bound-long"})
      for (boolean rocks : new boolean[] {false, true})
        for (boolean before : new boolean[] {false, true})
          for (boolean rank : new boolean[] {false, true})
            cases.add(Arguments.of(shape, rocks, before, rank));
    return tech.streamfusion.CanonicalTestMatrix.select(cases.stream());
  }

  @ParameterizedTest
  @MethodSource("variableCases")
  void independentFirstBoundsSurviveEmptyPartitionsAndNativeRecovery(
      String shape, boolean rocks, boolean before, boolean rank) throws Exception {
    verify(shape, false, rocks, before, rank, false, 0);
  }

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String shape :
        Stream.concat(
                Stream.of(
                    "int",
                    "string",
                    "long",
                    "not-null",
                    "collision",
                    "composite",
                    "three-sort",
                    "row-key",
                    "row-key-nested",
                    "row-key-padded"),
                Stream.concat(
                    CanonicalRankScalarFixture.shapes(), CanonicalRankPayloadFixture.shapes(false)))
            .collect(java.util.stream.Collectors.toList()))
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
    verify(shape, fast, rocks, before, rank, twoSorts, 0);
  }

  private void verify(
      String shape,
      boolean fast,
      boolean rocks,
      boolean before,
      boolean rank,
      boolean twoSorts,
      int offset)
      throws Exception {
    var layout = new Layout(shape, twoSorts);
    RowType output = layout.type;
    if (rank) {
      var fields = new ArrayList<>(output.getFields());
      fields.add(new RowType.RowField("rank", new BigIntType()));
      output = new RowType(fields);
    }
    if (shape.equals("scalar-float") || shape.equals("scalar-double")) {
      assertThrows(IllegalArgumentException.class, () -> contract(layout, fast));
      return;
    }
    try (var stock = stock(layout, output, fast, rocks, before, rank, offset);
        var nativeHarness = nativeHarness(layout, fast, rocks, before, rank, offset)) {
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
      if (shape.startsWith("row-key")
          || shape.startsWith("scalar-")
          || shape.startsWith("payload-"))
        try (var returned = stock(layout, output, fast, rocks, before, rank, offset)) {
          returned.initializeState(snapshot(nativeHarness));
          returned.open();
          assertEquals(
              CanonicalUnboundedOverScalarTest.raw(stock),
              CanonicalUnboundedOverScalarTest.raw(returned));
        }
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
      OperatorSubtaskState returned;
      if (offset == 0 && !layout.variable) returned = snapshot(nativeHarness);
      else {
        var checkpoint = nativeHarness.snapshot(3, 0);
        snapshots.add(checkpoint);
        try (var recovered = nativeHarness(layout, fast, rocks, before, rank, offset)) {
          recovered.initializeState(checkpoint);
          recovered.open();
          returned = snapshot(recovered);
        }
      }
      try (var restored = stock(layout, output, fast, rocks, before, rank, offset)) {
        restored.initializeState(returned);
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
    final boolean variable;

    Layout(String shape, boolean twoSorts) {
      this.shape = shape;
      variable = shape.startsWith("bound-");
      if (shape.startsWith("row-key")) {
        type =
            RowType.of(
                CanonicalRankRowKeyFixture.type(shape), new BigIntType(), new VarCharType(100));
        keys = new int[] {0};
        rowKeys = new int[] {0, 2};
        sort = 1;
      } else if (variable) {
        type =
            RowType.of(
                new IntType(),
                new BigIntType(),
                new VarCharType(100),
                shape.equals("bound-int") ? new IntType(false) : new BigIntType(false));
        keys = new int[] {0};
        rowKeys = new int[] {0, 2};
        sort = 1;
      } else if (shape.startsWith("payload-")) {
        type =
            RowType.of(
                new BigIntType(),
                new BigIntType(),
                new VarCharType(100),
                CanonicalRankPayloadFixture.type(shape));
        keys = new int[] {0};
        rowKeys = new int[] {0, 2};
        sort = 1;
      } else if (shape.startsWith("scalar-")) {
        type =
            RowType.of(
                CanonicalRankScalarFixture.type(shape),
                CanonicalRankScalarFixture.type(shape),
                new VarCharType(100));
        keys = new int[] {0};
        rowKeys = new int[] {0, 2};
        sort = 1;
      } else if (shape.equals("composite")) {
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
      sortColumns =
          shape.equals("three-sort")
              ? new int[] {sort, 2, keys[0]}
              : twoSorts
                  ? new int[] {sort, shape.startsWith("row-key") ? 2 : keys[0]}
                  : new int[] {sort};
    }

    RowData row(Integer key, Integer score, int id) {
      var label = StringData.fromString("row-😀-" + id);
      if (shape.startsWith("row-key"))
        return GenericRowData.of(
            CanonicalRankRowKeyFixture.key(shape, key, id),
            score == null ? null : score.longValue(),
            label);
      if (shape.startsWith("payload-"))
        return GenericRowData.of(
            key == null ? null : key.longValue(),
            score == null ? null : score.longValue(),
            label,
            CanonicalRankPayloadFixture.value(shape, id));
      if (shape.startsWith("scalar-"))
        return GenericRowData.of(
            CanonicalRankScalarFixture.value(shape, key),
            CanonicalRankScalarFixture.score(shape, score),
            label);
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
      if (variable) {
        long bound = key == null ? 1 : key == 2 ? -2 : 3;
        Object proposed = shape.equals("bound-int") ? (Object) (int) bound : bound;
        return GenericRowData.of(partition, value, label, proposed);
      }
      return GenericRowData.of(partition, value, label);
    }

    RowData update(Integer key, Integer score, int id) {
      var row = row(key, score, id);
      if (variable)
        ((GenericRowData) row).setField(3, shape.equals("bound-int") ? (Object) 99 : 99L);
      row.setRowKind(RowKind.UPDATE_AFTER);
      return row;
    }
  }

  private static RowDataKeySelector selector(RowType type, int... columns) {
    return KeySelectorUtil.getRowDataSelector(
        CanonicalUpdateRankTypedTest.class.getClassLoader(), columns, InternalTypeInfo.of(type));
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(
      Layout layout,
      RowType output,
      boolean fast,
      boolean rocks,
      boolean before,
      boolean rank,
      int offset)
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
                layout.variable ? new VariableRankRange(3) : new ConstantRankRange(offset + 1, 3),
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

  private static StockUpdatingTopNStateContract contract(Layout layout, boolean fast) {
    var selector = selector(layout.type, layout.keys);
    var keyType = (RowType) selector.getProducedType().toLogicalType();
    return new StockUpdatingTopNStateContract(
        new RowDataSerializer(keyType),
        selector,
        selector(layout.type, layout.rowKeys),
        selector(layout.type, layout.sortColumns),
        keyType,
        layout.type,
        fast,
        layout.variable,
        layout.keys);
  }

  private KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness(
      Layout layout, boolean fast, boolean rocks, boolean before, boolean rank, int offset)
      throws Exception {
    var contract = contract(layout, fast);
    var operator = new CanonicalRank(layout, fast, before, rank, offset);
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

  private static void push(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      RowType type,
      List<RowData> rows)
      throws Exception {
    harness.processElement(
        new StreamRecord<>(
            new ArrowBatch(
                CanonicalRankRowKeyFixture.required(type)
                    ? CanonicalRowKeyFixture.arrow(rows, type, new int[] {0})
                    : RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED, true))));
    harness.prepareSnapshotPreBarrier(0);
  }

  private OperatorSubtaskState snapshot(
      org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness<?, ?> harness)
      throws Exception {
    harness.prepareSnapshotPreBarrier(2);
    var state =
        harness
            .snapshotWithLocalState(
                2,
                0,
                org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                    org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
            .getJobManagerOwnedState();
    snapshots.add(state);
    return state;
  }

  private static int[] ascending(int length) {
    var order = new int[length];
    java.util.Arrays.fill(order, 1);
    return order;
  }

  private static final class CanonicalRank extends NativeColumnarTopNOperator {
    private boolean canonical;

    CanonicalRank(Layout layout, boolean fast, boolean before, boolean rank, int offset) {
      super(
          layout.keys,
          tech.streamfusion.planner.FlinkKeyGroupUtils.stockKeyDescriptor(layout.type, layout.keys),
          layout.type,
          layout.sortColumns,
          ascending(layout.sortColumns.length),
          ascending(layout.sortColumns.length),
          offset,
          fast ? 1 : 3,
          rank,
          false,
          layout.rowKeys,
          tech.streamfusion.planner.FlinkKeyGroupUtils.stockKeyDescriptor(
              layout.type, layout.rowKeys),
          before,
          false,
          -1,
          0,
          128,
          layout.variable ? 3 : -1,
          layout.variable);
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

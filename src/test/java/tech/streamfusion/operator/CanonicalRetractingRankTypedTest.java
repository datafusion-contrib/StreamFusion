package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.CanonicalAppendRankTypedTest.*;

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
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.state.CanonicalStateBackend;
import tech.streamfusion.state.CanonicalStateBackendFactory;
import tech.streamfusion.state.StockRetractingTopNStateContract;

@ExtendWith(CoalescingOff.class)
class CanonicalRetractingRankTypedTest {
  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String shape : List.of("int", "string", "long", "not-null", "collision", "composite"))
      for (String mode :
          List.of("fixed", "offset", "variable", "fixed-ttl", "offset-ttl", "variable-ttl"))
        for (boolean rocks : new boolean[] {false, true})
          for (boolean before : new boolean[] {false, true})
            for (boolean rank : new boolean[] {false, true})
              for (boolean twoSorts : new boolean[] {false, true})
                cases.add(Arguments.of(shape, mode, rocks, before, rank, twoSorts));
    return cases.stream();
  }

  @ParameterizedTest(name = "{0}/{1} rocks={2} before={3} projected={4} two-sorts={5}")
  @MethodSource("cases")
  void typedListsCountersAndBoundsSurviveBothSwitchOrders(
      String shape, String mode, boolean rocks, boolean before, boolean rank, boolean twoSorts)
      throws Exception {
    var layout = new Layout(shape, twoSorts);
    boolean variable = mode.startsWith("variable");
    long ttl = mode.endsWith("ttl") ? 1000 : 0;
    long start = mode.startsWith("offset") ? 2 : 1;
    boolean nativeFirst =
        (shape.hashCode()
                    + mode.hashCode()
                    + (rocks ? 1 : 0)
                    + (before ? 1 : 0)
                    + (rank ? 1 : 0)
                    + (twoSorts ? 1 : 0))
                % 2
            == 0;
    RowType output = layout.type;
    if (rank) {
      var fields = new ArrayList<>(output.getFields());
      fields.add(new RowType.RowField("rank", new BigIntType()));
      output = new RowType(fields);
    }
    var prefix =
        List.of(
            layout.row(1, 20, 1, 3),
            layout.row(1, 10, 2, 3),
            layout.row(1, 10, 3, 3),
            layout.row(1, 30, 4, 3),
            layout.row(2, 30, 1, 3),
            layout.row(null, 40, 1, 3),
            layout.row(1, shape.equals("not-null") ? 0 : null, 5, 3),
            layout.row(1, 50, 6, 3));
    var suffix =
        List.of(
            layout.change(1, 10, 2, 3, RowKind.DELETE),
            layout.row(1, 5, 7, 3),
            layout.change(2, 30, 1, 3, RowKind.UPDATE_BEFORE),
            layout.row(2, 10, 2, 4),
            layout.change(1, 50, 6, 3, RowKind.DELETE),
            layout.row(null, 35, 2, 3),
            layout.change(1, 10, 999, 3, RowKind.DELETE));
    var future =
        List.of(
            layout.change(1, 10, 3, 3, RowKind.DELETE),
            layout.row(1, 8, 8, 2),
            layout.row(2, shape.equals("not-null") ? 0 : null, 3, 3),
            layout.change(null, 35, 2, 3, RowKind.DELETE),
            layout.row(3, 7, 1, 3));
    try (var reference = stock(layout, output, start, variable, ttl, rocks, before, rank)) {
      reference.open();
      time(reference, 1000);
      feed(reference, layout.type, prefix);
      OperatorSubtaskState saved;
      try (var first = stock(layout, output, start, variable, ttl, rocks, before, rank);
          var nativeHarness = nativeHarness(layout, start, variable, ttl, rocks, before, rank)) {
        if (nativeFirst) {
          nativeHarness.open();
          push(nativeHarness, layout.type, prefix, 1000);
          assertEquals(
              bytes(output, reference.extractOutputValues()),
              bytes(output, collect(nativeHarness, output)));
          saved = snapshot(nativeHarness);
        } else {
          first.open();
          time(first, 1000);
          feed(first, layout.type, prefix);
          saved = snapshot(first);
        }
      }
      try (var second = stock(layout, output, start, variable, ttl, rocks, before, rank);
          var nativeHarness = nativeHarness(layout, start, variable, ttl, rocks, before, rank)) {
        if (nativeFirst) {
          second.initializeState(saved);
          second.open();
          time(second, 1500);
          feed(second, layout.type, suffix);
        } else {
          nativeHarness.initializeState(saved);
          nativeHarness.open();
          assertEquals(
              rocks ? "rocksdb" : "memory",
              ((CanonicalRank) nativeHarness.getOperator()).storage());
          push(nativeHarness, layout.type, suffix, 1500);
        }
        time(reference, 1500);
        feed(reference, layout.type, suffix);
        assertEquals(
            bytes(output, reference.extractOutputValues()),
            bytes(
                output,
                nativeFirst ? second.extractOutputValues() : collect(nativeHarness, output)));
        saved = nativeFirst ? snapshot(second) : snapshot(nativeHarness);
      }
      try (var third = stock(layout, output, start, variable, ttl, rocks, before, rank);
          var nativeHarness = nativeHarness(layout, start, variable, ttl, rocks, before, rank)) {
        if (nativeFirst) {
          nativeHarness.initializeState(saved);
          nativeHarness.open();
          push(nativeHarness, layout.type, future, 2200);
        } else {
          third.initializeState(saved);
          third.open();
          time(third, 2200);
          feed(third, layout.type, future);
        }
        time(reference, 2200);
        feed(reference, layout.type, future);
        assertEquals(
            bytes(output, reference.extractOutputValues()),
            bytes(
                output,
                nativeFirst ? collect(nativeHarness, output) : third.extractOutputValues()));
      }
    }
  }

  static final class Layout {
    final String shape;
    final RowType type;
    final int[] keys, sorts, ascending, nullsFirst;
    final int score, bound;

    Layout(String shape, boolean twoSorts) {
      this.shape = shape;
      RowType base;
      if (shape.equals("composite")) {
        base = RowType.of(new IntType(), new VarCharType(100), new IntType(), new VarCharType(100));
        keys = new int[] {3, 2};
        score = 0;
      } else {
        LogicalType key =
            shape.equals("string")
                ? new VarCharType(100)
                : shape.equals("long") ? new BigIntType() : new IntType();
        LogicalType value =
            shape.equals("long")
                ? new VarCharType(100)
                : shape.equals("string")
                    ? new IntType()
                    : new BigIntType(!shape.equals("not-null"));
        base =
            shape.equals("collision")
                ? RowType.of(
                    new LogicalType[] {key, value, new VarCharType(100)},
                    new String[] {"binary_key", "$stock_meta_4", "$stock_meta_3"})
                : RowType.of(key, value, new VarCharType(100));
        keys = new int[] {0};
        score = 1;
      }
      var fields = new ArrayList<>(base.getFields());
      bound = fields.size();
      fields.add(
          new RowType.RowField(
              shape.equals("collision") ? "$stock_meta_7" : "bound",
              shape.equals("int") || shape.equals("string")
                  ? new IntType(false)
                  : new BigIntType(false)));
      type = new RowType(fields);
      sorts = twoSorts ? new int[] {score, shape.equals("composite") ? 1 : 2} : new int[] {score};
      ascending =
          twoSorts
              ? new int[] {shape.equals("long") ? 0 : 1, 0}
              : new int[] {shape.equals("long") ? 0 : 1};
      nullsFirst = twoSorts ? new int[] {1, 0} : new int[] {1};
    }

    RowData row(Integer key, Integer score, int id, int bound) {
      var label = StringData.fromString("row-😀-" + id);
      Object size =
          type.getTypeAt(this.bound).is(LogicalTypeRoot.INTEGER) ? (Object) bound : (long) bound;
      if (shape.equals("composite"))
        return GenericRowData.of(
            score, label, key, key == null ? null : StringData.fromString("分区-" + key), size);
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
      return GenericRowData.of(partition, value, label, size);
    }

    RowData change(Integer key, Integer score, int id, int bound, RowKind kind) {
      var row = row(key, score, id, bound);
      row.setRowKind(kind);
      return row;
    }
  }

  private static RowDataKeySelector selector(RowType type, int... columns) {
    return KeySelectorUtil.getRowDataSelector(
        CanonicalRetractingRankTypedTest.class.getClassLoader(),
        columns,
        InternalTypeInfo.of(type));
  }

  private static ComparableRecordComparator comparator(Layout layout) {
    var sort = selector(layout.type, layout.sorts);
    var sortType = (RowType) sort.getProducedType().toLogicalType();
    var spec = SortSpec.builder();
    int[] positions = new int[layout.sorts.length];
    boolean[] ascending = new boolean[positions.length], nullsLast = new boolean[positions.length];
    for (int i = 0; i < positions.length; i++) {
      positions[i] = i;
      ascending[i] = layout.ascending[i] != 0;
      nullsLast[i] = layout.nullsFirst[i] == 0;
      spec.addField(i, ascending[i], nullsLast[i]);
    }
    var generated =
        ComparatorCodeGenerator.gen(
            new Configuration(),
            CanonicalRetractingRankTypedTest.class.getClassLoader(),
            "TypedCanonicalRetractComparator",
            sortType,
            spec.build());
    return new ComparableRecordComparator(
        generated,
        positions,
        sortType.getChildren().toArray(new LogicalType[0]),
        ascending,
        nullsLast);
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(
      Layout layout,
      RowType output,
      long start,
      boolean variable,
      long ttl,
      boolean rocks,
      boolean before,
      boolean rank)
      throws Exception {
    var function =
        new RetractableTopNFunction(
            StateConfigUtil.createTtlConfig(ttl),
            InternalTypeInfo.of(layout.type),
            comparator(layout),
            selector(layout.type, layout.sorts),
            RankType.ROW_NUMBER,
            variable ? new VariableRankRange(layout.bound) : new ConstantRankRange(start, 3),
            new EqualiserCodeGenerator(
                    layout.type, CanonicalRetractingRankTypedTest.class.getClassLoader())
                .generateRecordEqualiser("TypedCanonicalRetractEqualiser"),
            before,
            rank);
    var operator = new KeyedProcessOperator<RowData, RowData, RowData>(function);
    function.setKeyContext(operator);
    var key = selector(layout.type, layout.keys);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            operator, key, key.getProducedType(), 128, 1, 0);
    harness.setStateBackend(
        rocks
            ? tech.streamfusion.compat.CanonicalTestCompat.rocksBackend()
            : new HashMapStateBackend());
    harness.setup(new RowDataSerializer(output));
    return harness;
  }

  private KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness(
      Layout layout,
      long start,
      boolean variable,
      long ttl,
      boolean rocks,
      boolean before,
      boolean rank)
      throws Exception {
    var key = selector(layout.type, layout.keys);
    var keyType = (RowType) key.getProducedType().toLogicalType();
    var contract =
        new StockRetractingTopNStateContract(
            new RowDataSerializer(keyType),
            key,
            selector(layout.type, layout.sorts),
            keyType,
            layout.type,
            comparator(layout),
            variable,
            ttl > 0);
    var operator = new CanonicalRank(layout, start, variable, ttl, before, rank);
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

  private static int[] descriptor(int length) {
    var descriptor = new int[length + 1];
    java.util.Arrays.fill(descriptor, -1);
    descriptor[0] = Native.STOCK_KEY_PROJECTION;
    return descriptor;
  }

  private static void push(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      RowType type,
      List<RowData> rows,
      long now)
      throws Exception {
    time(harness, now);
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

  private static final class CanonicalRank extends NativeColumnarTopNOperator {
    private boolean canonical;

    CanonicalRank(
        Layout layout, long start, boolean variable, long ttl, boolean before, boolean rank) {
      super(
          layout.keys,
          descriptor(layout.keys.length),
          layout.type,
          layout.sorts,
          layout.ascending,
          layout.nullsFirst,
          start - 1,
          3,
          rank,
          true,
          null,
          null,
          before,
          false,
          -1,
          ttl,
          128,
          variable ? layout.bound : -1,
          variable);
    }

    String storage() {
      return rocksdbState() ? directRocksDBState() ? "rocksdb" : "rocksdb-snapshot" : "memory";
    }

    @Override
    protected org.apache.flink.streaming.api.operators.OperatorSnapshotFutures
        snapshotCanonicalState(
            long id,
            long now,
            org.apache.flink.runtime.checkpoint.CheckpointOptions options,
            org.apache.flink.runtime.state.CheckpointStreamFactory streams)
            throws Exception {
      canonical = true;
      try {
        return super.snapshotCanonicalState(id, now, options, streams);
      } finally {
        canonical = false;
      }
    }

    @Override
    protected byte[][] snapshotRawPartitions() {
      if (canonical)
        throw new AssertionError("Canonical retract export serialized raw native state");
      return super.snapshotRawPartitions();
    }

    @Override
    protected byte[][] snapshotCanonicalPartitions() {
      if (canonical)
        throw new AssertionError("Canonical retract export serialized raw native state");
      return super.snapshotCanonicalPartitions();
    }

    @Override
    protected String[] checkpointRocksDBHandle(String directory) {
      if (canonical) throw new AssertionError("Canonical retract export checkpointed native state");
      return super.checkpointRocksDBHandle(directory);
    }
  }
}

package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.codegen.HashCodeGenerator;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializerV2;
import org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetStateConfig;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.operator.*;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

class DorisMaterializerStrategyTest {
  static Stream<Arguments> cases() {
    var values = new ArrayList<Object[]>();
    values.add(new Object[] {"zero", new DoubleType(), 0.0, 2.0, -0.0});
    values.add(
        new Object[] {
          "nan",
          new DoubleType(),
          Double.longBitsToDouble(0x7ff8000000000001L),
          2.0,
          Double.longBitsToDouble(0x7ff8000000000002L)
        });
    values.add(
        new Object[] {
          "zero-pair",
          RowType.of(new DoubleType(), new DoubleType()),
          GenericRowData.of(0.0, 0.0),
          GenericRowData.of(2.0, 2.0),
          GenericRowData.of(-0.0, -0.0)
        });
    var nested = RowType.of(new DoubleType(), new DoubleType());
    var nestedSerializer = new RowDataSerializer(nested);
    values.add(
        new Object[] {
          "binary-row",
          nested,
          nestedSerializer.toBinaryRow(GenericRowData.of(0.0, 0.0)).copy(),
          nestedSerializer.toBinaryRow(GenericRowData.of(2.0, 2.0)).copy(),
          nestedSerializer.toBinaryRow(GenericRowData.of(-0.0, -0.0)).copy()
        });
    values.add(
        new Object[] {
          "array-null",
          new ArrayType(new IntType()),
          new GenericArrayData(new Object[] {1, null}),
          new GenericArrayData(new Object[] {2}),
          new GenericArrayData(new Object[] {1, null})
        });
    var mapType = new MapType(new VarCharType(VarCharType.MAX_LENGTH), new IntType());
    values.add(
        new Object[] {"map-order", mapType, map("a", 1, "b", 2), map("z", 9), map("b", 2, "a", 1)});
    values.add(new Object[] {"map-null", mapType, map("a", null), map("z", 9), map("a", null)});
    values.add(
        new Object[] {
          "map-duplicates",
          new MapType(new IntType(), new IntType()),
          duplicateMap(0),
          duplicateMap(9),
          duplicateMap(1)
        });
    values.add(
        new Object[] {
          "timestamp",
          new TimestampType(9),
          TimestampData.fromEpochMillis(-1, 999999),
          TimestampData.fromEpochMillis(1, 1),
          TimestampData.fromEpochMillis(-1, 999999)
        });
    values.add(
        new Object[] {
          "decimal",
          new DecimalType(38, 2),
          decimal("123456789012345678901234567890.12"),
          decimal("9.99"),
          decimal("123456789012345678901234567890.12")
        });
    double nan1 = Double.longBitsToDouble(0x7ff8000000000001L);
    double nan2 = Double.longBitsToDouble(0x7ff8000000000002L);
    Object firstNan = new GenericArrayData(new Object[] {nan1});
    Object secondArray = new GenericArrayData(new Object[] {2.0});
    Object otherNan = new GenericArrayData(new Object[] {nan2});
    values.add(
        new Object[] {
          "array-nan", new ArrayType(new DoubleType()), firstNan, secondArray, otherNan
        });
    values.add(
        new Object[] {
          "array-nan-retract", new ArrayType(new DoubleType()), firstNan, secondArray, otherNan
        });
    values.add(
        new Object[] {
          "array-float-nan",
          new ArrayType(new FloatType()),
          new GenericArrayData(new Object[] {Float.intBitsToFloat(0x7fc00001)}),
          new GenericArrayData(new Object[] {2.0f}),
          new GenericArrayData(new Object[] {Float.intBitsToFloat(0x7fc00002)})
        });
    values.add(
        new Object[] {
          "array-row-nan",
          new ArrayType(RowType.of(new DoubleType())),
          new GenericArrayData(new Object[] {GenericRowData.of(nan1)}),
          new GenericArrayData(new Object[] {GenericRowData.of(2.0)}),
          new GenericArrayData(new Object[] {GenericRowData.of(nan2)})
        });
    values.add(
        new Object[] {
          "map-nan-value-retract",
          new MapType(new VarCharType(VarCharType.MAX_LENGTH), new DoubleType()),
          map("a", nan1),
          map("b", 2.0),
          map("a", nan2)
        });
    values.add(
        new Object[] {
          "map-nan-key-retract",
          new MapType(new DoubleType(), new IntType()),
          new GenericMapData(java.util.Collections.singletonMap(nan1, 1)),
          new GenericMapData(java.util.Collections.singletonMap(2.0, 2)),
          new GenericMapData(java.util.Collections.singletonMap(nan2, 1))
        });
    values.add(
        new Object[] {
          "map-int-uncached",
          new MapType(new VarCharType(VarCharType.MAX_LENGTH), new IntType()),
          map("a", 1000),
          map("b", 2000),
          map("a", 1000)
        });
    values.add(
        new Object[] {
          "map-long-uncached",
          new MapType(new VarCharType(VarCharType.MAX_LENGTH), new BigIntType()),
          map("a", 1000L),
          map("b", 2000L),
          map("a", 1000L)
        });
    values.add(
        new Object[] {
          "map-short-uncached",
          new MapType(new VarCharType(VarCharType.MAX_LENGTH), new SmallIntType()),
          map("a", (short) 1000),
          map("b", (short) 2000),
          map("a", (short) 1000)
        });
    values.add(
        new Object[] {
          "map-float-boxed",
          new MapType(new VarCharType(VarCharType.MAX_LENGTH), new FloatType()),
          map("a", 1.0f),
          map("b", 2.0f),
          map("a", 1.0f)
        });
    values.add(new Object[] {"cursor", new BigIntType(), 10L, 20L, 10L});
    values.add(new Object[] {"adaptive-cycle", new BigIntType(), 10L, 20L, 10L});
    var cases = new ArrayList<Arguments>();
    for (String strategy : List.of("VALUE", "MAP", "ADAPTIVE"))
      for (boolean rocks : new boolean[] {false, true})
        for (boolean inputKey : new boolean[] {false, true})
          for (var value : values)
            cases.add(
                Arguments.of(
                    strategy, rocks, inputKey, value[0], value[1], value[2], value[3], value[4]));
    return cases.stream();
  }

  @ParameterizedTest(name = "{0} rocks={1} inputKey={2} {3}")
  @MethodSource("cases")
  void selectedReleasedStrategyMatchesAcrossTypesAndRestore(
      String strategy,
      boolean rocks,
      boolean inputKey,
      String name,
      LogicalType valueType,
      Object first,
      Object second,
      Object replacement)
      throws Exception {
    var type =
        RowType.of(
            new LogicalType[] {new BigIntType(), valueType, new BigIntType()},
            new String[] {"id", "uid", "n"});
    List<GenericRowData> rows =
        name.endsWith("-retract")
            ? List.of(
                row(RowKind.INSERT, first, 10L),
                row(RowKind.INSERT, second, 20L),
                row(RowKind.DELETE, replacement, 10L),
                row(RowKind.DELETE, second, 20L))
            : name.equals("adaptive-cycle")
                ? List.of(
                    row(RowKind.INSERT, 10L, 10L),
                    row(RowKind.INSERT, 20L, 20L),
                    row(RowKind.INSERT, 30L, 30L),
                    row(RowKind.DELETE, 30L, 30L),
                    row(RowKind.DELETE, 20L, 20L),
                    row(RowKind.INSERT, 40L, 40L),
                    row(RowKind.DELETE, 40L, 40L),
                    row(RowKind.DELETE, 10L, 10L))
                : name.equals("cursor")
                    ? List.of(
                        row(RowKind.INSERT, 10L, 10L),
                        row(RowKind.INSERT, 20L, 20L),
                        row(RowKind.INSERT, 30L, 30L),
                        row(RowKind.UPDATE_AFTER, 10L, 11L),
                        row(RowKind.INSERT, 40L, 40L),
                        row(RowKind.DELETE, 40L, 40L))
                    : List.of(
                        row(RowKind.INSERT, first, 10L),
                        row(RowKind.INSERT, second, 20L),
                        row(RowKind.UPDATE_AFTER, replacement, 11L),
                        row(RowKind.DELETE, second, 20L),
                        row(RowKind.DELETE, replacement, inputKey ? 99L : 11L));
    assertEquals(
        stock(type, strategy, rocks, inputKey, rows, name.equals("adaptive-cycle")),
        nativeRows(type, strategy, rocks, inputKey, rows, name.equals("adaptive-cycle")),
        name);
  }

  private List<String> stock(
      RowType type,
      String strategy,
      boolean rocks,
      boolean inputKey,
      List<GenericRowData> rows,
      boolean cycle)
      throws Exception {
    var loader = getClass().getClassLoader();
    var config = new Configuration();
    var key = RowType.of(type.getTypeAt(1));
    var settings =
        strategy.equals("MAP")
            ? SequencedMultiSetStateConfig.forMap(TimeDomain.EVENT_TIME, StateTtlConfig.DISABLED)
            : strategy.equals("VALUE")
                ? SequencedMultiSetStateConfig.forValue(
                    TimeDomain.EVENT_TIME, StateTtlConfig.DISABLED)
                : SequencedMultiSetStateConfig.adaptive(
                    TimeDomain.EVENT_TIME,
                    cycle ? 3L : 1L,
                    cycle ? 1L : 0L,
                    StateTtlConfig.DISABLED);
    var operator =
        SinkUpsertMaterializerV2.create(
            type,
            new EqualiserCodeGenerator(type, loader).generateRecordEqualiser("StrategyRowEq"),
            new EqualiserCodeGenerator(key, loader).generateRecordEqualiser("StrategyKeyEq"),
            HashCodeGenerator.generateRowHash(
                new CodeGeneratorContext(config, loader),
                type,
                "StrategyRowHash",
                new int[] {0, 1, 2}),
            HashCodeGenerator.generateRowHash(
                new CodeGeneratorContext(config, loader), key, "StrategyKeyHash", new int[] {0}),
            inputKey ? new int[] {1} : null,
            settings);
    try (var h =
        new KeyedOneInputStreamOperatorTestHarness<Integer, RowData, RowData>(
            operator, r -> 0, Types.INT, 128, 1, 0)) {
      if (rocks) h.setStateBackend(new EmbeddedRocksDBStateBackend());
      h.setup(new RowDataSerializer(type));
      h.open();
      for (var row : rows)
        h.processElement(
            new StreamRecord<>(new RowDataSerializer(type).toBinaryRow(row).copy(), 0));
      var output = new ArrayList<String>();
      for (var r : h.extractOutputStreamRecords()) output.add(tag(r.getValue()));
      return output;
    }
  }

  private List<String> nativeRows(
      RowType type,
      String strategy,
      boolean rocks,
      boolean inputKey,
      List<GenericRowData> rows,
      boolean cycle)
      throws Exception {
    TaskOffHeapMemory.initialize(
        new Configuration()
            .set(
                org.apache.flink.configuration.TaskManagerOptions.TASK_OFF_HEAP_MEMORY,
                org.apache.flink.configuration.MemorySize.ofMebiBytes(256)));
    int split = cycle ? 4 : rows.size() - 1;
    OperatorSubtaskState snapshot;
    var output = new ArrayList<String>();
    try (var allocator = new RootAllocator();
        var h = harness(type, strategy, rocks, inputKey, cycle)) {
      h.open();
      for (var row : rows.subList(0, split)) {
        push(h, allocator, type, row);
        output.addAll(collect(h, type));
      }
      snapshot = h.snapshot(1, 1);
    }
    try (var allocator = new RootAllocator();
        var h = harness(type, strategy, rocks, inputKey, cycle)) {
      h.initializeState(snapshot);
      h.open();
      for (var row : rows.subList(split, rows.size())) {
        push(h, allocator, type, row);
        output.addAll(collect(h, type));
      }
    }
    return output;
  }

  private KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      RowType type, String strategy, boolean rocks, boolean inputKey, boolean cycle)
      throws Exception {
    var config =
        new Configuration()
            .set(
                ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE_STRATEGY,
                ExecutionConfigOptions.SinkUpsertMaterializeStrategy.valueOf(strategy))
            .set(
                ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE_ADAPTIVE_THRESHOLD_HIGH,
                cycle ? 3L : 1L)
            .set(
                ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE_ADAPTIVE_THRESHOLD_LOW,
                cycle ? 1L : 0L);
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    var input =
        env.fromCollection(List.<ArrowBatch>of(), ArrowBatchTypeInformation.INSTANCE)
            .getTransformation();
    var transform =
        (OneInputTransformation<ArrowBatch, ArrowBatch>)
            new DorisSinkRuntime(
                    null,
                    type,
                    1,
                    new int[] {0},
                    new int[] {-1},
                    new String[3],
                    true,
                    inputKey ? new int[] {1} : null)
                .prepare(input, env, ExecNodeConfig.ofNodeConfig(config, false));
    var operator =
        (NativeSinkMaterializer)
            ((SimpleOperatorFactory<?>) transform.getOperatorFactory()).getOperator();
    var h =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            operator, b -> 0, Types.INT, 128, 1, 0);
    if (rocks)
      h.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(new Configuration(), getClass().getClassLoader()));
    h.setup(new ArrowBatchSerializer());
    return h;
  }

  private static void push(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h,
      RootAllocator allocator,
      RowType type,
      RowData row)
      throws Exception {
    h.processElement(
        new StreamRecord<>(
            new ArrowBatch(RowDataArrowConverter.write(List.of(row), type, allocator, true))));
  }

  private static List<String> collect(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h, RowType type) {
    var rows = new ArrayList<String>();
    while (!h.getOutput().isEmpty()) {
      Object event = h.getOutput().poll();
      if (event instanceof StreamRecord)
        try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (var row : RowDataArrowConverter.read(root, type)) rows.add(tag(row));
        }
    }
    return rows;
  }

  private static GenericRowData row(RowKind kind, Object value, long tag) {
    return GenericRowData.ofKind(kind, 1L, value, tag);
  }

  private static String tag(RowData row) {
    return row.getRowKind().shortString() + ":" + row.getLong(2);
  }

  private static GenericMapData map(Object... entries) {
    var map = new LinkedHashMap<Object, Object>();
    for (int i = 0; i < entries.length; i += 2)
      map.put(StringData.fromString((String) entries[i]), entries[i + 1]);
    return new GenericMapData(map);
  }

  private static BinaryMapData duplicateMap(int first) {
    return BinaryMapData.valueOf(
        BinaryArrayData.fromPrimitiveArray(new int[] {1, 1}),
        BinaryArrayData.fromPrimitiveArray(new int[] {first, 2}));
  }

  private static DecimalData decimal(String value) {
    return DecimalData.fromBigDecimal(new java.math.BigDecimal(value), 38, 2);
  }
}

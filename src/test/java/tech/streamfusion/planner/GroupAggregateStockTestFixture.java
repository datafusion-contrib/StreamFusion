package tech.streamfusion.planner;

import java.lang.reflect.Field;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.state.StockGroupArithmeticStateContract;
import tech.streamfusion.state.StockGroupExtremaStateContract;
import tech.streamfusion.state.StockStateContract;

final class GroupAggregateStockTestFixture implements AutoCloseable {
  final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness;
  final Object function;
  final RowDataKeySelector selector;
  final RowType accumulatorType, outputType, inputType;
  final int[] kinds, scalarFields, countFields, valueColumns, keyColumns;
  final int maximum;
  final boolean before;
  final long ttl;

  @SuppressWarnings("unchecked")
  GroupAggregateStockTestFixture(String mode, boolean rocks, long ttl) throws Exception {
    this(mode, rocks, ttl, null);
  }

  @SuppressWarnings("unchecked")
  GroupAggregateStockTestFixture(String mode, boolean rocks, long ttl, Boolean beforePolicy)
      throws Exception {
    this(mode, rocks, ttl, beforePolicy, false);
  }

  @SuppressWarnings("unchecked")
  GroupAggregateStockTestFixture(
      String mode, boolean rocks, long ttl, Boolean beforePolicy, boolean append) throws Exception {
    this(mode, rocks, ttl, beforePolicy, append, false);
  }

  @SuppressWarnings("unchecked")
  GroupAggregateStockTestFixture(
      String mode,
      boolean rocks,
      long ttl,
      Boolean beforePolicy,
      boolean append,
      boolean integerKey)
      throws Exception {
    this(mode, rocks, ttl, beforePolicy, append, integerKey, false);
  }

  @SuppressWarnings("unchecked")
  GroupAggregateStockTestFixture(
      String mode,
      boolean rocks,
      long ttl,
      Boolean beforePolicy,
      boolean append,
      boolean integerKey,
      boolean global)
      throws Exception {
    this(mode, rocks, ttl, beforePolicy, append, integerKey, global, "BIGINT");
  }

  @SuppressWarnings("unchecked")
  GroupAggregateStockTestFixture(
      String mode,
      boolean rocks,
      long ttl,
      Boolean beforePolicy,
      boolean append,
      boolean integerKey,
      boolean global,
      String valueSqlType)
      throws Exception {
    if (!valueSqlType.equals("BIGINT")
        && ((!mode.equals("extrema") && !mode.equals("counted-extrema")) || append))
      throw new IllegalArgumentException("Typed fixture requires retracting extrema");
    var valueType =
        valueSqlType.startsWith("TIME(")
            ? new org.apache.flink.table.types.logical.TimeType(
                Integer.parseInt(valueSqlType.substring(5, valueSqlType.length() - 1)))
            : org.apache.flink.table.types.logical.utils.LogicalTypeParser.parse(
                valueSqlType, GroupAggregateStockTestFixture.class.getClassLoader());
    this.ttl = ttl;
    keyColumns = global ? new int[0] : new int[] {0};
    maximum = global ? 1 : 128;
    String keyType = integerKey ? "INT" : "BIGINT";
    var expectedInput =
        global
            ? (mode.equals("star")
                ? RowType.of(
                    new org.apache.flink.table.types.logical.BigIntType(),
                    new org.apache.flink.table.types.logical.BigIntType())
                : RowType.of(valueType))
            : RowType.of(
                integerKey
                    ? new org.apache.flink.table.types.logical.IntType()
                    : new org.apache.flink.table.types.logical.BigIntType(),
                valueType);
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "false");
    var table = NativePlannerTestEnvironment.create(config);
    table.getConfig().set("table.exec.mini-batch.enabled", "false");
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("table.exec.state.ttl", ttl + " ms");
    table.registerCatalog(
        "fixture",
        new org.apache.flink.table.catalog.GenericInMemoryCatalog("fixture", "db") {
          @Override
          public java.util.Optional<org.apache.flink.table.factories.Factory> getFactory() {
            return java.util.Optional.of(new org.example.canonical.CanonicalFixtureFactory());
          }
        });
    table.useCatalog("fixture");
    table.executeSql(
        "CREATE TABLE input_table(k "
            + keyType
            + ",v "
            + valueSqlType
            + ") WITH"
            + " ('connector'='canonical-fixture','input'='unused.csv','ready'='unused.ready','insert-only'='"
            + append
            + "')");
    table.useCatalog("default_catalog");
    String fields =
        mode.equals("counted-extrema")
            ? "COUNT(v) AS a,MIN(v) AS b,MAX(v) AS c"
            : mode.equals("hidden-extrema")
                ? "SUM(v) AS c,AVG(v) AS d,MIN(v) AS e,MAX(v) AS f"
                : mode.equals("arithmetic")
                    ? "COUNT(*) AS a,COUNT(v) AS b,SUM(v) AS c,AVG(v) AS d"
                    : mode.equals("hidden")
                        ? "SUM(v) AS c,AVG(v) AS d"
                        : mode.equals("extrema")
                            ? "MIN(v) AS a,MAX(v) AS b"
                            : mode.equals("mixed-extrema")
                                ? "COUNT(*) AS a,COUNT(v) AS b,SUM(v) AS c,AVG(v) AS d,MIN(v) AS"
                                    + " e,MAX(v) AS f"
                                : mode.equals("count") ? "COUNT(v) AS a" : "COUNT(*) AS a";
    table.executeSql(
        "CREATE TABLE output_table("
            + (global ? "" : "k " + keyType + ",")
            + (mode.equals("counted-extrema")
                ? "a BIGINT,b " + valueSqlType + ",c " + valueSqlType
                : mode.equals("hidden-extrema")
                    ? "c BIGINT,d BIGINT,e BIGINT,f BIGINT"
                    : mode.equals("arithmetic")
                        ? "a BIGINT,b BIGINT,c BIGINT,d BIGINT"
                        : mode.equals("hidden")
                            ? "c BIGINT,d BIGINT"
                            : mode.equals("extrema")
                                ? "a " + valueSqlType + ",b " + valueSqlType
                                : mode.equals("mixed-extrema")
                                    ? "a BIGINT,b BIGINT,c BIGINT,d BIGINT,e BIGINT,f BIGINT"
                                    : "a BIGINT")
            + ") WITH ('connector'='blackhole')");
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var transforms =
        CanonicalRegularJoinPlanTest.translate(
            planner,
            "INSERT INTO output_table SELECT "
                + (global ? "" : "k,")
                + fields
                + " FROM fixture.db.input_table"
                + (global ? "" : " GROUP BY k"));
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transforms);
    var node =
        graph.getStreamNodes().stream()
            .filter(
                n ->
                    n.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) n.getOperatorFactory()).getOperator()
                            instanceof KeyedProcessOperator
                        && ((KeyedProcessOperator<?, ?, ?>)
                                    ((SimpleOperatorFactory<?>) n.getOperatorFactory())
                                        .getOperator())
                                .getUserFunction()
                            instanceof GroupAggFunction)
            .findFirst()
            .orElseThrow(() -> new AssertionError(graph.getStreamingPlanAsJSON()));
    inputType =
        global
            ? RowType.of((LogicalType[]) field(node.getTypeSerializersIn()[0], "types"))
            : expectedInput;
    if (global) {
      if (node.getParallelism() != 1
          || tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(node) != 1)
        throw new AssertionError(
            "Stock global owner must be singleton with maximum parallelism one");
    }
    var operator =
        (KeyedProcessOperator<RowData, RowData, RowData>)
            ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator();
    var plannedFunction = operator.getUserFunction();
    if (beforePolicy != null) {
      var generated =
          (org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction)
              field(plannedFunction, "genAggsHandler");
      var equaliser =
          (org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser)
              field(plannedFunction, "genRecordEqualiser");
      var acc = (LogicalType[]) field(plannedFunction, "accTypes");
      var plannedCounter = field(plannedFunction, "recordCounter");
      int counter =
          plannedCounter.getClass()
                  == org.apache.flink.table.runtime.operators.aggregate.RecordCounter.of(-1)
                      .getClass()
              ? -1
              : (int) field(plannedCounter, "indexOfCountStar");
      operator =
          new KeyedProcessOperator<>(
              new GroupAggFunction(generated, equaliser, acc, counter, beforePolicy, ttl));
    }
    function = operator.getUserFunction();
    selector = (RowDataKeySelector) node.getStatePartitioners()[0];
    accumulatorType = RowType.of((LogicalType[]) field(function, "accTypes"));
    outputType = RowType.of((LogicalType[]) field(node.getTypeSerializerOut(), "types"));
    before = (boolean) field(function, "generateUpdateBefore");
    switch (mode) {
      case "arithmetic":
        kinds = new int[] {3, 3, 22};
        scalarFields = new int[] {0, 1, 2};
        countFields = new int[] {-1, -1, -1};
        valueColumns = new int[] {-1, global ? 0 : 1, global ? 0 : 1};
        break;
      case "hidden":
        kinds = new int[] {0, 4};
        scalarFields = append ? new int[] {0, 1} : new int[] {0, 2};
        countFields = append ? new int[] {-1, 2} : new int[] {1, 3};
        valueColumns = new int[] {global ? 0 : 1, global ? 0 : 1};
        break;
      case "hidden-extrema":
        kinds = append ? new int[] {0, 4, 10, 11} : new int[] {0, 4, 38, 39};
        scalarFields = append ? new int[] {0, 1, 3, 4} : new int[] {0, 2, 4, 5};
        countFields = append ? new int[] {-1, 2, -1, -1} : new int[] {1, 3, -1, -1};
        valueColumns = new int[] {global ? 0 : 1, global ? 0 : 1, global ? 0 : 1, global ? 0 : 1};
        break;
      case "counted-extrema":
        kinds = new int[] {3, 38, 39};
        scalarFields = new int[] {0, 1, 2};
        countFields = new int[] {-1, -1, -1};
        valueColumns = new int[] {global ? 0 : 1, global ? 0 : 1, global ? 0 : 1};
        break;
      case "extrema":
        kinds = append ? new int[] {10, 11} : new int[] {38, 39};
        scalarFields = new int[] {0, 1};
        countFields = new int[] {-1, -1};
        valueColumns = new int[] {global ? 0 : 1, global ? 0 : 1};
        break;
      case "mixed-extrema":
        kinds = append ? new int[] {3, 3, 22, 10, 11} : new int[] {3, 3, 22, 38, 39};
        scalarFields = new int[] {0, 1, 2, 3, 4};
        countFields = new int[] {-1, -1, -1, -1, -1};
        valueColumns =
            new int[] {-1, global ? 0 : 1, global ? 0 : 1, global ? 0 : 1, global ? 0 : 1};
        break;
      case "count":
        kinds = new int[] {3};
        scalarFields = new int[] {0};
        countFields = new int[] {-1};
        valueColumns = new int[] {global ? 0 : 1};
        break;
      case "star":
        kinds = new int[] {3};
        scalarFields = new int[] {0};
        countFields = new int[] {-1};
        valueColumns = new int[] {-1};
        break;
      default:
        throw new IllegalArgumentException(mode);
    }
    harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            operator, selector, selector.getProducedType(), maximum, 1, 0);
    harness.setStateBackend(
        rocks
            ? tech.streamfusion.compat.CanonicalTestCompat.rocksBackend()
            : new HashMapStateBackend());
    harness.setup((TypeSerializer<RowData>) node.getTypeSerializerOut());
  }

  StockStateContract contract() throws Exception {
    var keys =
        selector
            .getProducedType()
            .createSerializer(
                tech.streamfusion.compat.FlinkPlanCompat.serializerConfig(
                    new org.apache.flink.api.common.ExecutionConfig()));
    var counter = field(function, "recordCounter");
    int recordCountField =
        counter.getClass()
                == org.apache.flink.table.runtime.operators.aggregate.RecordCounter.of(-1)
                    .getClass()
            ? -1
            : (int) field(counter, "indexOfCountStar");
    if (java.util.Arrays.stream(kinds).anyMatch(kind -> kind == 38 || kind == 39)) {
      var maps = new String[kinds.length];
      var mapKeys = new TypeSerializer<?>[kinds.length];
      var mapValues = new TypeSerializer<?>[kinds.length];
      for (int i = 0; i < kinds.length; i++)
        if (kinds[i] == 38 || kinds[i] == 39) {
          maps[i] = "agg" + i + "$map";
          mapKeys[i] =
              org.apache.flink.table.runtime.typeutils.ExternalSerializer.of(
                  org.apache.flink.table.types.utils.DataTypeUtils.toInternalDataType(
                      accumulatorType.getTypeAt(scalarFields[i]).getChildren().get(0).copy(false)));
          mapValues[i] =
              org.apache.flink.table.runtime.typeutils.ExternalSerializer.of(
                  org.apache.flink.table.api.DataTypes.BIGINT());
        }
      return new StockGroupExtremaStateContract(
          keys,
          (RowType) selector.getProducedType().toLogicalType(),
          accumulatorType,
          kinds,
          scalarFields,
          countFields,
          maps,
          mapKeys,
          mapValues,
          recordCountField,
          ttl > 0);
    }
    return new StockGroupArithmeticStateContract(
        keys,
        (RowType) selector.getProducedType().toLogicalType(),
        accumulatorType,
        kinds,
        scalarFields,
        countFields,
        recordCountField,
        ttl > 0);
  }

  int[] valueTypes() {
    var result = new int[kinds.length];
    for (int i = 0; i < result.length; i++) {
      if (valueColumns[i] < 0 || kinds[i] == 3) continue;
      switch (inputType.getTypeAt(valueColumns[i]).getTypeRoot()) {
        case BIGINT:
          result[i] = 0;
          break;
        case INTEGER:
          result[i] = 2;
          break;
        case SMALLINT:
          result[i] = 4;
          break;
        case TINYINT:
          result[i] = 5;
          break;
        case DATE:
          result[i] = 8;
          break;
        case TIME_WITHOUT_TIME_ZONE:
          result[i] = 9;
          break;
        case BOOLEAN:
          result[i] = 10;
          break;
        default:
          throw new IllegalArgumentException("Unqualified fixture value type");
      }
    }
    return result;
  }

  static Object field(Object target, String name) throws Exception {
    for (Class<?> type = target.getClass(); type != null; type = type.getSuperclass()) {
      try {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
      } catch (NoSuchFieldException ignored) {
      }
    }
    throw new NoSuchFieldException(name);
  }

  org.apache.flink.api.common.state.ValueState<?> rawState() throws Exception {
    var state = (org.apache.flink.api.common.state.ValueState<?>) field(function, "accState");
    return ttl > 0
        ? (org.apache.flink.api.common.state.ValueState<?>) field(state, "original")
        : state;
  }

  java.util.Map<String, String> rawStateBytes() throws Exception {
    var backend =
        (org.apache.flink.runtime.state.CheckpointableKeyedStateBackend<RowData>)
            harness.getOperator().<RowData>getKeyedStateBackend();
    if (java.util.Arrays.stream(kinds).anyMatch(kind -> kind == 38 || kind == 39)) {
      var resources = backend.savepoint().getSnapshotResources();
      try (AutoCloseable release = resources::release) {
        var result = new java.util.TreeMap<String, String>();
        var metadata = resources.getMetaInfoSnapshots();
        try (var iterator = resources.createKVStateIterator()) {
          while (iterator.isValid()) {
            result.put(
                metadata.get(iterator.kvStateId()).getName()
                    + ":"
                    + java.util.Base64.getEncoder().encodeToString(iterator.key()),
                java.util.Base64.getEncoder().encodeToString(iterator.value()));
            iterator.next();
          }
        }
        return result;
      }
    }
    var result = new java.util.TreeMap<String, String>();
    var values = rawState();
    var serializer = new RowDataSerializer(accumulatorType);
    try (var keys =
        backend.getKeys("accState", org.apache.flink.runtime.state.VoidNamespace.INSTANCE)) {
      for (var key :
          keys.map(backend.getKeySerializer()::copy)
              .collect(java.util.stream.Collectors.toList())) {
        backend.setCurrentKey(key);
        Object raw = values.value();
        if (raw == null) continue;
        long written = 0;
        if (raw instanceof org.apache.flink.runtime.state.ttl.TtlValue) {
          written = ((org.apache.flink.runtime.state.ttl.TtlValue<?>) raw).getLastAccessTimestamp();
          raw = ((org.apache.flink.runtime.state.ttl.TtlValue<?>) raw).getUserValue();
        }
        var keyBytes = new org.apache.flink.core.memory.DataOutputSerializer(64);
        backend.getKeySerializer().serialize(key, keyBytes);
        var valueBytes = new org.apache.flink.core.memory.DataOutputSerializer(64);
        serializer.serialize((RowData) raw, valueBytes);
        result.put(
            java.util.Base64.getEncoder().encodeToString(keyBytes.getCopyOfBuffer()),
            written
                + ":"
                + java.util.Base64.getEncoder().encodeToString(valueBytes.getCopyOfBuffer()));
      }
    }
    return result;
  }

  @Override
  public void close() throws Exception {
    harness.close();
  }
}

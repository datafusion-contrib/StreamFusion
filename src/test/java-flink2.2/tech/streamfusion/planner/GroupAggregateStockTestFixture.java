package tech.streamfusion.planner;

import java.lang.reflect.Field;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
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

final class GroupAggregateStockTestFixture implements AutoCloseable {
  final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness;
  final Object function;
  final RowDataKeySelector selector;
  final RowType accumulatorType, outputType;
  final int[] kinds, scalarFields, countFields, valueColumns;
  final boolean before;
  final long ttl;

  @SuppressWarnings("unchecked")
  GroupAggregateStockTestFixture(String mode, boolean rocks, long ttl) throws Exception {
    this(mode, rocks, ttl, null);
  }

  @SuppressWarnings("unchecked")
  GroupAggregateStockTestFixture(String mode, boolean rocks, long ttl, Boolean beforePolicy)
      throws Exception {
    this.ttl = ttl;
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
        "CREATE TABLE input_table(k BIGINT,v BIGINT) WITH"
            + " ('connector'='canonical-fixture','input'='unused.csv','ready'='unused.ready')");
    table.useCatalog("default_catalog");
    String fields =
        mode.equals("arithmetic")
            ? "COUNT(*) AS a,COUNT(v) AS b,SUM(v) AS c,AVG(v) AS d"
            : mode.equals("hidden")
                ? "SUM(v) AS c,AVG(v) AS d"
                : mode.equals("count") ? "COUNT(v) AS a" : "COUNT(*) AS a";
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,"
            + (mode.equals("arithmetic")
                ? "a BIGINT,b BIGINT,c BIGINT,d BIGINT"
                : mode.equals("hidden") ? "c BIGINT,d BIGINT" : "a BIGINT")
            + ") WITH ('connector'='blackhole')");
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var transforms =
        CanonicalRegularJoinPlanTest.translate(
            planner,
            "INSERT INTO output_table SELECT k,"
                + fields
                + " FROM fixture.db.input_table GROUP BY k");
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
      int counter = (int) field(field(plannedFunction, "recordCounter"), "indexOfCountStar");
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
        valueColumns = new int[] {-1, 1, 1};
        break;
      case "hidden":
        kinds = new int[] {0, 4};
        scalarFields = new int[] {0, 2};
        countFields = new int[] {1, 3};
        valueColumns = new int[] {1, 1};
        break;
      case "count":
        kinds = new int[] {3};
        scalarFields = new int[] {0};
        countFields = new int[] {-1};
        valueColumns = new int[] {1};
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
            operator, selector, selector.getProducedType(), 128, 1, 0);
    harness.setStateBackend(rocks ? new EmbeddedRocksDBStateBackend() : new HashMapStateBackend());
    harness.setup((TypeSerializer<RowData>) node.getTypeSerializerOut());
  }

  StockGroupArithmeticStateContract contract() throws Exception {
    var keys =
        selector
            .getProducedType()
            .createSerializer(
                new org.apache.flink.api.common.ExecutionConfig().getSerializerConfig());
    int recordCountField = (int) field(field(function, "recordCounter"), "indexOfCountStar");
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

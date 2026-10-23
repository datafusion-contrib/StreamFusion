package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@org.junit.jupiter.api.condition.DisabledIfSystemProperty(
    named = "streamfusion.experimental.jvmGroupAggregates",
    matches = "false")
class FlinkTypedJvmAggregateSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"VARBINARY", "BINARY(4)"})
  void releasedBinaryDistinctValidationRemainsAHostFailure(String name) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var comparison =
          NativeFailureParity.run(
              () -> environment(name, false, false, metrics),
              "SELECT k, remember(DISTINCT v) FROM src GROUP BY k");
      comparison.assertFailure(
          org.apache.flink.table.api.TableException.class,
          "Distinct aggregate function does not support type:",
          NativeFailureParity.Phase.PLANNING,
          NativeFailureParity.Route.UNPLANNED);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "STRING",
        "CHAR(4)",
        "BOOLEAN",
        "TINYINT",
        "SMALLINT",
        "FLOAT",
        "DOUBLE",
        "VARBINARY",
        "BINARY(4)",
        "DECIMAL(18,8)",
        "DECIMAL(38,18)",
        "DATE",
        "TIME",
        "TIMESTAMP(9)",
        "TIMESTAMP_LTZ(9)",
        "DAY_TIME",
        "YEAR_MONTH"
      })
  void typedDistinctAndFilteredCallbacksPreserveReleasedValueSemantics(String name) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var comparison =
          NativeFailureParity.run(
              () -> environment(name, true, false, metrics),
              "SELECT k, remember("
                  + (name.contains("BINARY") ? "" : "DISTINCT ")
                  + "v) FILTER (WHERE k IS NULL OR k > 1), COUNT(*)"
                  + " FROM src GROUP BY k");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      assertEquals(perKey(comparison.host().rows()), perKey(comparison.nativeRun().rows()));
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmGroup");
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "DECIMAL(18,8)",
        "DECIMAL(38,18)",
        "TIMESTAMP(9)",
        "TIMESTAMP_LTZ(9)",
        "BINARY(4)",
        "DOUBLE",
        "DATE",
        "DAY_TIME"
      })
  void typedAccumulatorFieldsSurviveRepeatedRecovery(String name) throws Exception {
    var valueType = FlinkTypedTableFunctionSqlHarnessTest.valueType(name);
    var type =
        DataTypes.ROW(DataTypes.FIELD("k", DataTypes.INT()), DataTypes.FIELD("v", valueType));
    for (String backend :
        List.of("hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory")) {
      try (var recovery =
          new PortableSqlRecovery(
                  backend,
                  rows(name, true, false),
                  ExternalTypeInfo.<Row>of(type),
                  Schema.newBuilder().fromRowDataType(type).build(),
                  true,
                  16,
                  64)
              .withExecutionMetrics()) {
        var comparison =
            NativeFailureParity.runRecovery(
                () -> register(recovery.uninterrupted(), valueType),
                () -> register(recovery.get(), valueType),
                "SELECT k, remember(v), COUNT(*) FROM recovery_input GROUP BY k");
        comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeJvmGroup");
        SharedFlinkCluster.assertNativeMemoryReleased("typed-jvm-aggregate-recovery");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "CHAR(4)",
        "BOOLEAN",
        "TINYINT",
        "SMALLINT",
        "FLOAT",
        "DOUBLE",
        "VARBINARY",
        "BINARY(4)",
        "DECIMAL(18,8)",
        "DECIMAL(38,18)",
        "DATE",
        "TIME",
        "TIMESTAMP(9)",
        "TIMESTAMP_LTZ(9)",
        "DAY_TIME",
        "YEAR_MONTH"
      })
  void typedArgumentsResultsAndAccumulatorsPreserveChangelog(String name) {
    for (boolean updating : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var comparison =
            NativeFailureParity.run(
                () -> environment(name, updating, false, metrics),
                "SELECT k, remember(v), COUNT(*) FROM src GROUP BY k");
        comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
        assertEquals(perKey(comparison.host().rows()), perKey(comparison.nativeRun().rows()));
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmGroup");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "CHAR(4)",
        "BOOLEAN",
        "TINYINT",
        "SMALLINT",
        "FLOAT",
        "DOUBLE",
        "VARBINARY",
        "BINARY(4)",
        "DECIMAL(18,8)",
        "DECIMAL(38,18)",
        "DATE",
        "TIME",
        "TIMESTAMP(9)",
        "TIMESTAMP_LTZ(9)",
        "DAY_TIME",
        "YEAR_MONTH"
      })
  void newGroupingKeyTypesBypassThePrimitiveAccumulatorCache(String name) {
    String property = "streamfusion.experimental.jvmAccumulatorCacheEntries";
    String previous = System.getProperty(property);
    try {
      System.setProperty(property, "3");
      for (int parallelism : new int[] {1, 3}) {
        try (var metrics = new RecoveryExecutionMetrics()) {
          var comparison =
              NativeFailureParity.run(
                  () -> environment(name, true, true, metrics, parallelism),
                  "SELECT k, remember(v), COUNT(*) FROM src GROUP BY k");
          comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
          assertEquals(perKey(comparison.host().rows()), perKey(comparison.nativeRun().rows()));
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmGroup");
          assertEquals(0, metrics.counterValue("jvmAccumulatorCacheHits"));
        }
      }
    } finally {
      if (previous == null) System.clearProperty(property);
      else System.setProperty(property, previous);
    }
  }

  static TableEnvironment environment(
      String name, boolean updating, boolean typedKey, RecoveryExecutionMetrics metrics) {
    return environment(name, updating, typedKey, metrics, 1);
  }

  private static TableEnvironment environment(
      String name,
      boolean updating,
      boolean typedKey,
      RecoveryExecutionMetrics metrics,
      int parallelism) {
    var valueType = FlinkTypedTableFunctionSqlHarnessTest.valueType(name);
    var keyType = typedKey ? valueType : DataTypes.INT();
    var resultType = typedKey ? DataTypes.INT() : valueType;
    var schema = DataTypes.ROW(DataTypes.FIELD("k", keyType), DataTypes.FIELD("v", resultType));
    var rows = rows(name, updating, typedKey);
    var env = metrics.environment(new Configuration(), parallelism, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    table.createTemporaryView(
        "src",
        table.fromChangelogStream(
            fromData(env, ExternalTypeInfo.<Row>of(schema), rows.toArray(new Row[0])),
            Schema.newBuilder().fromRowDataType(schema).build()));
    return register(table, resultType);
  }

  static List<Row> rows(String name, boolean updating, boolean typedKey) {
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 48; i++) rows.add(row(name, typedKey, i, RowKind.INSERT));
    if (updating) {
      for (int i = 0; i < 48; i++) rows.add(row(name, typedKey, i, RowKind.DELETE));
      for (int i = 0; i < 16; i++) rows.add(row(name, typedKey, i, RowKind.INSERT));
      rows.add(row(name, typedKey, 0, RowKind.UPDATE_BEFORE));
      rows.add(row(name, typedKey, 8, RowKind.UPDATE_AFTER));
    }
    return rows;
  }

  private static Row row(String name, boolean typedKey, int i, RowKind kind) {
    Object value = FlinkTypedTableFunctionSqlHarnessTest.value(name, i / 4);
    Object key = i % 5 == 0 ? null : i % 4;
    return typedKey ? Row.ofKind(kind, value, i) : Row.ofKind(kind, key, value);
  }

  static TableEnvironment register(TableEnvironment table, DataType valueType) {
    table.createTemporarySystemFunction("remember", new Remember(valueType));
    return table;
  }

  private static Map<Object, List<List<Object>>> perKey(List<List<Object>> rows) {
    var result = new HashMap<Object, List<List<Object>>>();
    for (var row : rows) result.computeIfAbsent(row.get(1), key -> new ArrayList<>()).add(row);
    return result;
  }

  /** Records the most recent callback value, including NULL, until its group is empty. */
  public static class Remember extends AggregateFunction<Object, Row> {
    private final DataType type;
    private transient boolean opened;

    public Remember(DataType type) {
      this.type = type;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(type)
          .accumulatorTypeStrategy(
              TypeStrategies.explicit(
                  DataTypes.ROW(
                      DataTypes.FIELD("value", type),
                      DataTypes.FIELD("count", DataTypes.BIGINT()))))
          .outputTypeStrategy(TypeStrategies.explicit(type))
          .build();
    }

    @Override
    public void open(FunctionContext context) {
      if (opened) throw new IllegalStateException("aggregate opened twice");
      opened = true;
    }

    @Override
    public Row createAccumulator() {
      return Row.of(null, 0L);
    }

    public void accumulate(Row accumulator, Object value) {
      if (!opened) throw new IllegalStateException("aggregate is closed");
      accumulator.setField(0, value instanceof byte[] ? ((byte[]) value).clone() : value);
      accumulator.setField(1, (Long) accumulator.getField(1) + 1);
    }

    public void retract(Row accumulator, Object value) {
      if (!opened) throw new IllegalStateException("aggregate is closed");
      long remaining = (Long) accumulator.getField(1) - 1;
      accumulator.setField(1, remaining);
      if (remaining == 0) accumulator.setField(0, null);
    }

    @Override
    public Object getValue(Row accumulator) {
      if (!opened) throw new IllegalStateException("aggregate is closed");
      return accumulator.getField(0);
    }

    @Override
    public void close() {
      if (!opened) throw new IllegalStateException("aggregate closed twice");
      opened = false;
    }
  }
}

package tech.streamfusion;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class JvmFunctionContractsBenchmark {
  @Test
  void aggregateCompleteJobsRetainTypedCallbacksStateAndBothTransposes() throws Exception {
    for (String name :
        System.getProperty("boundary.aggregates", "DECIMAL(38,18);TIMESTAMP(9);BINARY(4)")
            .split(";")) run(name, false);
  }

  @Test
  void wideDecimalGroupingKeysRetainProductionSerializationAndExchange() throws Exception {
    run("DECIMAL(38,18)", true);
  }

  @Test
  void legacyPojoAccumulatorsRetainDistinctStateAndBothTransposes() throws Exception {
    UdfTypeBoundaryBenchmark.run(
        "JVM_AGG_LEGACY_POJO",
        FlinkLegacyJvmAggregateSqlHarnessTest::benchmarkEnvironment,
        FlinkLegacyJvmAggregateSqlHarnessTest.QUERY,
        "NativeJvmGroup",
        Boolean.parseBoolean(System.getProperty("boundary.expectNative", "true")));
  }

  private static void run(String name, boolean typedKey) throws Exception {
    boolean nativeExpected =
        Boolean.parseBoolean(System.getProperty("boundary.expectNative", "true"));
    UdfTypeBoundaryBenchmark.run(
        "JVM_AGG_" + (typedKey ? "KEY_" : "") + name,
        (rows, metrics) -> environment(name, typedKey, rows, metrics),
        "SELECT k, remember(v), COUNT(*) FROM src GROUP BY k",
        "NativeJvmGroup",
        nativeExpected);
  }

  private static TableEnvironment environment(
      String name, boolean typedKey, int rows, RecoveryExecutionMetrics metrics) {
    var valueType = FlinkTypedTableFunctionSqlHarnessTest.valueType(name);
    var resultType = typedKey ? DataTypes.INT() : valueType;
    var type =
        DataTypes.ROW(
            DataTypes.FIELD("k", typedKey ? valueType : DataTypes.INT()),
            DataTypes.FIELD("v", resultType));
    Object[] values =
        java.util.stream.IntStream.range(0, 8)
            .mapToObj(i -> FlinkTypedTableFunctionSqlHarnessTest.value(name, i))
            .toArray();
    int keys = Integer.getInteger("boundary.keys", 64);
    var env = metrics.environment(new Configuration(), 1, 0);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    var source =
        env.fromSequence(0, Math.max(0, rows - 1))
            .filter(i -> i < rows)
            .map(
                i -> {
                  Object value = values[(int) ((i / 4) % values.length)];
                  return typedKey ? Row.of(value, i.intValue()) : Row.of((int) (i % keys), value);
                })
            .returns(ExternalTypeInfo.<Row>of(type));
    table.createTemporaryView(
        "src", table.fromDataStream(source, Schema.newBuilder().fromRowDataType(type).build()));
    return FlinkTypedJvmAggregateSqlHarnessTest.register(table, resultType);
  }
}

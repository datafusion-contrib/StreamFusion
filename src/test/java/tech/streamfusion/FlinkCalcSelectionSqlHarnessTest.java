package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkCalcSelectionSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(ints = {0, 1, 10, 50, 100})
  void selectiveWidePayloadsKeepValuesAndTypes(int percent) throws Exception {
    compare(
        "SELECT id, SUBSTRING(payload, 1, 32), amount + CAST(1 AS DECIMAL(20,2)) "
            + "FROM src WHERE MOD(id, 100) < "
            + percent);
    compare("SELECT SUBSTRING(payload, -3, 2) FROM src WHERE MOD(id, 100) < " + percent);
  }

  @Test
  void sharedInputsAndNestedConsumersKeepExistingPath() throws Exception {
    compare("SELECT SUBSTRING(payload, -3) FROM src WHERE MOD(id, 3) = 0");
    compare("SELECT payload, SUBSTRING(payload, 1, 32) FROM src WHERE MOD(id, 3) = 0");
    compare(
        "SELECT UPPER(SUBSTRING(payload, 1, 32)), SUBSTRING(payload, id, 2) FROM src WHERE MOD(id,"
            + " 3) = 0");
  }

  @Test
  void rejectedRowsDoNotEvaluateFailingScalar() throws Exception {
    compare("SELECT SUBSTRING(payload, 1, 32), CAST(text_value AS INT) FROM src WHERE" + " id < 0");
    compare(
        "SELECT SUBSTRING(payload, 1, 32), CAST(text_value AS INT) FROM src WHERE"
            + " MOD(id, 2) = 0");
    compare(
        "SELECT SUBSTRING(payload, 1, 32), CASE WHEN MOD(id, 2) = 0 THEN CAST(text_value AS INT)"
            + " ELSE -1 END FROM src WHERE MOD(id, 3) = 0");
  }

  @Test
  void coalesceRetainsReleasedHostEvaluation() {
    assertMatchingFailure(
        "SELECT SUBSTRING(payload, 1, 32), COALESCE(CASE WHEN MOD(id, 2) = 0 THEN CAST(NULL AS INT)"
            + " ELSE id END, CAST(text_value AS INT)) FROM src WHERE id > 0");
  }

  @Test
  void retainedFailureStillMatchesHost() {
    assertMatchingFailure(
        "SELECT SUBSTRING(payload, 1, 32), CAST(text_value AS INT) FROM src WHERE id = 1");
  }

  private static void assertMatchingFailure(String sql) {
    var result = NativeFailureParity.run(FlinkCalcSelectionSqlHarnessTest::environment, sql);
    assertTrue(result.host().failure() != null, result.toString());
    result.assertFailure(
        result.host().rootCause().getClass(),
        "bad",
        NativeFailureParity.Phase.ROW_EVALUATION,
        NativeFailureParity.Route.NATIVE);
  }

  @Test
  void downstreamNativeAggregateConsumesSelectedResults() throws Exception {
    String sql =
        "SELECT SUM(amount + CAST(1 AS DECIMAL(20,2))), "
            + "MIN(SUBSTRING(payload, 1, 32)) FROM src WHERE MOD(id, 100) < 50";
    String plan = NativePlanner.explain(environment(), sql);
    assertTrue(plan.contains("NativeCalc"), plan);
    assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
    NativeParity.assertChangelogParity(FlinkCalcSelectionSqlHarnessTest::environment, sql);
  }

  private static void compare(String sql) throws Exception {
    var host = environment();
    var nativeTables = environment();
    NativePlanner.install(nativeTables);
    assertEquals(
        host.sqlQuery(sql).getResolvedSchema().getColumnDataTypes(),
        nativeTables.sqlQuery(sql).getResolvedSchema().getColumnDataTypes());
    String plan = NativePlanner.explain(environment(), sql);
    assertTrue(plan.contains("NativeCalc"), plan);
    NativeParity.assertParity(FlinkCalcSelectionSqlHarnessTest::environment, sql);
  }

  private static TableEnvironment environment() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var tables = StreamTableEnvironment.create(env);
    Row[] rows = new Row[256];
    for (int i = 0; i < rows.length; i++) {
      rows[i] =
          Row.of(
              i % 13 == 0 ? null : i,
              i % 7 == 0 ? null : (i % 2 == 0 ? "abc".repeat(1400) : "é中🙂".repeat(500)),
              i % 11 == 0 ? null : BigDecimal.valueOf(i * 17L, 2),
              i % 2 == 0 ? "42" : "bad");
    }
    tables.createTemporaryView(
        "src",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "payload", "amount", "text_value"},
                Types.INT,
                Types.STRING,
                Types.BIG_DEC,
                Types.STRING),
            rows),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("payload", DataTypes.STRING())
            .column("amount", DataTypes.DECIMAL(20, 2))
            .column("text_value", DataTypes.STRING())
            .build());
    return tables;
  }
}

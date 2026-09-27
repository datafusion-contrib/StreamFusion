package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkDecimalFloorCeilSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(ints = {12, 18, 19, 20, 38})
  void scalarConsumersKeepFlinksValueAndDeclaredType(int precision) throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> environment(precision),
        "SELECT CAST(FLOOR(d) AS STRING), CAST(CEIL(d) AS STRING), "
            + "CAST(CEILING(d) AS STRING), CAST(FLOOR(d) AS BIGINT), "
            + "CAST(CEIL(d) AS DECIMAL(20,1)), FLOOR(d) + CAST(1 AS DECIMAL(20,0)), "
            + "FLOOR(d) < CEIL(d) FROM src");
  }

  @Test
  void precision38ConsumersRetainLargeValuesAndNulls() throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> BuiltinFunctionParity.environment(ROW(FIELD("d", DECIMAL(38,9))),
            List.of(Row.of(new BigDecimal("99999999999999999999999999999.999999999")),
                Row.of(new BigDecimal("-99999999999999999999999999999.999999999")),
                Row.of(new BigDecimal("0.000000001")), Row.of((Object) null))),
        "SELECT CAST(FLOOR(d) AS STRING), CAST(CEIL(d) AS STRING), "
            + "CAST(FLOOR(d) AS DECIMAL(38,1)), CEIL(d) + CAST(1 AS DECIMAL(38,0)) FROM src");
  }

  @Test
  void selectionsAndPredicatesKeepTheRoundingInsideTheConsumer() throws Exception {
    BuiltinFunctionParity.assertParity(() -> environment(20),
        "SELECT CAST(IF(d < 0, FLOOR(d), CEIL(d)) AS STRING), "
            + "IF(n = 0, FLOOR(d), CEIL(d)), "
            + "CAST(COALESCE(FLOOR(d), CAST(0 AS DECIMAL(20,0))) AS STRING), "
            + "IF(d < 0, CAST(FLOOR(d) AS DECIMAL(21,1)), "
            + "CAST(CEIL(d) AS DECIMAL(21,1))) FROM src WHERE FLOOR(d) <> CEIL(d)");
  }

  @Test
  void unselectedAndFilteredFailuresAreSkipped() throws Exception {
    BuiltinFunctionParity.assertParity(() -> environment(20),
        "SELECT CASE WHEN n = 0 THEN CAST(FLOOR(d) AS STRING) "
            + "ELSE CAST(FLOOR(d / n) AS STRING) END FROM src");
    BuiltinFunctionParity.assertParity(() -> environment(20),
        "SELECT CAST(FLOOR(d / n) AS STRING) FROM src WHERE n = 99");
  }

  @Test
  void selectedDivisionFailureRetainsTheHostError() {
    NativeFailureParity.run(() -> environment(20),
            "SELECT CAST(FLOOR(d / n) AS STRING) FROM src")
        .assertFailure(ArithmeticException.class, "zero", NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @Test
  void nativeCalcEvaluatesConsumersAcrossBatches() throws Exception {
    var reporter = InMemoryReporter.createWithRetainedMetrics();
    var cluster = new MiniClusterResource(new MiniClusterResourceConfiguration.Builder()
        .setConfiguration(reporter.addToConfiguration(new Configuration()))
        .setNumberTaskManagers(1).setNumberSlotsPerTaskManager(2).build());
    cluster.before();
    try {
      var env = new TestStreamEnvironment(cluster.getMiniCluster(), 1);
      var table = StreamTableEnvironment.create(env);
      table.createTemporaryView("src", env.fromSequence(0, 5002)
          .map(i -> Row.of(i % 7 == 0 ? null : BigDecimal.valueOf(i - 2501, 3)))
          .returns(Types.ROW_NAMED(new String[] {"d"}, Types.BIG_DEC)),
          Schema.newBuilder().column("d", DECIMAL(20,3)).build());
      var scan = NativePlanner.install(table);
      var result = table.executeSql("SELECT CAST(FLOOR(d) AS STRING), CAST(CEIL(d) AS STRING) FROM src");
      long count = 0;
      try (var rows = result.collect()) {
        while (rows.hasNext()) {
          BigDecimal value = count % 7 == 0 ? null : BigDecimal.valueOf(count - 2501, 3);
          assertEquals(Row.of(value == null ? null : value.setScale(0, RoundingMode.FLOOR).toString(),
              value == null ? null : value.setScale(0, RoundingMode.CEILING).toString()), rows.next());
          count++;
        }
      }
      assertEquals(5003, count);
      assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
      var groups = reporter.findOperatorMetricGroups(
          result.getJobClient().orElseThrow().getJobID(), "(?i)NativeCalcExecNode");
      assertEquals(1, groups.size());
      var metrics = reporter.getMetricsByGroup(groups.iterator().next());
      assertEquals(5003, ((Counter) metrics.get("numRecordsIn")).getCount());
      assertEquals(5003, ((Counter) metrics.get("numRecordsOut")).getCount());
    } finally {
      cluster.after();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"FLOOR(d)", "CEIL(d)", "ABS(FLOOR(d))",
      "COALESCE(FLOOR(d), CAST(0 AS DECIMAL(20,0)))",
      "FLOOR(d), JSON_VALUE(CAST(n AS STRING), '$')"})
  void unnormalizedDecimalResultsRetainTheHostWriterFailure(String expression) {
    var result = NativeFailureParity.run(() -> environment(20), "SELECT " + expression + " FROM src");
    result.assertFailure(AssertionError.class, "", NativeFailureParity.Phase.ROW_EVALUATION,
        NativeFailureParity.Route.FALLBACK);
    assertTrue(result.nativeRun().fallbackReasons().stream()
        .anyMatch(reason -> reason.contains("value-dependent precision")), result.toString());
    assertTrue(List.of(result.host().rootCause().getStackTrace()).stream()
        .anyMatch(frame -> frame.getClassName().contains("AbstractBinaryWriter")), result.toString());
  }

  private static TableEnvironment environment(int precision) {
    return BuiltinFunctionParity.environment(
        ROW(FIELD("d", DECIMAL(precision,3)), FIELD("n", INT())),
        List.of(Row.of(new BigDecimal("123.456"), 0),
            Row.of(new BigDecimal("-123.456"), 1), Row.of(new BigDecimal("12.000"), 1),
            Row.of(new BigDecimal("-0.001"), 0), Row.of(BigDecimal.ZERO.setScale(3), 1),
            Row.of(null, 0)));
  }
}

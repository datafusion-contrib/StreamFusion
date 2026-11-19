package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import tech.streamfusion.planner.NativePlanner;

class FlinkDecimalSignSqlHarnessTest {
  private static final int[] PRECISIONS = IntStream.concat(
      IntStream.rangeClosed(1, 18), IntStream.of(19, 38)).toArray();
  private static final InMemoryReporter REPORTER = InMemoryReporter.createWithRetainedMetrics();
  private static final MiniClusterResource CLUSTER = new MiniClusterResource(
      new MiniClusterResourceConfiguration.Builder()
          .setConfiguration(REPORTER.addToConfiguration(new Configuration()))
          .setNumberTaskManagers(1).setNumberSlotsPerTaskManager(2).build());

  @BeforeAll
  static void startCluster() throws Exception {
    CLUSTER.before();
  }

  @AfterAll
  static void stopCluster() {
    CLUSTER.after();
  }

  @Test
  void stringConsumersPreserveEveryCompactZeroIntegerWidthResult() throws Exception {
    var rows = assertParity("SELECT " + expressions("CAST(SIGN(d%s) AS STRING)") + " FROM src", 4);
    for (int column = 0; column < PRECISIONS.length; column++) {
      int precision = PRECISIONS[column];
      assertEquals(precision <= 18 ? "1." + "0".repeat(precision) : null, rows.get(0).getField(column));
      assertEquals(precision <= 18 ? "-1." + "0".repeat(precision) : null, rows.get(1).getField(column));
      assertEquals("0." + "0".repeat(precision), rows.get(2).getField(column));
      assertEquals(null, rows.get(3).getField(column));
    }
  }

  @Test
  void directResultsRemainIndependentOfConsumerFusion() throws Exception {
    assertParity("SELECT " + expressions("SIGN(d%s)") + " FROM src", 4);
  }

  @Test
  void nullPredicatesRemainIndependentOfStringConsumers() throws Exception {
    assertParity("SELECT " + expressions("SIGN(d%s) IS NULL") + " FROM src", 4);
  }

  @Test
  void characterTargetsKeepReleasedFormattingAndLimits() throws Exception {
    assertParity("SELECT CAST(SIGN(d1) AS CHAR(5)), CAST(SIGN(d18) AS VARCHAR(4)) FROM src", 4);
  }

  @Test
  void tryCastRetainsItsExistingFallback() throws Exception {
    NativeParity.assertFallbackReasonContains(() -> environment(4),
        "SELECT TRY_CAST(SIGN(d1) AS STRING), TRY_CAST(SIGN(d19) AS STRING) FROM src",
        "unsupported TRY_CAST DECIMAL");
  }

  @Test
  void selectionsAndPredicatesPreserveSignBeforeNormalization() throws Exception {
    assertParity("SELECT CAST(CASE WHEN id < 2 THEN SIGN(d1) ELSE d1 END AS STRING), "
        + "CAST(COALESCE(SIGN(d18), d18) AS STRING), "
        + "CAST(IF(id < 2, SIGN(d1), d1) AS STRING), "
        + "CAST(SIGN(d1) AS STRING) = '1.0', "
        + "CAST(SIGN(d18) AS STRING) IS NOT NULL "
        + "FROM src WHERE CAST(SIGN(d1) AS STRING) <> '-1.0'", 4);
  }

  @Test
  void unselectedAndAllFilteredFailuresAreSkipped() throws Exception {
    assertParity("SELECT CASE WHEN id >= 0 THEN CAST(SIGN(d1) AS STRING) "
        + "ELSE CAST(SIGN(d1 / (id - id)) AS STRING) END FROM src", 4);
    assertEquals(List.of(), assertParity("SELECT CAST(SIGN(d1 / (id - id)) AS STRING) "
        + "FROM src WHERE id < 0", 4));
    assertParity("SELECT id >= 0 OR CAST(SIGN(d1 / (id - id)) AS STRING) = '1.0', "
        + "id < 0 AND CAST(SIGN(d1 / (id - id)) AS STRING) = '1.0' FROM src", 4);
  }

  @Test
  void selectedFailuresRetainTheHostError() {
    NativeFailureParity.run(() -> environment(4),
        "SELECT CAST(SIGN(d1 / (id - id)) AS STRING) FROM src")
        .assertFailure(ArithmeticException.class, "zero", NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @Test
  void nativeCalcConsumesMultipleBatches() throws Exception {
    assertEquals(5003, assertParity("SELECT CAST(SIGN(d1) AS STRING), "
        + "CAST(SIGN(d18) AS STRING), CAST(SIGN(d19) AS STRING) FROM src", 5003).size());
  }

  private static String expressions(String template) {
    return IntStream.of(PRECISIONS).mapToObj(p -> String.format(template, p))
        .collect(Collectors.joining(", "));
  }

  private static TableEnvironment environment(int count) {
    var fields = new ArrayList<org.apache.flink.table.api.DataTypes.Field>();
    fields.add(FIELD("id", INT().notNull()));
    for (int precision : PRECISIONS) fields.add(FIELD("d" + precision, DECIMAL(precision, precision)));
    DataType type = ROW(fields.toArray(new org.apache.flink.table.api.DataTypes.Field[0]));
    var env = new TestStreamEnvironment(CLUSTER.getMiniCluster(), 1);
    var table = StreamTableEnvironment.create(env);
    table.createTemporaryView("src", env.fromSequence(0, count - 1)
        .map(i -> {
          Row row = new Row(PRECISIONS.length + 1);
          row.setField(0, i.intValue());
          BigDecimal value = i % 4 == 3 ? null : BigDecimal.valueOf(i % 4 == 0 ? 1 : i % 4 == 1 ? -1 : 0, 1);
          for (int column = 0; column < PRECISIONS.length; column++)
            row.setField(column + 1, value == null ? null : value.setScale(PRECISIONS[column]));
          return row;
        }).returns(ExternalTypeInfo.<Row>of(type)), Schema.newBuilder().fromRowDataType(type).build());
    return table;
  }

  private static List<Row> assertParity(String sql, int count) throws Exception {
    var host = environment(count);
    var candidate = environment(count);
    var scan = NativePlanner.install(candidate);
    assertEquals(host.sqlQuery(sql).getResolvedSchema(), candidate.sqlQuery(sql).getResolvedSchema());
    List<Row> expected = collect(host.executeSql(sql));
    var result = candidate.executeSql(sql);
    List<Row> actual = collect(result);
    assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
    assertFalse(scan.functionExecutions().isEmpty(), scan.explainSummary());
    assertTrue(scan.functionExecutions().stream().anyMatch(route -> route.contains("SIGN")),
        scan.explainSummary());
    var groups = REPORTER.findOperatorMetricGroups(result.getJobClient().orElseThrow().getJobID(),
        "(?i)NativeCalc\\(");
    assertFalse(groups.isEmpty(), scan.explainSummary());
    long inputRows = groups.stream().mapToLong(group ->
        ((Counter) REPORTER.getMetricsByGroup(group).get("numRecordsIn")).getCount()).sum();
    assertEquals(count, inputRows, scan.explainSummary());
    assertEquals(expected, actual, sql);
    return actual;
  }

  private static List<Row> collect(org.apache.flink.table.api.TableResult result) throws Exception {
    var rows = new ArrayList<Row>();
    try (var iterator = result.collect()) {
      while (iterator.hasNext()) rows.add(iterator.next());
    }
    return rows;
  }
}

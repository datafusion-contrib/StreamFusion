package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkTimestampLtzIfSqlHarnessTest {
  static Stream<Arguments> precisionsAndZones() {
    return Stream.of("UTC", "Asia/Shanghai", "America/Los_Angeles")
        .flatMap(zone -> Stream.of(0, 3, 6, 9).map(p -> Arguments.of(p, zone)));
  }

  @ParameterizedTest
  @MethodSource("precisionsAndZones")
  void nullableBranchesPreserveInstantsAndResolvedTypes(int precision, String zone)
      throws Exception {
    String sql = "SELECT IF(c,a,b), IF(c, CAST(NULL AS TIMESTAMP_LTZ(" + precision
        + ")), b), IF(c, IF(n = 0, a, b), a), CASE WHEN c THEN a ELSE b END, "
        + "COALESCE(a,b), IFNULL(a,b) FROM src";
    var types = environment(precision, zone).sqlQuery(sql).getResolvedSchema().getColumnDataTypes();
    assertTrue(types.stream().allMatch(type -> type.equals(TIMESTAMP_LTZ(precision))),
        types.toString());
    BuiltinFunctionParity.assertParity(() -> environment(precision, zone), sql);
  }

  @ParameterizedTest
  @ValueSource(strings = {"UTC", "Asia/Shanghai", "America/Los_Angeles"})
  void mixedPrecisionsUseFlinksBranchCasts(String zone) throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> environment(9, zone),
        "SELECT IF(c, CAST(a AS TIMESTAMP_LTZ(3)), b), "
            + "IF(c, a, CAST(b AS TIMESTAMP_LTZ(6))) FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings = {"UTC", "Asia/Shanghai", "America/Los_Angeles"})
  void unselectedAndFilteredFailingBranchesAreSkipped(String zone) throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> environment(3, zone),
        "SELECT IF(n = 0, a, TO_TIMESTAMP_LTZ(1000 / n, 3)), "
            + "IF(n <> 0, TO_TIMESTAMP_LTZ(1000 / n, 3), b) FROM src");
    BuiltinFunctionParity.assertParity(
        () -> environment(3, zone),
        "SELECT IF(c, TO_TIMESTAMP_LTZ(1000 / n, 3), b) FROM src WHERE n = 99");
  }

  @Test
  void selectedFailingBranchRetainsTheHostError() {
    NativeFailureParity.run(
            () -> environment(3, "UTC"),
            "SELECT IF(n = 0, TO_TIMESTAMP_LTZ(1000 / n, 3), b) FROM src")
        .assertFailure(ArithmeticException.class, "zero",
            NativeFailureParity.Phase.ROW_EVALUATION, NativeFailureParity.Route.NATIVE);
  }

  @Test
  void nativeCalcProcessesAndEmitsMultipleBatches() throws Exception {
    var reporter = InMemoryReporter.createWithRetainedMetrics();
    var cluster = new MiniClusterResource(
        new MiniClusterResourceConfiguration.Builder()
            .setConfiguration(reporter.addToConfiguration(new Configuration()))
            .setNumberTaskManagers(1)
            .setNumberSlotsPerTaskManager(2)
            .build());
    cluster.before();
    try {
      var table = environment(9, "America/Los_Angeles", 5003,
          new TestStreamEnvironment(cluster.getMiniCluster(), 1));
      var scan = NativePlanner.install(table);
      var result = table.executeSql("SELECT IF(c,a,b) FROM src");
      long count = 0;
      try (var rows = result.collect()) {
        while (rows.hasNext()) {
          Row input = inputRow((int) count++);
          assertEquals(Row.of(Boolean.TRUE.equals(input.getField(0))
              ? input.getField(1) : input.getField(2)), rows.next());
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

  private static TableEnvironment environment(int precision, String zone) {
    return environment(precision, zone, 18, StreamExecutionEnvironment.getExecutionEnvironment());
  }

  private static TableEnvironment environment(int precision, String zone, int count,
      StreamExecutionEnvironment env) {
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneId.of(zone));
    var type = ROW(FIELD("c", BOOLEAN()), FIELD("a", TIMESTAMP_LTZ(precision)),
        FIELD("b", TIMESTAMP_LTZ(precision)), FIELD("n", INT()));
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < count; i++) rows.add(inputRow(i));
    table.createTemporaryView("src", fromData(env, rows, ExternalTypeInfo.<Row>of(type)),
        Schema.newBuilder().fromRowDataType(type).build());
    return table;
  }

  private static Row inputRow(int i) {
    Instant[] values = {
        Instant.parse("1969-12-31T23:59:59.999999999Z"),
        Instant.parse("2024-03-10T09:59:59.123456789Z"),
        Instant.parse("2024-03-10T10:00:00Z"),
        Instant.parse("2024-11-03T08:30:00Z"),
        Instant.parse("2024-11-03T09:30:00Z"),
        null
    };
    Boolean[] conditions = {true, false, null};
    return Row.of(conditions[i / values.length % conditions.length], values[i % values.length],
        values[(i + 3) % values.length], i % 2);
  }
}

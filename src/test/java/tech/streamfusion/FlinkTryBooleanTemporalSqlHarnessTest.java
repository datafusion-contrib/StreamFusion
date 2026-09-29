package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkTryBooleanTemporalSqlHarnessTest {
  private static final String[] VALUES = {
      "true", "FALSE", "t", "f", "yes", "NO", "1", "0", " true", "false ", "",
      "bad", "tr\u0000ue", "\u662f", null,
      "2000-2-29", "1969-1-2", "12:34:56.1", "23:59:59.12",
      "2024-02-29", "2023-02-29", "2024-13-01", "0001-01-01", "9999-12-31",
      "10000-01-01", "-0001-01-01", "12:34:56.123456789", "00:00:00", "23:59:59.999",
      "12:60:00", "1969-12-31 23:59:59.999999999",
      "2024-03-10 01:59:59.123456789", "2024-03-10 02:30:00", "2024-03-10 03:00:00",
      "2024-11-03 01:30:00", "2024-02-29 12:34:56.123456789", "2024-02-29T12:34:56Z",
      "0000-01-01 00:00:00", "0001-01-01 00:00:00", "9999-12-31 23:59:59.999999999",
      "2023-02-29 00:00:00", "2024-02-30 00:00:00", "2024-02-29 24:00:00",
      "2024-2-29 1:2:3", "2024-02-29 12:34:56.", "2024-02-29 12:34:56.1234567890"
  };

  static Stream<Arguments> temporalTypesAndZones() {
    return Stream.of("UTC", "Asia/Shanghai", "America/Los_Angeles")
        .flatMap(zone -> Stream.of("DATE", "TIME(0)", "TIME(3)",
            "TIMESTAMP(0)", "TIMESTAMP(3)", "TIMESTAMP(6)", "TIMESTAMP(9)",
            "TIMESTAMP_LTZ(0)", "TIMESTAMP_LTZ(3)", "TIMESTAMP_LTZ(6)", "TIMESTAMP_LTZ(9)")
            .map(type -> Arguments.of(type, zone)));
  }

  @ParameterizedTest
  @MethodSource("temporalTypesAndZones")
  void directAndComposedTemporalFastPathsMatchReleasedFlink(String type, String zone) throws Exception {
    for (String input : List.of("s", "TRIM(s)")) {
      BuiltinFunctionParity.assertParity(() -> environment(zone, false),
          "SELECT TRY_CAST(" + input + " AS " + type + ") FROM src");
    }
  }

  @Test
  void composedTemporalCastEvaluatesItsStatefulChildOnce() throws Exception {
    BuiltinFunctionParity.assertParity(() -> {
      var table = environment("UTC", false);
      table.createTemporarySystemFunction("marked_text", MarkedText.class);
      return table;
    }, "SELECT TRY_CAST(marked_text(s) AS DATE) FROM src");
  }

  public static final class MarkedText extends org.apache.flink.table.functions.ScalarFunction {
    private int calls;
    public String eval(String text) { return ++calls % 2 == 0 ? "2000-02-29" : text; }
  }

  @ParameterizedTest
  @MethodSource("temporalTypesAndZones")
  void temporalValuesTypesAndMalformedNullsMatchFlink(String type, String zone) throws Exception {
    BuiltinFunctionParity.assertParity(() -> environment(zone, false),
        "SELECT id, TRY_CAST(s AS " + type + "), TRY_CAST(s AS " + type + ") IS NULL FROM src");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void booleanSpellingsAndConsumersMatchFlink(boolean legacy) throws Exception {
    BuiltinFunctionParity.assertParity(() -> environment("UTC", legacy),
        "SELECT id, TRY_CAST(s AS BOOLEAN), NOT TRY_CAST(s AS BOOLEAN), "
            + "COALESCE(TRY_CAST(s AS BOOLEAN), FALSE), "
            + "s IS NULL OR TRY_CAST(s AS BOOLEAN) FROM src");
    BuiltinFunctionParity.assertParity(() -> environment("UTC", legacy),
        "SELECT id FROM src WHERE TRY_CAST(s AS BOOLEAN)");
  }

  @Test
  void nonNullAndFixedCharacterInputsStillProduceNullableResults() throws Exception {
    var type = ROW(FIELD("s", CHAR(5).notNull()));
    var rows = List.of(Row.of("true"), Row.of("bad"));
    String sql = "SELECT TRY_CAST(s AS BOOLEAN), TRY_CAST(s AS DATE) FROM src";
    var table = BuiltinFunctionParity.environment(type, rows);
    assertEquals(List.of(BOOLEAN(), DATE()), table.sqlQuery(sql).getResolvedSchema().getColumnDataTypes());
    BuiltinFunctionParity.assertParity(() -> BuiltinFunctionParity.environment(type, rows), sql);
  }

  @ParameterizedTest
  @ValueSource(strings = {"BOOLEAN", "DATE", "TIME(3)", "TIMESTAMP(9)", "TIMESTAMP_LTZ(9)"})
  void tryCastDoesNotSwallowChildFailures(String type) {
    NativeFailureParity.run(() -> environment("UTC", false),
            "SELECT TRY_CAST(CAST(1 / (id - id) AS STRING) AS " + type + ") FROM src")
        .assertFailure(ArithmeticException.class, "zero", NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @ParameterizedTest
  @ValueSource(strings = {"BOOLEAN", "DATE", "TIME(3)", "TIMESTAMP(9)", "TIMESTAMP_LTZ(9)"})
  void unselectedAndFilteredChildFailuresAreSkipped(String type) throws Exception {
    String failing = "TRY_CAST(CAST(1 / (id - id) AS STRING) AS " + type + ")";
    BuiltinFunctionParity.assertParity(() -> environment("UTC", false),
        "SELECT CASE WHEN id >= 0 THEN TRY_CAST(s AS " + type + ") ELSE " + failing + " END FROM src");
    BuiltinFunctionParity.assertParity(() -> environment("UTC", false),
        "SELECT " + failing + " FROM src WHERE id < 0");
  }

  @ParameterizedTest
  @ValueSource(strings = {"DATE", "TIME(3)", "TIMESTAMP(9)", "TIMESTAMP_LTZ(9)"})
  void legacyTemporalModeStillMatchesReleasedFlink(String type) throws Exception {
    BuiltinFunctionParity.assertParity(() -> environment("America/Los_Angeles", true),
        "SELECT TRY_CAST(s AS " + type + ") FROM src");
  }

  @Test
  void outOfRangeTimePreservesReleasedFlinkCollectionBehavior() {
    var result = NativeFailureParity.run(
        () -> BuiltinFunctionParity.environment(ROW(FIELD("s", STRING())),
            List.of(Row.of("24:00:00"))), "SELECT TRY_CAST(s AS TIME(3)) FROM src");
    if (tech.streamfusion.compat.FlinkTestCapabilities.TIME_TRY_CAST_REJECTS_OUT_OF_RANGE) {
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(null, result.host().rows().get(0).get(1));
    } else {
      result.assertFailure(java.time.DateTimeException.class, "HourOfDay",
          NativeFailureParity.Phase.COLLECTION, NativeFailureParity.Route.NATIVE);
    }
  }

  @Test
  void nativeAndGeneratedConversionsProcessMultipleBatches() throws Exception {
    var reporter = InMemoryReporter.createWithRetainedMetrics();
    var cluster = new MiniClusterResource(new MiniClusterResourceConfiguration.Builder()
        .setConfiguration(reporter.addToConfiguration(new Configuration()))
        .setNumberTaskManagers(1).setNumberSlotsPerTaskManager(2).build());
    cluster.before();
    try {
      var env = new TestStreamEnvironment(cluster.getMiniCluster(), 1);
      var table = StreamTableEnvironment.create(env);
      table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
      table.createTemporaryView("src", env.fromSequence(0, 5002)
          .map(i -> switch ((int) (i % 4)) {
            case 0 -> Row.of("yes", "1969-12-31 23:59:59.999999999");
            case 1 -> Row.of("NO", "2024-03-10 02:30:00.123456789");
            case 2 -> Row.of("invalid", "bad");
            default -> Row.of(null, null);
          }).returns(Types.ROW_NAMED(new String[] {"b", "s"}, Types.STRING, Types.STRING)));
      var scan = NativePlanner.install(table);
      var result = table.executeSql("SELECT TRY_CAST(b AS BOOLEAN), "
          + "TRY_CAST(s AS TIMESTAMP(9)), TRY_CAST(s AS TIMESTAMP_LTZ(9)) FROM src");
      var beforeEpoch = LocalDateTime.parse("1969-12-31T23:59:59.999999999");
      var spring = LocalDateTime.parse("2024-03-10T02:30:00.123456789");
      List<Row> expected = List.of(Row.of(true, beforeEpoch, beforeEpoch.toInstant(ZoneOffset.UTC)),
          Row.of(false, spring, spring.toInstant(ZoneOffset.UTC)), Row.of(null, null, null),
          Row.of(null, null, null));
      long count = 0;
      try (var rows = result.collect()) {
        while (rows.hasNext()) assertEquals(expected.get((int) (count++ % 4)), rows.next());
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

  private static TableEnvironment environment(String zone, boolean legacy) {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < VALUES.length; i++) rows.add(Row.of(i, VALUES[i]));
    var table = BuiltinFunctionParity.environment(ROW(FIELD("id", INT()), FIELD("s", STRING())), rows, zone);
    if (legacy) table.getConfig().getConfiguration().setString("table.exec.legacy-cast-behaviour", "ENABLED");
    return table;
  }
}

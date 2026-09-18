package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.SqlAuditHarness.Comparison.*;
import static tech.streamfusion.SqlAuditHarness.Kind.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.SqlAuditHarness.Spec;

@org.junit.jupiter.api.parallel.Execution(org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD)
class FlinkPortableSqlAuditTest {
  private static final Map<String, Map<String, Object>> RESULTS = new TreeMap<>();
  private static final List<String> JSON_PATHS =
      List.of("$[-1]", "$[-2147483648]", "$[--1]", "$[1 2]", "$[");

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void portableCase(Spec spec) {
    int openedBefore = PortableSqlFixtures.OPENED.get();
    int closedBefore = PortableSqlFixtures.CLOSED.get();
    try (var recovery =
        spec.id().equals("checkpoint-recovery") ? new PortableSqlRecovery() : null) {
      var runs =
          SqlAuditHarness.execute(
              spec,
              recovery == null
                  ? () -> PortableSqlFixtures.environment(spec.mode(), spec.settings())
                  : recovery);
      boolean passed = false;
      try {
        SqlAuditHarness.verify(spec, runs);
        if (recovery != null) recovery.verify();
        if (!spec.functions().isEmpty() && spec.expected() == HOST_FAILURE) {
          assertTrue(
              runs.nativeRun().substitutions() > 0, "failing scalar must still use native Calc");
        }
        if (!spec.functions().isEmpty() && spec.expected() != HOST_FAILURE) {
          int opens = PortableSqlFixtures.OPENED.get() - openedBefore;
          int closes = PortableSqlFixtures.CLOSED.get() - closedBefore;
          assertTrue(opens >= 2, "both executions must open their functions");
          assertEquals(opens, closes, "every opened fixture must close");
        }
        passed = true;
      } finally {
        var record = SqlAuditHarness.record(spec, runs, passed);
        if (recovery != null) {
          record.put("checkpointRecovery", recovery.observations());
          record.put(
              "recoveryConfiguration",
              Map.of("checkpointIntervalMillis", 50, "restartAttempts", 1));
        }
        assertNull(RESULTS.put(spec.id(), record), "duplicate case id");
      }
    }
  }

  static Stream<Spec> cases() {
    List<Spec> cases = new ArrayList<>();
    cases.add(
        success(
            "scalar",
            "SELECT id, test_scalar(v) FROM test_input",
            "test_input",
            "test_scalar",
            NATIVE_PARITY,
            "",
            List.of(row(1, 31L), row(2, 61L), row(3, null), row(4, 25L), row(5, -8L))));
    cases.add(
        success(
            "nested-scalar",
            "SELECT id, test_nested(nested) FROM nested_input",
            "nested_input",
            "test_nested",
            EXPLICIT_FALLBACK,
            "UDF return type not native: ROW",
            List.of(
                row(1, org.apache.flink.types.Row.of(17L, new String[] {"a", null, "b"})),
                row(2, org.apache.flink.types.Row.of(null, new String[0])),
                row(3, null))));
    cases.add(
        success(
            "table-function",
            "SELECT s.id, t.v, t.pos FROM test_input s, LATERAL TABLE(test_split(s.text_value)) AS"
                + " t(v, pos)",
            "test_input",
            "test_split",
            EXPLICIT_FALLBACK,
            "correlate:",
            List.of(
                row(1, "red", 0),
                row(1, "blue", 1),
                row(3, "", 0),
                row(4, "blue", 0),
                row(4, "", 1),
                row(4, "red", 2),
                row(5, "x", 0))));
    cases.add(
        success(
            "aggregate-function",
            "SELECT k, test_aggregate(v) FROM test_input GROUP BY k",
            "test_input",
            "test_aggregate",
            EXPLICIT_FALLBACK,
            "only SUM/MIN/MAX/COUNT/AVG/FIRST_VALUE/LAST_VALUE/SINGLE_VALUE aggregates",
            List.of(row("A", 27L), row("B", 8L))));
    cases.add(
        success(
            "cdc-aggregate-function",
            "SELECT k, test_aggregate(v) FROM cdc_input GROUP BY k",
            "cdc_input",
            "test_aggregate",
            EXPLICIT_FALLBACK,
            "only SUM/MIN/MAX/COUNT/AVG/FIRST_VALUE/LAST_VALUE/SINGLE_VALUE aggregates",
            List.of(row("A", 12L), row("B", 8L))));
    cases.add(
        success(
            "cdc-native-sum",
            "SELECT k, SUM(v) FROM cdc_input GROUP BY k",
            "cdc_input",
            "",
            NATIVE_PARITY,
            "",
            List.of(row("A", 12L), row("B", 8L))));
    cases.add(
        new Spec(
            "cdc-scalar-row-kinds",
            RuntimeExecutionMode.STREAMING,
            Map.of(),
            "SELECT id, k, test_scalar(v) FROM cdc_input",
            Set.of("cdc_input"),
            Set.of("test_scalar"),
            NATIVE_PARITY,
            "",
            null,
            ORDERED_CHANGELOG,
            List.of(row(1, "A", 37L), row(4, "B", 25L))));
    cases.add(
        success(
            "scan-only", "SELECT * FROM test_input", "test_input", "", SCAN_ONLY, "", List.of()));
    cases.add(
        failure(
            "open-failure",
            "SELECT test_fail_open(v) FROM test_input",
            "test_fail_open",
            "portable fixture open failure",
            NativeFailureParity.Phase.INITIALIZATION));
    cases.add(
        failure(
            "evaluation-failure",
            "SELECT test_fail_eval(v) FROM test_input",
            "test_fail_eval",
            "portable fixture negative value",
            NativeFailureParity.Phase.ROW_EVALUATION));

    var parameters = new LinkedHashMap<String, List<String>>();
    parameters.put("JSON_PATH", JSON_PATHS);
    parameters.put("MODE", List.of("lax", "strict"));
    for (var variant : SqlAuditHarness.expand(parameters)) {
      String path = variant.get("JSON_PATH");
      String sql =
          SqlAuditHarness.render(
              "SELECT id, JSON_VALUE(doc, '${MODE} ${JSON_PATH}' DEFAULT 'empty' ON EMPTY DEFAULT"
                  + " 'error' ON ERROR) FROM test_input",
              variant);
      cases.add(
          success(
              "json/" + variant.get("MODE") + "/" + path,
              sql,
              "test_input",
              "",
              NATIVE_PARITY,
              "",
              List.of()));
    }

    cases.add(
        failure(
            "first-value-original-signature",
            "SELECT k, FIRST_VALUE(v, ord) FROM test_input GROUP BY k",
            "",
            "FIRST_VALUE",
            NativeFailureParity.Phase.PLANNING));
    cases.add(
        keyed(
            success(
                "first-value-declared-order",
                "SELECT k, v FROM (SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY ord, id)"
                    + " rn FROM test_input WHERE v IS NOT NULL) WHERE rn = 1",
                "test_input",
                "",
                NATIVE_PARITY,
                "",
                List.of(row("A", 20L), row("B", 8L)))));
    cases.add(
        keyed(
            success(
                "first-value-reversed-tie",
                "SELECT k, v FROM (SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY ord, id"
                    + " DESC) rn FROM test_input WHERE v IS NOT NULL) WHERE rn = 1",
                "test_input",
                "",
                NATIVE_PARITY,
                "",
                List.of(row("A", -3L), row("B", 8L)))));
    cases.add(
        failure(
            "timestamp-original-signature",
            "SELECT TO_TIMESTAMP(ts_text, 'yyyy-MM-dd HH:mm:ss', 'UTC') FROM test_input",
            "",
            "TO_TIMESTAMP",
            NativeFailureParity.Phase.PLANNING));
    cases.add(
        failure(
            "date-format-original-signature",
            "SELECT DATE_FORMAT(TO_TIMESTAMP(ts_text), 'yyyy-MM-dd HH:mm:ss', 'UTC') FROM"
                + " test_input",
            "",
            "DATE_FORMAT",
            NativeFailureParity.Phase.PLANNING));
    for (String zone : List.of("UTC", "America/Los_Angeles")) {
      cases.add(
          withSettings(
              success(
                  "wall-time/" + zone,
                  "SELECT id, TO_TIMESTAMP(ts_text, 'yyyy-MM-dd HH:mm:ss') FROM test_input",
                  "test_input",
                  "",
                  NATIVE_PARITY,
                  "",
                  List.of()),
              Map.of("table.local-time-zone", zone)));
      cases.add(
          withSettings(
              success(
                  "instant/" + zone,
                  "SELECT id, CAST(TO_TIMESTAMP(ts_text, 'yyyy-MM-dd HH:mm:ss') AS"
                      + " TIMESTAMP_LTZ(3)) FROM test_input",
                  "test_input",
                  "",
                  NATIVE_PARITY,
                  "",
                  List.of()),
              Map.of("table.local-time-zone", zone)));
      cases.add(
          withSettings(
              success(
                  "format-instant/" + zone,
                  "SELECT id, DATE_FORMAT(TO_TIMESTAMP_LTZ(v * 1000, 3), 'yyyy-MM-dd HH:mm:ss')"
                      + " FROM test_input",
                  "test_input",
                  "",
                  NATIVE_PARITY,
                  "",
                  List.of()),
              Map.of("table.local-time-zone", zone)));
    }
    cases.add(
        failure(
            "implicit-key-comparison",
            "SELECT l.id FROM test_input l JOIN right_input r ON l.text_key = r.int_key",
            "",
            "implicit type conversion",
            NativeFailureParity.Phase.PLANNING));
    cases.add(
        new Spec(
            "explicit-numeric-key-comparison",
            RuntimeExecutionMode.STREAMING,
            Map.of(),
            "SELECT l.id FROM test_input l JOIN right_input r ON CAST(l.text_key AS INT) ="
                + " r.int_key",
            Set.of("test_input", "right_input"),
            Set.of(),
            NATIVE_PARITY,
            "",
            null,
            MATERIALIZED,
            List.of(row(1), row(2), row(3))));
    cases.add(
        failure(
            "streaming-non-time-sort",
            "SELECT id, ord FROM test_input ORDER BY ord, id",
            "",
            "non-time",
            NativeFailureParity.Phase.PLANNING));
    cases.add(
        new Spec(
            "batch-non-time-sort",
            RuntimeExecutionMode.BATCH,
            Map.of(),
            "SELECT id, ord FROM test_input ORDER BY ord, id",
            Set.of("test_input"),
            Set.of(),
            BATCH_HOST_ONLY,
            "",
            null,
            ORDERED_CHANGELOG,
            List.of(row(2, 10L), row(5, 10L), row(3, 20L), row(1, 30L), row(4, 40L))));
    cases.add(
        new Spec(
            "streaming-updating-over",
            RuntimeExecutionMode.STREAMING,
            Map.of(),
            "SELECT k, SUM(v) OVER (ORDER BY pt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
                + " FROM (SELECT k, SUM(v) v, PROCTIME() pt FROM cdc_input GROUP BY k)",
            Set.of("cdc_input"),
            Set.of(),
            HOST_FAILURE,
            "update",
            NativeFailureParity.Phase.PLANNING,
            MATERIALIZED,
            List.of()));
    cases.add(
        new Spec(
            "checkpoint-recovery",
            RuntimeExecutionMode.STREAMING,
            Map.of(),
            "SELECT k, SUM(v) FROM recovery_input GROUP BY k",
            Set.of("recovery_input"),
            Set.of(),
            NATIVE_PARITY,
            "",
            null,
            MATERIALIZED,
            List.of(row(0, 1488L), row(1, 1520L), row(2, 1552L))));
    assertEquals(
        cases.size(),
        cases.stream().map(Spec::id).distinct().count(),
        "duplicate declared case id");
    return cases.stream();
  }

  private static Spec success(
      String id,
      String sql,
      String table,
      String function,
      SqlAuditHarness.Kind kind,
      String reason,
      List<List<Object>> golden) {
    return new Spec(
        id,
        RuntimeExecutionMode.STREAMING,
        Map.of(),
        sql,
        Set.of(table),
        function.isEmpty() ? Set.of() : Set.of(function),
        kind,
        reason,
        null,
        MATERIALIZED,
        golden);
  }

  private static Spec failure(
      String id, String sql, String function, String reason, NativeFailureParity.Phase phase) {
    return new Spec(
        id,
        RuntimeExecutionMode.STREAMING,
        Map.of(),
        sql,
        Set.of("test_input"),
        function.isEmpty() ? Set.of() : Set.of(function),
        HOST_FAILURE,
        reason,
        phase,
        MATERIALIZED,
        List.of());
  }

  private static Spec keyed(Spec spec) {
    return new Spec(
        spec.id(),
        spec.mode(),
        spec.settings(),
        spec.sql(),
        spec.tables(),
        spec.functions(),
        spec.expected(),
        spec.reason(),
        spec.failurePhase(),
        KEYED_FIRST_FIELD,
        spec.golden());
  }

  private static Spec withSettings(Spec spec, Map<String, String> settings) {
    return new Spec(
        spec.id(),
        spec.mode(),
        settings,
        spec.sql(),
        spec.tables(),
        spec.functions(),
        spec.expected(),
        spec.reason(),
        spec.failurePhase(),
        spec.comparison(),
        temporalGolden(spec.id(), settings.get("table.local-time-zone")));
  }

  private static List<List<Object>> temporalGolden(String id, String zone) {
    var january = java.time.LocalDateTime.of(2020, 1, 2, 3, 4, 5);
    var july = java.time.LocalDateTime.of(2020, 7, 2, 3, 4, 5);
    if (id.startsWith("wall-time/"))
      return List.of(row(1, january), row(2, july), row(3, null), row(4, january), row(5, january));
    var timezone = java.time.ZoneId.of(zone);
    if (id.startsWith("instant/"))
      return List.of(
          row(1, january.atZone(timezone).toInstant()),
          row(2, july.atZone(timezone).toInstant()),
          row(3, null),
          row(4, january.atZone(timezone).toInstant()),
          row(5, january.atZone(timezone).toInstant()));
    var formatter =
        java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(timezone);
    return List.of(
        row(1, formatter.format(java.time.Instant.ofEpochSecond(10))),
        row(2, formatter.format(java.time.Instant.ofEpochSecond(20))),
        row(3, null),
        row(4, formatter.format(java.time.Instant.ofEpochSecond(8))),
        row(5, formatter.format(java.time.Instant.ofEpochSecond(-3))));
  }

  private static List<Object> row(Object... values) {
    return Arrays.asList(values);
  }

  @Test
  void preflightRejectsUnexpandedParametersAndMissingFixtures() {
    for (String placeholder : List.of("$JSON_PATH", "${JSON_PATH}", "{{JSON_PATH}}")) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              SqlAuditHarness.render(
                  "SELECT JSON_VALUE(doc, '" + placeholder + "') FROM test_input", Map.of()));
    }
    var table = PortableSqlFixtures.environment(RuntimeExecutionMode.STREAMING, Map.of());
    for (Spec missing :
        List.of(
            success(
                "missing-table",
                "SELECT * FROM not_registered",
                "not_registered",
                "",
                SCAN_ONLY,
                "",
                List.of()),
            success(
                "missing-function",
                "SELECT not_registered(v) FROM test_input",
                "test_input",
                "not_registered",
                NATIVE_PARITY,
                "",
                List.of()))) {
      assertThrows(IllegalArgumentException.class, () -> SqlAuditHarness.preflight(missing, table));
    }
    assertThrows(
        IllegalArgumentException.class, () -> SqlAuditHarness.expand(Map.of("PATH", List.of())));
    assertThrows(
        IllegalArgumentException.class,
        () -> SqlAuditHarness.expand(Map.of("PATH", List.of("a", "a"))));
  }

  @AfterAll
  static void writeCoverage() throws Exception {
    List<String> declared = cases().map(Spec::id).toList();
    List<String> unexecuted = declared.stream().filter(id -> !RESULTS.containsKey(id)).toList();
    Map<String, Long> counts = new TreeMap<>();
    RESULTS
        .values()
        .forEach(result -> counts.merge(result.get("outcome").toString(), 1L, Long::sum));
    Map<String, Object> report = new LinkedHashMap<>();
    report.put("corpus", "portable-public-issue-fixtures");
    report.put("originalPrivateAuditAvailable", false);
    report.put(
        "standardFixtureDefaults",
        Map.of(
            "parallelism",
            1,
            "table.local-time-zone",
            "UTC",
            "table.optimizer.agg-phase-strategy",
            "ONE_PHASE"));
    report.put("declaredCases", declared.size());
    report.put("executedCases", RESULTS.size());
    report.put("unexecuted", unexecuted);
    report.put(
        "passedCases",
        RESULTS.values().stream().filter(r -> Boolean.TRUE.equals(r.get("passed"))).count());
    report.put("outcomes", counts);
    report.put("cases", RESULTS.values());
    Path output = Path.of("target", "sql-audit", "portable-cases.json");
    Files.createDirectories(output.getParent());
    new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(output.toFile(), report);
    assertTrue(unexecuted.isEmpty(), "audit cases were not executed: " + unexecuted);
  }
}

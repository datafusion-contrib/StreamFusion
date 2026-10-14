package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import tech.streamfusion.planner.NativePlanner;

class FlinkAttachedWindowOnePhaseSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> assignedBoundsKeepResultsAndBothWindowsExecute() {
    return Stream.of("ONE_PHASE", "AUTO")
        .flatMap(
            phase ->
                Stream.of("TUMBLE", "HOP")
                    .flatMap(
                        window ->
                            Stream.of("UTC", "Asia/Shanghai", "America/Los_Angeles")
                                .flatMap(
                                    zone ->
                                        Stream.of(false, true)
                                            .map(
                                                ltz ->
                                                    DynamicTest.dynamicTest(
                                                        phase + "/" + window + "/" + zone + "/ltz="
                                                            + ltz,
                                                        () ->
                                                            compare(
                                                                phase, window, zone, ltz,
                                                                false))))));
  }

  @TestFactory
  Stream<DynamicTest> cumulativeResultsAlsoKeepTheirAssignedBounds() {
    return Stream.of("UTC", "Asia/Shanghai", "America/Los_Angeles")
        .flatMap(
            zone ->
                Stream.of(false, true)
                    .map(
                        ltz ->
                            DynamicTest.dynamicTest(
                                zone + "/ltz=" + ltz,
                                () -> compare("ONE_PHASE", "CUMULATE", zone, ltz, false))));
  }

  @TestFactory
  Stream<DynamicTest> emptyAttachedWindowsProduceNoRows() {
    return Stream.of("TUMBLE", "HOP")
        .map(
            window ->
                DynamicTest.dynamicTest(
                    window, () -> compare("ONE_PHASE", window, "UTC", false, true)));
  }

  @TestFactory
  Stream<DynamicTest> exactEndMinusOneWatermarksAndLateRowsKeepAssignedBounds() {
    return Stream.of("ONE_PHASE", "TWO_PHASE")
        .flatMap(
            phase ->
                Stream.of("TUMBLE", "HOP")
                    .map(
                        window ->
                            DynamicTest.dynamicTest(
                                phase + "/" + window,
                                () -> {
                                  String args =
                                      window.equals("HOP")
                                          ? ", INTERVAL '1' SECOND, INTERVAL '2' SECOND"
                                          : ", INTERVAL '1' SECOND";
                                  String sql =
                                      "SELECT MAX(n), window_start, window_end FROM (SELECT k,"
                                          + " COUNT(*) AS n, window_start, window_end FROM TABLE("
                                          + window
                                          + "(TABLE src, DESCRIPTOR(rt)"
                                          + args
                                          + ")) GROUP BY k, window_start, window_end) counts GROUP"
                                          + " BY window_start, window_end";
                                  var expected = new ArrayList<List<Object>>();
                                  expected.add(
                                      List.of(
                                          "+I",
                                          1L,
                                          LocalDateTime.ofEpochSecond(
                                              window.equals("HOP") ? -1 : 0, 0, ZoneOffset.UTC),
                                          LocalDateTime.ofEpochSecond(1, 0, ZoneOffset.UTC)));
                                  expected.add(
                                      List.of(
                                          "+I",
                                          window.equals("HOP") ? 3L : 1L,
                                          LocalDateTime.ofEpochSecond(
                                              window.equals("HOP") ? 0 : 1, 0, ZoneOffset.UTC),
                                          LocalDateTime.ofEpochSecond(2, 0, ZoneOffset.UTC)));
                                  if (window.equals("HOP"))
                                    expected.add(
                                        List.of(
                                            "+I",
                                            1L,
                                            LocalDateTime.ofEpochSecond(1, 0, ZoneOffset.UTC),
                                            LocalDateTime.ofEpochSecond(3, 0, ZoneOffset.UTC)));
                                  NativeParity.assertKindedParity(
                                      () ->
                                          FlinkWindowLateSliceSqlHarnessTest.boundaryEnvironment(
                                              phase),
                                      sql,
                                      expected);
                                })));
  }

  @Test
  void filteredAttachedAggregationKeepsStockExecution() throws Exception {
    validateOtherPlans("FILTER");
  }

  @Test
  void sessionResultsKeepTheirExistingAggregatePath() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSessionTableFunction();
    validateOtherPlans("SESSION");
  }

  @Test
  void defaultAttachedWindowRouteKeepsStockExecution() throws Exception {
    validateOtherPlans("DEFAULT");
  }

  private static void validateOtherPlans(String mode) throws Exception {
    boolean session = mode.equals("SESSION");
    try (var metrics = new RecoveryExecutionMetrics()) {
      var env = metrics.environment(new Configuration(), 1, 0);
      var table = StreamTableEnvironment.create(env);
      table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
      if (!mode.equals("DEFAULT"))
        table.getConfig().set("streamfusion.window.attached.single-phase.enabled", "true");
      var type = ROW(FIELD("k", INT()), FIELD("v", BIGINT()), FIELD("rt", TIMESTAMP(3)));
      table.createTemporaryView(
          "src",
          table.fromDataStream(
              fromData(
                  env,
                  List.of(
                      Row.of(1, 2L, LocalDateTime.of(2020, 1, 1, 0, 0)),
                      Row.of(1, 3L, LocalDateTime.of(2020, 1, 1, 0, 0, 1))),
                  ExternalTypeInfo.<Row>of(type)),
              Schema.newBuilder()
                  .fromRowDataType(type)
                  .watermark("rt", "rt - INTERVAL '1' SECOND")
                  .build()));
      var scan = NativePlanner.install(table);
      String tvf =
          session
              ? "SESSION(TABLE src PARTITION BY k, DESCRIPTOR(rt), INTERVAL '2' SECOND)"
              : "TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '2' SECOND)";
      String sql =
          "SELECT MAX(n)"
              + (mode.equals("FILTER") ? " FILTER (WHERE n > 1)" : "")
              + ", window_start, window_end FROM (SELECT k, COUNT(*) n, window_start, window_end"
              + " FROM TABLE("
              + tvf
              + ") GROUP BY k, window_start, window_end) counts GROUP BY window_start, window_end";
      String plan = table.explainSql(sql);
      var result = table.executeSql(sql);
      var actual = new ArrayList<Row>();
      try (var rows = result.collect()) {
        while (rows.hasNext()) actual.add(rows.next());
      }
      result.getJobClient().orElseThrow().getJobExecutionResult().get();
      if (session) {
        assertFalse(plan.contains("NativeColumnarWindowAggregate"), plan);
        assertTrue(
            scan.substitutions() > 0 && scan.fallbackReasons().isEmpty(), scan.explainSummary());
        assertEquals(
            List.of(
                Row.of(
                    2L, LocalDateTime.of(2020, 1, 1, 0, 0), LocalDateTime.of(2020, 1, 1, 0, 0, 3))),
            actual);
      } else {
        assertEquals(0, scan.substitutions(), scan.explainSummary());
        assertTrue(
            scan.fallbackReasons().stream().anyMatch(reason -> reason.contains("attached")),
            scan.explainSummary());
      }
    }
  }

  private static void compare(String phase, String window, String zone, boolean ltz, boolean empty)
      throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      Map<List<Object>, Integer> expected = null;
      List<String> expectedTypes = null;
      for (boolean nativeEnabled : new boolean[] {false, true}) {
        var env = metrics.environment(new Configuration(), 1, 0);
        var table = StreamTableEnvironment.create(env);
        table.getConfig().setLocalTimeZone(ZoneId.of(zone));
        table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
        table.getConfig().set("streamfusion.window.attached.single-phase.enabled", "true");
        var type =
            ROW(
                FIELD("k", INT()),
                FIELD("v", BIGINT()),
                FIELD("rt", ltz ? TIMESTAMP_LTZ(3) : TIMESTAMP(3)));
        var input = new ArrayList<Row>();
        long start = Instant.parse("2026-03-08T09:59:58Z").toEpochMilli();
        for (int i = 0; i < 2051; i++) {
          Instant instant = Instant.ofEpochMilli(start + i * 25L);
          Object time = ltz ? (Object) instant : LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
          input.add(
              Row.of(i % 11 == 0 ? null : i % 5, i % 7 == 0 ? null : (long) (i % 17 - 8), time));
        }
        org.apache.flink.streaming.api.datastream.DataStream<Row> stream =
            fromData(env, input, ExternalTypeInfo.<Row>of(type));
        if (empty) stream = stream.filter(row -> false);
        table.createTemporaryView(
            "src",
            table.fromDataStream(
                stream,
                Schema.newBuilder()
                    .fromRowDataType(type)
                    .watermark("rt", "rt - INTERVAL '1' SECOND")
                    .build()));
        table.getConfig().set("streamfusion.native.enabled", Boolean.toString(nativeEnabled));
        var scan = NativePlanner.install(table);
        String arguments =
            (window.equals("HOP") || window.equals("CUMULATE"))
                ? ", INTERVAL '2' SECOND, INTERVAL '10' SECOND"
                : ", INTERVAL '10' SECOND";
        String sql =
            "SELECT MAX(n), SUM(n), AVG(mx), COUNT(*), wstart, wend FROM (SELECT k, COUNT(v) AS n,"
                + " MAX(v) AS mx, window_start AS wstart, window_end AS wend FROM TABLE("
                + window
                + "(TABLE src, DESCRIPTOR(rt)"
                + arguments
                + "))"
                + " GROUP BY k, window_start, window_end) counts GROUP BY wstart, wend";
        var result = table.executeSql(sql);
        var actual = new HashMap<List<Object>, Integer>();
        try (var rows = result.collect()) {
          while (rows.hasNext()) {
            Row row = rows.next();
            var fields = new ArrayList<Object>();
            fields.add(row.getKind());
            for (int i = 0; i < row.getArity(); i++) fields.add(row.getField(i));
            actual.merge(fields, 1, Integer::sum);
          }
        }
        result.getJobClient().orElseThrow().getJobExecutionResult().get();
        var types =
            result.getResolvedSchema().getColumnDataTypes().stream()
                .map(Object::toString)
                .collect(tech.streamfusion.compat.ListCollectors.toList());
        if (!nativeEnabled) {
          expected = actual;
          expectedTypes = types;
        } else {
          assertEquals(expected, actual, "window results and RowKinds differ");
          assertEquals(expectedTypes, types);
          assertTrue(
              scan.substitutions() > 0 && scan.fallbackReasons().isEmpty(), scan.explainSummary());
          if (empty) assertTrue(actual.isEmpty());
          else {
            String job = result.getJobClient().orElseThrow().getJobID().toString();
            var executed =
                metrics.nativeOperators().stream()
                    .filter(
                        row ->
                            job.equals(row.get("jobId"))
                                && row.get("operator").toString().contains("WindowAgg")
                                && ((Number) row.get("recordsIn")).longValue() > 0)
                    .collect(tech.streamfusion.compat.ListCollectors.toList());
            assertTrue(executed.size() >= 2, executed.toString());
            assertTrue(
                executed.stream()
                    .anyMatch(row -> ((Number) row.get("recordsIn")).longValue() == input.size()),
                executed.toString());
          }
        }
      }
    }
  }
}

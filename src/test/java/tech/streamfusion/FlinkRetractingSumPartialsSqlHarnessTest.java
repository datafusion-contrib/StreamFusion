package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import tech.streamfusion.planner.NativePlanner;

class FlinkRetractingSumPartialsSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> exactSumCountsAndRecordLivenessKeepTheCompleteChangelog() {
    return Stream.of("TINYINT", "SMALLINT", "INT", "BIGINT")
        .flatMap(
            type ->
                Stream.of(1, 2, 4)
                    .map(
                        bundle ->
                            DynamicTest.dynamicTest(
                                type + "/" + bundle, () -> compare(type, bundle, 1))));
  }

  @Test
  void severalArrowBatchesKeepOrderedRetractions() throws Exception {
    compare("BIGINT", 4, 128);
  }

  @TestFactory
  Stream<DynamicTest> unverifiedSumRetractionTypesKeepStockExecution() {
    return Stream.of("DECIMAL(12,2)", "FLOAT", "DOUBLE")
        .map(
            type ->
                DynamicTest.dynamicTest(
                    type,
                    () -> {
                      try (var metrics = new RecoveryExecutionMetrics()) {
                        var env = metrics.environment(new Configuration(), 1, 0);
                        var table = StreamTableEnvironment.create(env);
                        table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
                        table.getConfig().set("table.exec.mini-batch.enabled", "true");
                        table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
                        table.getConfig().set("table.exec.mini-batch.size", "2");
                        var rowType = ROW(FIELD("k", INT()), FIELD("v", BIGINT()));
                        table.createTemporaryView(
                            "src",
                            table.fromChangelogStream(
                                fromData(env, input("BIGINT"), ExternalTypeInfo.<Row>of(rowType)),
                                Schema.newBuilder().fromRowDataType(rowType).build()));
                        var scan = NativePlanner.install(table);
                        var result =
                            table.executeSql(
                                "SELECT k, SUM(CAST(v AS " + type + ")) FROM src GROUP BY k");
                        try (var rows = result.collect()) {
                          while (rows.hasNext()) rows.next();
                        }
                        result.getJobClient().orElseThrow().getJobExecutionResult().get();
                        assertEquals(0, scan.substitutions(), scan.explainSummary());
                        assertFalse(scan.fallbackReasons().isEmpty(), scan.explainSummary());
                      }
                    }));
  }

  private static void compare(String typeName, int bundle, int repeats) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      List<List<Object>> expected = null;
      List<String> expectedTypes = null;
      for (boolean nativeEnabled : new boolean[] {false, true}) {
        var env = metrics.environment(new Configuration(), 1, 0);
        var table = StreamTableEnvironment.create(env);
        table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
        table.getConfig().set("table.exec.mini-batch.enabled", "true");
        table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
        table.getConfig().set("table.exec.mini-batch.size", Integer.toString(bundle));
        var valueType =
            typeName.equals("TINYINT")
                ? TINYINT()
                : typeName.equals("SMALLINT")
                    ? SMALLINT()
                    : typeName.equals("INT") ? INT() : BIGINT();
        var rowType = ROW(FIELD("k", INT()), FIELD("v", valueType));
        var input = new ArrayList<Row>();
        for (int i = 0; i < repeats; i++) {
          var cycle = input(typeName);
          // One key gives a deterministic global count-trigger cadence across many local bundles.
          if (repeats > 1) for (Row row : cycle) row.setField(0, 0);
          input.addAll(cycle);
        }
        table.createTemporaryView(
            "src",
            table.fromChangelogStream(
                fromData(env, input, ExternalTypeInfo.<Row>of(rowType)),
                Schema.newBuilder().fromRowDataType(rowType).build()));
        table.getConfig().set("streamfusion.native.enabled", Boolean.toString(nativeEnabled));
        var scan = NativePlanner.install(table);
        String sql =
            "SELECT k, SUM(v), COUNT(v), AVG(v), SUM(v) FILTER (WHERE v > 0)"
                + " FROM src GROUP BY k";
        var result = table.executeSql(sql);
        var actual = new ArrayList<List<Object>>();
        try (var rows = result.collect()) {
          while (rows.hasNext()) {
            var row = rows.next();
            var values = new ArrayList<Object>();
            values.add(row.getKind().shortString());
            for (int i = 0; i < row.getArity(); i++) values.add(row.getField(i));
            actual.add(values);
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
          assertEquals(expected, actual, "complete ordered changelog differs");
          assertEquals(expectedTypes, types);
          assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
          String job = result.getJobClient().orElseThrow().getJobID().toString();
          var executed =
              metrics.nativeOperators().stream()
                  .filter(row -> job.equals(row.get("jobId")))
                  .collect(tech.streamfusion.compat.ListCollectors.toList());
          assertTrue(
              executed.stream()
                  .anyMatch(
                      row ->
                          row.get("operator").toString().startsWith("NativeColumnarLocalGroupAgg")
                              && ((Number) row.get("recordsIn")).longValue() == input.size()),
              executed.toString());
          assertTrue(
              executed.stream()
                  .anyMatch(
                      row ->
                          row.get("operator").toString().startsWith("NativeColumnarGroupAgg")
                              && ((Number) row.get("recordsIn")).longValue() > 0),
              executed.toString());
        }
      }
    }
  }

  private static List<Row> input(String type) {
    var rows = new ArrayList<Row>();
    rows.add(Row.ofKind(RowKind.INSERT, 0, null));
    add(rows, RowKind.INSERT, 0, 10, type);
    add(rows, RowKind.UPDATE_BEFORE, 0, 10, type);
    add(rows, RowKind.UPDATE_AFTER, 0, 20, type);
    add(rows, RowKind.DELETE, 0, 20, type);
    rows.add(Row.ofKind(RowKind.DELETE, 0, null));
    add(rows, RowKind.INSERT, 0, 0, type);
    add(rows, RowKind.DELETE, 0, 0, type);
    add(rows, RowKind.INSERT, 0, 7, type);
    add(rows, RowKind.DELETE, 0, 7, type);
    add(rows, RowKind.DELETE, 0, 7, type);
    rows.add(Row.ofKind(RowKind.INSERT, 0, null));
    long max =
        type.equals("TINYINT")
            ? Byte.MAX_VALUE
            : type.equals("SMALLINT")
                ? Short.MAX_VALUE
                : type.equals("INT") ? Integer.MAX_VALUE : Long.MAX_VALUE;
    add(rows, RowKind.INSERT, null, max, type);
    add(rows, RowKind.INSERT, null, 1, type);
    add(rows, RowKind.DELETE, null, max, type);
    add(rows, RowKind.DELETE, null, 1, type);
    add(rows, RowKind.INSERT, null, -max, type);
    add(rows, RowKind.INSERT, null, -2, type);
    add(rows, RowKind.DELETE, null, -max, type);
    add(rows, RowKind.DELETE, null, -2, type);
    add(rows, RowKind.INSERT, null, 11, type);
    return rows;
  }

  private static void add(List<Row> rows, RowKind kind, Integer key, long value, String type) {
    Object converted;
    switch (type) {
      case "TINYINT":
        converted = (byte) value;
        break;
      case "SMALLINT":
        converted = (short) value;
        break;
      case "INT":
        converted = (int) value;
        break;
      default:
        converted = value;
    }
    rows.add(Row.ofKind(kind, key, converted));
  }
}

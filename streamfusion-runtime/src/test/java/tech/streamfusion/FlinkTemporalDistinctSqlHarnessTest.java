package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkTemporalDistinctSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {
      "CAST(DATE '2025-01-01' + MOD(id,2)*INTERVAL '1' DAY AS DATE)",
      "CAST(TIMESTAMP '2025-01-01 00:00:00.123' + MOD(id,2)*INTERVAL '1' SECOND AS TIME(3))",
      "TIMESTAMP '2025-01-01 00:00:00.123' + MOD(id,2)*INTERVAL '1' SECOND"})
  void runtimeExpressionsDeduplicateAcrossOneRowBundles(String expression) {
    var result = NativeFailureParity.run(
        () -> input(ROW(FIELD("id", INT())), List.of(Row.of(1), Row.of(2), Row.of(3)), "UTC"),
        "SELECT MOD(id,2), COUNT(DISTINCT " + expression + ") FROM src GROUP BY MOD(id,2)");
    compare(result);
    assertEquals(java.util.Map.of(List.of(0, 1L), 1L, List.of(1, 1L), 1L),
        SqlAuditHarness.materialized(result.nativeRun().rows()));
  }

  @org.junit.jupiter.api.Test
  void nestedDistinctKeysRetainNativeMembership() {
    compare(NativeFailureParity.run(
        () -> input("DATE", "UTC"), "SELECT COUNT(DISTINCT ARRAY[k]) FROM src"));
  }

  @ParameterizedTest
  @CsvSource({"DATE,UTC", "TIME0,UTC", "TIME3,UTC", "TIMESTAMP0,UTC", "TIMESTAMP3,UTC",
      "TIMESTAMP6,UTC", "TIMESTAMP9,UTC", "TIMESTAMP9,Asia/Shanghai",
      "LTZ3,UTC", "LTZ9,Asia/Shanghai", "LTZ9,America/Los_Angeles"})
  void twoPhaseMembershipKeepsTemporalValuesAndFilters(String type, String zone) {
    for (String sql : List.of(
        "SELECT k, COUNT(DISTINCT v), COUNT(DISTINCT v), COUNT(DISTINCT v) FILTER (WHERE keep),"
            + " COUNT(DISTINCT v) FILTER (WHERE NOT keep) FROM src GROUP BY k",
        "SELECT COUNT(DISTINCT v) FROM src",
        "SELECT COUNT(DISTINCT v) FROM src WHERE k<0")) {
      var result = NativeFailureParity.run(() -> input(type, zone), sql);
      compare(result);
      if (sql.contains("GROUP BY")) {
        assertEquals(java.util.Map.of(List.of(0, 3L, 3L, 3L, 3L), 1L,
            List.of(1, 0L, 0L, 0L, 0L), 1L),
            SqlAuditHarness.materialized(result.nativeRun().rows()), result::toString);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"DATE,hashmap", "TIME0,hashmap", "TIMESTAMP9,hashmap",
      "DATE,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
      "TIME0,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
      "TIMESTAMP9,tech.streamfusion.state.RocksDBNativeStateBackendFactory"})
  void temporalMembershipSurvivesRepeatedRecovery(String type, String backend) {
    var rowType = rowType(type);
    try (var recovery = new PortableSqlRecovery(backend, rows(type), ExternalTypeInfo.<Row>of(rowType),
        Schema.newBuilder().fromRowDataType(rowType).build(), false, 8, 16).withExecutionMetrics()) {
      compare(NativeFailureParity.runRecovery(
          () -> configure(recovery.uninterrupted(), "Asia/Shanghai"),
          () -> configure(recovery.get(), "Asia/Shanghai"),
          "SELECT k, COUNT(DISTINCT v), COUNT(DISTINCT v) FILTER (WHERE keep)"
              + " FROM recovery_input GROUP BY k"));
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeColumnarLocalGroupAggregate");
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeColumnarGroupAggregate");
    }
  }

  @org.junit.jupiter.api.Test
  void timeCastKeepsTheReleasedPrecisionSemantics() {
    var result = NativeFailureParity.run(() -> input("TIMESTAMP3", "UTC"),
        "SELECT k, COUNT(DISTINCT CAST(v + INTERVAL '1' DAY AS TIME(3))) FROM src GROUP BY k");
    compare(result);
  }

  private static void compare(NativeFailureParity.Comparison result) {
    assertNull(result.host().failure(), result::toString);
    assertNull(result.nativeRun().failure(), result::toString);
    assertEquals(result.host().resultTypes(), result.nativeRun().resultTypes());
    assertEquals(SqlAuditHarness.materialized(result.host().rows()),
        SqlAuditHarness.materialized(result.nativeRun().rows()), result::toString);
    assertEquals(NativeFailureParity.Route.NATIVE, result.nativeRun().route(), result::toString);
    assertTrue(result.nativeRun().plan().contains("NativeColumnarLocalGroupAggregate"), result::toString);
    assertTrue(result.nativeRun().plan().contains("NativeColumnarGroupAggregate"), result::toString);
    assertTrue(result.nativeRun().fallbackReasons().isEmpty(), result::toString);
  }

  private static org.apache.flink.table.types.DataType valueType(String type) {
    if (type.equals("DATE")) return DATE();
    int precision = Character.digit(type.charAt(type.length()-1), 10);
    if (type.startsWith("TIME") && !type.startsWith("TIMESTAMP")) return TIME(precision);
    if (type.startsWith("LTZ")) return TIMESTAMP_LTZ(precision);
    return TIMESTAMP(precision);
  }

  private static org.apache.flink.table.types.DataType rowType(String type) {
    return ROW(FIELD("k", INT()), FIELD("v", valueType(type)), FIELD("keep", BOOLEAN()));
  }

  private static List<Row> rows(String type) {
    List<?> values;
    switch (type) {
      case "DATE":
        values = List.of(LocalDate.of(1,1,1), LocalDate.of(1969,12,31), LocalDate.of(9999,12,31));
        break;
      case "TIME0":
      case "TIME3":
        // Flink's internal TIME retains milliseconds even with a declared precision of zero.
        values = List.of(LocalTime.of(0,0,0,123_000_000), LocalTime.of(0,0,0,124_000_000),
            LocalTime.of(23,59,59,999_000_000));
        break;
      case "LTZ3":
      case "LTZ9":
        values = type.equals("LTZ3")
            ? List.of(Instant.parse("1969-12-31T23:59:59.123Z"),
                Instant.parse("1969-12-31T23:59:59.124Z"), Instant.parse("9999-12-31T00:00:00Z"))
            : List.of(Instant.parse("1969-12-31T23:59:59.123456789Z"),
                Instant.parse("1969-12-31T23:59:59.123456790Z"), Instant.parse("9999-12-31T00:00:00Z"));
        break;
      default:
        int p = Character.digit(type.charAt(type.length()-1), 10);
        int nanos = p == 0 ? 0 : p == 3 ? 123_000_000 : p == 6 ? 123_456_000 : 123_456_789;
        int step = p == 0 ? 1_000_000_000 : p == 3 ? 1_000_000 : p == 6 ? 1000 : 1;
        var first = LocalDateTime.of(1969,12,31,23,59,59,nanos);
        values = List.of(first, first.plusNanos(step), LocalDateTime.of(9999,12,31,0,0,0));
    }
    List<Row> rows = new ArrayList<>();
    for (int repeat = 0; repeat < 8; repeat++) {
      for (Object value : values) rows.add(Row.of(0, value, repeat % 2 == 0));
      rows.add(Row.of(1, null, true));
      rows.add(Row.of(0, null, false));
    }
    return rows;
  }

  private static TableEnvironment input(String type, String zone) {
    return input(rowType(type), rows(type), zone);
  }

  private static TableEnvironment input(org.apache.flink.table.types.DataType rowType,
      List<Row> rows, String zone) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.createTemporaryView("src", table.fromDataStream(fromData(env, rows,
        ExternalTypeInfo.<Row>of(rowType)), Schema.newBuilder().fromRowDataType(rowType).build()));
    return configure(table, zone);
  }

  private static TableEnvironment configure(TableEnvironment table, String zone) {
    table.getConfig().set("table.local-time-zone", zone);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("table.exec.mini-batch.size", "1");
    return table;
  }
}

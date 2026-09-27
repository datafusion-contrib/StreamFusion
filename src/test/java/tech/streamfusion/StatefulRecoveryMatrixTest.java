package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@org.junit.jupiter.api.parallel.Execution(org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD)
class StatefulRecoveryMatrixTest {
  private static final Map<String, Map<String, Object>> RESULTS = new java.util.TreeMap<>();

  @ParameterizedTest(name = "group-{0}-rocks={1}")
  @CsvSource({"false,false", "true,false", "false,true", "true,true"})
  void groupedTypesAcrossTwoRestores(boolean bigKey, boolean rocks) {
    String text = "long-string-".repeat(1024);
    List<Row> input = new ArrayList<>();
    input.add(row(bigKey, RowKind.INSERT, 1, "1.25", text));
    input.add(row(bigKey, RowKind.INSERT, 1, "2.50", text));
    input.add(row(bigKey, RowKind.INSERT, 2, null, null));
    input.add(row(bigKey, RowKind.INSERT, 1, "3.75", "other"));
    input.add(row(bigKey, RowKind.DELETE, 1, "1.25", text));
    input.add(row(bigKey, RowKind.DELETE, 1, "2.50", text));
    input.add(row(bigKey, RowKind.DELETE, 2, null, null));
    input.add(row(bigKey, RowKind.INSERT, 2, "9.50", text));
    input.add(row(bigKey, RowKind.DELETE, 1, "3.75", "other"));
    input.add(row(bigKey, RowKind.INSERT, 1, "-4.50", null));
    input.add(row(bigKey, RowKind.INSERT, 2, "-0.50", text));
    input.add(row(bigKey, RowKind.DELETE, 2, "9.50", text));
    var type =
        Types.ROW_NAMED(
            new String[] {"k", "amount", "text_value"},
            bigKey ? Types.LONG : Types.INT,
            Types.BIG_DEC,
            Types.STRING);
    var schema =
        Schema.newBuilder()
            .column("k", bigKey ? DataTypes.BIGINT() : DataTypes.INT())
            .column("amount", DataTypes.DECIMAL(20, 2))
            .column("text_value", DataTypes.STRING())
            .build();
    String backend = rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap";
    try (var recovery = new PortableSqlRecovery(backend, input, type, schema, true, 4, 8)) {
      var runs =
          NativeFailureParity.run(
              recovery::uninterrupted,
              recovery,
              "SELECT k, SUM(amount), COUNT(DISTINCT text_value) FROM recovery_input GROUP BY k");
      Map<List<Object>, Long> expected =
          Map.of(
              List.of(key(bigKey, 1), new BigDecimal("-4.50"), 0L), 1L,
              List.of(key(bigKey, 2), new BigDecimal("-0.50"), 1L), 1L);
      verify(
          recovery,
          runs,
          expected,
          "group-" + (bigKey ? "bigint" : "int") + "-" + (rocks ? "rocksdb" : "memory"),
          backend,
          List.of(4, 8),
          "NativeColumnarGroupAggregate");
    }
  }

  @ParameterizedTest(name = "join-rocks={0}")
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void updatingJoinAcrossTwoRestores(boolean rocks) {
    String left = "left-payload-".repeat(1024);
    String right = "right-payload-".repeat(1024);
    List<Row> input =
        List.of(
            joinRow(RowKind.INSERT, 0, 1, "1.25", left),
            joinRow(RowKind.INSERT, 0, 1, "1.25", left),
            joinRow(RowKind.INSERT, 1, 1, "5.00", right),
            joinRow(RowKind.INSERT, 1, 1, "5.00", right),
            joinRow(RowKind.INSERT, 0, 1, "8.00", left),
            joinRow(RowKind.INSERT, 1, 2, "7.00", right),
            joinRow(RowKind.DELETE, 0, 1, "1.25", left),
            joinRow(RowKind.DELETE, 1, 1, "5.00", right),
            joinRow(RowKind.INSERT, 0, 2, "2.00", null),
            joinRow(RowKind.UPDATE_BEFORE, 0, 1, "8.00", left),
            joinRow(RowKind.UPDATE_AFTER, 0, 1, "3.00", left),
            joinRow(RowKind.INSERT, 1, null, "5.00", right),
            joinRow(RowKind.DELETE, 0, 1, "1.25", left),
            joinRow(RowKind.DELETE, 1, 1, "5.00", right),
            joinRow(RowKind.INSERT, 1, 1, "4.00", right),
            joinRow(RowKind.INSERT, 1, 1, "4.00", right),
            joinRow(RowKind.DELETE, 1, 2, "7.00", right),
            joinRow(RowKind.INSERT, 1, 2, "9.00", right));
    var type =
        Types.ROW_NAMED(
            new String[] {"side_id", "k", "amount", "text_value"},
            Types.INT,
            Types.INT,
            Types.BIG_DEC,
            Types.STRING);
    var schema =
        Schema.newBuilder()
            .column("side_id", DataTypes.INT())
            .column("k", DataTypes.INT())
            .column("amount", DataTypes.DECIMAL(20, 2))
            .column("text_value", DataTypes.STRING())
            .build();
    String backend = rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap";
    try (var recovery = new PortableSqlRecovery(backend, input, type, schema, true, 6, 12)) {
      var runs =
          NativeFailureParity.run(
              recovery::uninterrupted,
              recovery,
              "SELECT l.k, l.amount, l.text_value, r.amount, r.text_value FROM recovery_input l"
                  + " JOIN recovery_input r ON l.k = r.k AND l.amount < r.amount"
                  + " WHERE l.side_id = 0 AND r.side_id = 1");
      Map<List<Object>, Long> expected =
          Map.of(
              List.of(1, new BigDecimal("3.00"), left, new BigDecimal("4.00"), right), 2L,
              java.util.Arrays.asList(
                      2, new BigDecimal("2.00"), null, new BigDecimal("9.00"), right),
                  1L);
      verify(
          recovery,
          runs,
          expected,
          "join-" + (rocks ? "rocksdb" : "memory"),
          backend,
          List.of(6, 12),
          "NativeColumnarUpdatingJoin");
    }
  }

  private static Row joinRow(RowKind kind, int side, Integer key, String amount, String text) {
    return Row.ofKind(kind, side, key, new BigDecimal(amount), text);
  }

  private static void verify(
      PortableSqlRecovery recovery,
      NativeFailureParity.Comparison runs,
      Map<List<Object>, Long> expected,
      String id,
      String backend,
      List<Integer> boundaries,
      String operator) {
    boolean passed = false;
    try {
      assertAll(
          () -> assertNull(runs.host().failure(), runs.toString()),
          () -> assertNull(runs.nativeRun().failure(), runs.toString()),
          () -> assertEquals(expected, SqlAuditHarness.materialized(runs.host().rows())),
          () -> assertEquals(expected, SqlAuditHarness.materialized(runs.nativeRun().rows())),
          () -> assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes()),
          () -> assertEquals(NativeFailureParity.Route.NATIVE, runs.nativeRun().route()),
          () -> assertTrue(runs.nativeRun().plan().contains(operator), runs.nativeRun().plan()),
          recovery::verifyRepeatedRecovery);
      passed = true;
    } finally {
      RESULTS.put(
          id,
          Map.of(
              "passed", passed,
              "configuration",
                  Map.of(
                      "backend",
                      backend,
                      "parallelism",
                      1,
                      "maxParallelism",
                      128,
                      "checkpointOffsets",
                      boundaries,
                      "physicalBatchRows",
                      1024,
                      "logicalMiniBatch",
                      false),
              "host", SqlAuditHarness.outcome(runs.host()),
              "native", SqlAuditHarness.outcome(runs.nativeRun()),
              "nativePlan", runs.nativeRun().plan(),
              "materializedResultsEqual",
                  SqlAuditHarness.materialized(runs.host().rows())
                      .equals(SqlAuditHarness.materialized(runs.nativeRun().rows())),
              "recovery", recovery.observations()));
    }
  }

  @org.junit.jupiter.api.AfterAll
  static void writeEvidence() throws Exception {
    var output = java.nio.file.Path.of("target", "sql-audit", "stateful-recovery.json");
    java.nio.file.Files.createDirectories(output.getParent());
    new com.fasterxml.jackson.databind.ObjectMapper()
        .writerWithDefaultPrettyPrinter()
        .writeValue(output.toFile(), RESULTS);
  }

  private static Object key(boolean big, int key) {
    if (big) return (long) key;
    return key;
  }

  private static Row row(boolean big, RowKind kind, int key, String amount, String text) {
    return Row.ofKind(kind, key(big, key), amount == null ? null : new BigDecimal(amount), text);
  }
}

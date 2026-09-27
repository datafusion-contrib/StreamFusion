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
      var expected =
          Map.of(
              List.of(key(bigKey, 1), new BigDecimal("-4.50"), 0L), 1L,
              List.of(key(bigKey, 2), new BigDecimal("-0.50"), 1L), 1L);
      boolean passed = false;
      try {
        assertAll(
            () -> assertNull(runs.host().failure(), runs.toString()),
            () -> assertNull(runs.nativeRun().failure(), runs.toString()),
            () -> assertEquals(expected, SqlAuditHarness.materialized(runs.host().rows())),
            () -> assertEquals(expected, SqlAuditHarness.materialized(runs.nativeRun().rows())),
            () -> assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes()),
            () -> assertEquals(NativeFailureParity.Route.NATIVE, runs.nativeRun().route()),
            () ->
                assertTrue(
                    runs.nativeRun().plan().contains("NativeColumnarGroupAggregate"),
                    runs.nativeRun().plan()),
            recovery::verifyRepeatedRecovery);
        passed = true;
      } finally {
        String id = "group-" + (bigKey ? "bigint" : "int") + "-" + (rocks ? "rocksdb" : "memory");
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
                        List.of(4, 8),
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

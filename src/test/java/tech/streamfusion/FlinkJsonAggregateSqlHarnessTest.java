package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkJsonAggregateSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({
    "false,false,false",
    "false,false,true",
    "false,true,false",
    "false,true,true",
    "true,false,false",
    "true,false,true",
    "true,true,false",
    "true,true,true"
  })
  void positiveRetentionAdmitsArrayAndObjectViews(boolean object, boolean mini, boolean updating) {
    var removed = row("a", "a");
    removed.setKind(RowKind.DELETE);
    Row[] rows =
        updating
            ? new Row[] {row("a", "a"), row("b", "b"), row("c", null), removed, row("a", "last")}
            : new Row[] {row("a", "a"), row("b", "b"), row("c", null)};
    for (String nulls : List.of(" NULL ON NULL", " ABSENT ON NULL")) {
      var comparison =
          NativeFailureParity.run(
              () -> {
                var table = FlinkStringAggregateHostContractTest.environment(updating, rows);
                table.getConfig().set("table.exec.state.ttl", "1 h");
                table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
                table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(mini));
                table.getConfig().set("table.exec.mini-batch.size", "2");
                table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
                return table;
              },
              "SELECT sep, "
                  + (object ? "JSON_OBJECTAGG(json_key VALUE v" : "JSON_ARRAYAGG(v")
                  + nulls
                  + "), COUNT(*) FROM src GROUP BY sep");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertNative(comparison);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void arraysPreserveNullPoliciesOrderDuplicatesAndEscapes(boolean updating) {
    for (String policy : List.of("", " NULL ON NULL", " ABSENT ON NULL")) {
      check(
          updating,
          "SELECT JSON_ARRAYAGG(v" + policy + ") FROM src",
          row("a", "a"),
          row("b", "b"),
          row("c", "a"),
          row("d", null),
          row("e", "\0\n\"\\/é🚀"),
          row("f", ""));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void malformedUtf16RetainsTheObservedAggregateBoundary(boolean updating) {
    String malformed = new String(new char[] {'\ud800'});
    check(updating, "SELECT JSON_ARRAYAGG(v) FROM src", row("key", malformed));
    check(updating, "SELECT JSON_OBJECTAGG(json_key VALUE v) FROM src", row(malformed, malformed));
  }

  @Test
  void arrayRetractionRemovesTheFirstMatchingOccurrence() {
    var removed = row("a", "a");
    removed.setKind(RowKind.DELETE);
    for (String policy : List.of(" NULL ON NULL", " ABSENT ON NULL")) {
      check(
          true,
          "SELECT JSON_ARRAYAGG(v" + policy + ") FROM src",
          row("a", "a"),
          row("b", "b"),
          row("a", "a"),
          row("n", null),
          removed);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void objectsPreserveEscapingUtf16KeyOrderAndNullPolicies(boolean updating) {
    for (String policy : List.of("", " NULL ON NULL", " ABSENT ON NULL")) {
      check(
          updating,
          "SELECT JSON_OBJECTAGG(json_key VALUE v" + policy + ") FROM src",
          row("\ue000", "\0\n\"\\/é🚀"),
          row("🚀", ""),
          row("z", null),
          row("a", "a"));
    }
  }

  @Test
  void objectRetractionUsesTheKeyAndAllowsReinsertion() {
    var removed = row("a", "different value");
    removed.setKind(RowKind.DELETE);
    check(
        true,
        "SELECT JSON_OBJECTAGG(json_key VALUE v) FROM src",
        row("a", "first"),
        row("b", "second"),
        removed,
        row("a", "last"));
  }

  @Test
  void numericValuesAndAggregateCompositionKeepTheirTypes() {
    check(
        false,
        "SELECT sep, JSON_ARRAYAGG(n), COUNT(*), LISTAGG(v, ',') FROM src GROUP BY sep",
        row("a", "a"),
        row("b", null),
        row("c", "b"));
    check(
        false,
        "SELECT JSON_ARRAYAGG(keep_row), JSON_ARRAYAGG(CAST(v AS CHAR(4))) FROM src",
        row("a", "a"),
        row("b", "abcdef"),
        row("c", "é🚀"),
        row("d", null));
    check(
        false,
        "SELECT JSON_OBJECTAGG(json_key VALUE n) FROM src",
        row("a", "unused"),
        row("b", "unused"));
  }

  @Test
  void decimalRenderingRetainsTheSelectedReleaseContract() {
    var comparison =
        NativeFailureParity.run(
            () ->
                FlinkStringAggregateHostContractTest.environment(
                    false, row("a", "v"), row("b", "v")),
            "SELECT JSON_ARRAYAGG(CAST(n AS DECIMAL(20,2))) FROM src");
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertNative(comparison);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void miniBatchPreservesFailurePrecedence(boolean duplicateNullKey) {
    var comparison =
        NativeFailureParity.run(
            () -> {
              var table =
                  FlinkStringAggregateHostContractTest.environment(
                      false, row(null, "first"), row(duplicateNullKey ? null : "other", "second"));
              table.getConfig().set("table.exec.mini-batch.enabled", "true");
              table.getConfig().set("table.exec.mini-batch.size", "3");
              table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
              return table;
            },
            "SELECT JSON_OBJECTAGG(json_key VALUE v)"
                + (duplicateNullKey ? "" : ", SINGLE_VALUE(n)")
                + " FROM src");
    assertNative(comparison);
    assertNotNull(comparison.host().failure());
    assertNotNull(comparison.nativeRun().failure());
    assertEquals(
        comparison.host().rootCause().getClass(),
        comparison.nativeRun().rootCause().getClass(),
        comparison.toString());
    assertEquals(
        tableExceptionCount(comparison.host().failure()),
        tableExceptionCount(comparison.nativeRun().failure()),
        comparison.toString());
    if (!duplicateNullKey || !tech.streamfusion.compat.JsonRuntimeCompat.LEGACY_SQL_JSON)
      assertEquals(
          comparison.host().rootCause().getMessage(),
          comparison.nativeRun().rootCause().getMessage());
  }

  @Test
  void emptyInputAndAllNullValuesKeepTheReleasedRowsAndSchemas() {
    for (String expression :
        List.of(
            "JSON_ARRAYAGG(v)",
            "JSON_ARRAYAGG(v NULL ON NULL)",
            "JSON_OBJECTAGG(json_key VALUE v)",
            "JSON_OBJECTAGG(json_key VALUE v ABSENT ON NULL)")) {
      check(false, "SELECT " + expression + " FROM src");
      check(false, "SELECT " + expression + " FROM src", row("a", null), row("b", null));
    }
  }

  @Test
  void duplicateKeyFailsBeforeNullOmission() {
    var duplicate = row("a", null);
    var comparison =
        NativeFailureParity.run(
            () ->
                FlinkStringAggregateHostContractTest.environment(
                    false, row("a", "first"), duplicate),
            "SELECT JSON_OBJECTAGG(json_key VALUE v ABSENT ON NULL) FROM src");
    comparison.assertFailure(
        org.apache.flink.table.api.TableException.class,
        "Duplicate keys are not allowed in JSON_OBJECTAGG",
        NativeFailureParity.Phase.ROW_EVALUATION,
        NativeFailureParity.Route.NATIVE);
    assertNative(comparison);
  }

  @Test
  void nullKeysRetainTheVersionSpecificFailureAndOmission() {
    for (String policy : List.of("", " ABSENT ON NULL")) {
      for (String value : new String[] {"nonnull", null}) {
        var comparison =
            NativeFailureParity.run(
                () -> FlinkStringAggregateHostContractTest.environment(false, row(null, value)),
                "SELECT JSON_OBJECTAGG(json_key VALUE v" + policy + ") FROM src");
        boolean legacy = tech.streamfusion.compat.JsonRuntimeCompat.LEGACY_SQL_JSON;
        if (legacy && value == null && !policy.isEmpty()) {
          comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
        } else {
          assertNotNull(comparison.host().failure());
          assertNotNull(comparison.nativeRun().failure());
          String type =
              legacy
                  ? "org.apache.flink.table.api.TableException"
                  : "org.apache.flink.table.api.TableRuntimeException";
          String message =
              legacy
                  ? "The accumulator state could not be serialized."
                  : "Map views don't support null keys.";
          assertCause(comparison.host().failure(), type, message);
          assertCause(comparison.nativeRun().failure(), type, message);
          assertEquals(
              comparison.host().rootCause().getClass(),
              comparison.nativeRun().rootCause().getClass());
        }
        assertNative(comparison);
      }
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "hashmap,false",
    "hashmap,true",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,false",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,true"
  })
  void twoRestoresRetainArrayOrderAndObjectMembership(String backend, boolean updating)
      throws Exception {
    for (boolean object : new boolean[] {false, true}) {
      var input = new java.util.ArrayList<Row>();
      for (int i = 0; i < 96; i++) {
        int original = updating && i >= 64 ? i - 64 : i;
        input.add(
            Row.ofKind(
                updating && i >= 64 ? RowKind.DELETE : RowKind.INSERT,
                original % 3,
                original % 7 == 0 ? null : "é🚀" + original % 5,
                "key-" + original));
      }
      try (var recovery =
          new PortableSqlRecovery(
                  backend,
                  input,
                  org.apache.flink.api.common.typeinfo.Types.ROW_NAMED(
                      new String[] {"k", "v", "json_key"},
                      org.apache.flink.api.common.typeinfo.Types.INT,
                      org.apache.flink.api.common.typeinfo.Types.STRING,
                      org.apache.flink.api.common.typeinfo.Types.STRING),
                  org.apache.flink.table.api.Schema.newBuilder()
                      .column("k", org.apache.flink.table.api.DataTypes.INT())
                      .column("v", org.apache.flink.table.api.DataTypes.STRING())
                      .column("json_key", org.apache.flink.table.api.DataTypes.STRING())
                      .build(),
                  updating,
                  24,
                  72)
              .withExecutionMetrics()) {
        java.util.function.UnaryOperator<org.apache.flink.table.api.TableEnvironment> configure =
            table -> {
              table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
              table.getConfig().set("table.exec.mini-batch.enabled", "false");
              table.getConfig().set("streamfusion.transpose.batchRows", "13");
              return table;
            };
        var comparison =
            NativeFailureParity.runRecovery(
                () -> configure.apply(recovery.uninterrupted()),
                () -> configure.apply(recovery.get()),
                "SELECT k, "
                    + (object
                        ? "JSON_OBJECTAGG(json_key VALUE v)"
                        : "JSON_ARRAYAGG(v NULL ON NULL)")
                    + " FROM recovery_input GROUP BY k");
        comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertNative(comparison);
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(
            recovery.nativeExecution(), "NativeColumnarGroupAggregate");
        SharedFlinkCluster.assertNativeMemoryReleased("JSON-aggregate-recovery");
      }
    }
  }

  private static int tableExceptionCount(Throwable cause) {
    int count = 0;
    for (; cause != null; cause = cause.getCause())
      if (cause.getClass() == org.apache.flink.table.api.TableException.class) count++;
    return count;
  }

  private static void assertCause(Throwable cause, String type, String message) {
    for (; cause != null; cause = cause.getCause()) {
      if (cause.getClass().getName().equals(type) && message.equals(cause.getMessage())) return;
    }
    fail("Missing " + type + ": " + message);
  }

  private static void check(boolean updating, String sql, Row... input) {
    var comparison =
        NativeFailureParity.run(
            () -> FlinkStringAggregateHostContractTest.environment(updating, input), sql);
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertNative(comparison);
    assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
    assertEquals(comparison.host().rows(), comparison.nativeRun().rows());
  }

  private static void assertNative(NativeFailureParity.Comparison comparison) {
    assertEquals(NativeFailureParity.Route.NATIVE, comparison.nativeRun().route());
    assertTrue(
        comparison.nativeRun().plan().contains("NativeColumnarGroupAggregate"),
        comparison.toString());
  }

  private static Row row(String key, String value) {
    return Row.of(value, ",", key, 1L, true);
  }
}

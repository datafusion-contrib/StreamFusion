package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkJsonObjectPartialsSqlHarnessTest {
  static Stream<Arguments> profiles() {
    return Stream.of("v", "CAST(v AS DECIMAL(20,3))", "CAST(v AS CHAR(8))")
        .flatMap(
            value ->
                Stream.of(false, true)
                    .flatMap(
                        updating ->
                            Stream.of(false, true)
                                .map(retained -> Arguments.of(value, updating, retained))));
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void emptyScalarAndGroupedInputsKeepReleasedResults(boolean grouped) {
    var comparison =
        NativeFailureParity.run(
            () -> environment(null, false, false, List.of()),
            "SELECT "
                + (grouped ? "k," : "")
                + "JSON_OBJECTAGG(jk VALUE v),COUNT(*) FROM src"
                + (grouped ? " GROUP BY k" : ""));
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
  }

  @ParameterizedTest
  @CsvSource({"false,1", "false,3", "true,1", "true,3"})
  void nullAndEmptyKeysRetainReleasedPartialTransport(boolean emptyFirst, int bundle) {
    var rows =
        List.of(
            Row.of(1, emptyFirst ? "" : null, "first"),
            Row.of(1, emptyFirst ? null : "", "second"));
    var comparison =
        NativeFailureParity.run(
            () -> {
              var table = environment(null, false, false, rows);
              table.getConfig().set("table.exec.mini-batch.size", Integer.toString(bundle));
              return table;
            },
            "SELECT k,JSON_OBJECTAGG(jk VALUE v) FROM src GROUP BY k");
    if (comparison.host().failure() == null)
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    else {
      assertNotNull(comparison.nativeRun().failure());
      assertEquals(
          comparison.host().rootCause().getClass(), comparison.nativeRun().rootCause().getClass());
      assertEquals(
          comparison.host().rootCause().getMessage(),
          comparison.nativeRun().rootCause().getMessage());
    }
    assertTrue(comparison.nativeRun().plan().contains("NativeColumnarLocalGroupAggregate"));
  }

  @ParameterizedTest
  @CsvSource({"7,false", "13,false", "33,false", "7,true", "13,true", "33,true"})
  void mergedDuplicateFailuresRetainReleasedMapIterationOrder(int size, boolean reverse) {
    var rows = new ArrayList<Row>();
    for (int i = 0; i < size; i++) rows.add(Row.of(1, "key" + i, "v"));
    for (int i = 0; i < size; i++) rows.add(Row.of(1, "key" + (reverse ? size - 1 - i : i), "v"));
    var comparison =
        NativeFailureParity.run(
            () -> {
              var table = environment(null, false, false, rows);
              table.getConfig().set("table.exec.mini-batch.size", Integer.toString(size));
              table.getConfig().set("streamfusion.transpose.batchRows", Integer.toString(size));
              return table;
            },
            "SELECT k,JSON_OBJECTAGG(jk VALUE v) FROM src GROUP BY k");
    assertNotNull(comparison.host().failure());
    assertNotNull(comparison.nativeRun().failure());
    assertEquals(
        comparison.host().rootCause().getClass(), comparison.nativeRun().rootCause().getClass());
    assertEquals(
        comparison.host().rootCause().getMessage(),
        comparison.nativeRun().rootCause().getMessage());
    assertTrue(comparison.nativeRun().plan().contains("NativeColumnarLocalGroupAggregate"));
  }

  @ParameterizedTest
  @MethodSource("profiles")
  void mapPartialsComposeWithNumericExtremaAndDistinctViews(
      String value, boolean updating, boolean retained) {
    var rows =
        new ArrayList<Row>(
            List.of(
                Row.of(1, "a", "1.250"),
                Row.of(1, "b", null),
                Row.of(1, "é🚀\0", "2.300"),
                Row.of(2, "a", "3.125"),
                Row.of(2, "quote\"", "4.000"),
                Row.of(1, "c", "5.100")));
    if (updating) rows.add(Row.ofKind(RowKind.DELETE, 1, "a", "1.250"));
    try (var metrics = new RecoveryExecutionMetrics()) {
      var comparison =
          NativeFailureParity.run(
              () -> environment(metrics, updating, retained, rows),
              "SELECT k,JSON_OBJECTAGG(jk VALUE "
                  + value
                  + " NULL ON NULL),COUNT(*),"
                  + "JSON_OBJECTAGG(jk VALUE "
                  + value
                  + " ABSENT ON NULL),MIN(v),COUNT(DISTINCT jk) FROM src GROUP BY k");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      assertTrue(comparison.host().plan().contains("LocalGroupAggregate"));
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators(), "NativeColumnarLocalGroupAggregate");
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators(), "NativeColumnarGroupAggregate");
    }
  }

  @ParameterizedTest
  @CsvSource({
    "NULL ON NULL,2,false",
    "NULL ON NULL,7,false",
    "ABSENT ON NULL,2,false",
    "ABSENT ON NULL,7,false",
    "NULL ON NULL,2,true",
    "NULL ON NULL,7,true",
    "ABSENT ON NULL,2,true",
    "ABSENT ON NULL,7,true"
  })
  void duplicateChecksInsideAndAcrossPartialsKeepTheReleasedFailure(
      String policy, int bundle, boolean retained) {
    var rows = new ArrayList<Row>();
    rows.add(Row.of(1, "a", "first"));
    for (int i = 0; i < 5; i++) rows.add(Row.of(1, "k" + i, "v" + i));
    rows.add(Row.of(1, "a", null));
    var comparison =
        NativeFailureParity.run(
            () -> {
              var table = environment(null, false, retained, rows);
              table.getConfig().set("table.exec.mini-batch.size", Integer.toString(bundle));
              return table;
            },
            "SELECT k,JSON_OBJECTAGG(jk VALUE v " + policy + ") FROM src GROUP BY k");
    if (policy.equals("ABSENT ON NULL") && bundle == 2) {
      // A NULL omitted by the local map leaves no key for the global duplicate check.
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    } else {
      assertNotNull(comparison.host().failure());
      assertNotNull(comparison.nativeRun().failure());
      assertEquals(
          comparison.host().rootCause().getClass(), comparison.nativeRun().rootCause().getClass());
      assertEquals(
          comparison.host().rootCause().getMessage(),
          comparison.nativeRun().rootCause().getMessage());
      assertEquals(comparison.host().phase(), comparison.nativeRun().phase());
    }
    assertTrue(comparison.nativeRun().plan().contains("NativeColumnarLocalGroupAggregate"));
  }

  @ParameterizedTest
  @CsvSource({
    "false,NULL ON NULL",
    "false,ABSENT ON NULL",
    "true,NULL ON NULL",
    "true,ABSENT ON NULL"
  })
  void nullKeysKeepHeapAndRetainedMapFailureBoundaries(boolean duplicate, String policy) {
    var rows = new ArrayList<Row>();
    rows.add(Row.of(1, null, "first"));
    if (duplicate) rows.add(Row.of(1, null, "second"));
    var comparison =
        NativeFailureParity.run(
            () -> environment(null, false, false, rows),
            "SELECT k,JSON_OBJECTAGG(jk VALUE v " + policy + ") FROM src GROUP BY k");
    if (!duplicate && tech.streamfusion.compat.JsonRuntimeCompat.LEGACY_SQL_JSON) {
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      return;
    }
    assertNotNull(comparison.host().failure());
    assertNotNull(comparison.nativeRun().failure());
    assertEquals(
        comparison.host().rootCause().getClass(), comparison.nativeRun().rootCause().getClass());
    assertEquals(comparison.host().phase(), comparison.nativeRun().phase());
    assertTrue(comparison.nativeRun().plan().contains("NativeColumnarLocalGroupAggregate"));
  }

  @ParameterizedTest
  @CsvSource({
    "hashmap,false",
    "hashmap,true",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,false",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,true"
  })
  void twoRestoresKeepMergedObjectsAndRecordCounts(String backend, boolean updating)
      throws Exception {
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 96; i++) {
      int id = updating && i >= 64 ? i - 64 : i;
      rows.add(
          Row.ofKind(
              updating && i >= 64 ? RowKind.DELETE : RowKind.INSERT,
              id % 3,
              "k" + id,
              id % 7 == 0 ? null : "é🚀" + id));
    }
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows,
                Types.ROW_NAMED(
                    new String[] {"k", "jk", "v"}, Types.INT, Types.STRING, Types.STRING),
                Schema.newBuilder()
                    .column("k", org.apache.flink.table.api.DataTypes.INT())
                    .column("jk", org.apache.flink.table.api.DataTypes.STRING())
                    .column("v", org.apache.flink.table.api.DataTypes.STRING())
                    .build(),
                updating,
                24,
                72)
            .withExecutionMetrics()) {
      var comparison =
          NativeFailureParity.runRecovery(
              () -> configure(recovery.uninterrupted(), true),
              () -> configure(recovery.get(), true),
              "SELECT k,JSON_OBJECTAGG(jk VALUE v),JSON_OBJECTAGG(jk VALUE v ABSENT ON"
                  + " NULL),COUNT(*) FROM recovery_input GROUP BY k");
      assertNull(comparison.host().failure(), comparison::toString);
      assertNull(comparison.nativeRun().failure(), comparison::toString);
      assertEquals(NativeFailureParity.Route.NATIVE, comparison.nativeRun().route());
      assertEquals(
          SqlAuditHarness.materialized(comparison.host().rows()),
          SqlAuditHarness.materialized(comparison.nativeRun().rows()));
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarLocalGroupAggregate");
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
    }
  }

  private static StreamTableEnvironment environment(
      RecoveryExecutionMetrics metrics, boolean updating, boolean retained, List<Row> rows) {
    var env =
        metrics == null
            ? StreamExecutionEnvironment.getExecutionEnvironment()
            : metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    var source =
        env.fromCollection(
            rows,
            Types.ROW_NAMED(new String[] {"k", "jk", "v"}, Types.INT, Types.STRING, Types.STRING));
    table.createTemporaryView(
        "src",
        updating
            ? table.fromChangelogStream(source, Schema.newBuilder().build(), ChangelogMode.all())
            : table.fromDataStream(source));
    return (StreamTableEnvironment) configure(table, retained);
  }

  private static org.apache.flink.table.api.TableEnvironment configure(
      org.apache.flink.table.api.TableEnvironment table, boolean retained) {
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("table.exec.mini-batch.size", "3");
    table.getConfig().set("table.exec.state.ttl", retained ? "1 h" : "0 ms");
    table.getConfig().set("streamfusion.transpose.batchRows", "2");
    return table;
  }
}

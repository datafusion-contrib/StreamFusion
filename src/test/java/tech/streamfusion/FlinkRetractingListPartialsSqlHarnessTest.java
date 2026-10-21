package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkRetractingListPartialsSqlHarnessTest {
  static Stream<Arguments> profiles() {
    return Stream.of(",", "::", "", "é🚀")
        .flatMap(
            separator ->
                Stream.of(false, true)
                    .flatMap(
                        retained ->
                            Stream.of(2, 7)
                                .map(bundle -> Arguments.of(separator, retained, bundle))));
  }

  @ParameterizedTest
  @MethodSource("profiles")
  void orderedListsFiltersAndTrailingPartialsCompose(
      String separator, boolean retained, int bundle) {
    var rows =
        List.of(
            Row.of(1, "a", 1L, true),
            Row.of(1, "b", 2L, true),
            Row.of(1, "a", 1L, true),
            Row.of(2, null, 3L, false),
            Row.of(2, "", 4L, true),
            Row.of(null, "quote\"", 5L, true),
            Row.ofKind(RowKind.DELETE, 1, "a", 1L, true),
            Row.of(1, "a", 1L, true),
            Row.ofKind(RowKind.DELETE, 2, "", 4L, true),
            Row.of(2, "c", 4L, false),
            Row.of(1, "é🚀\0", 2L, true));
    try (var metrics = new RecoveryExecutionMetrics()) {
      var comparison =
          NativeFailureParity.run(
              () -> environment(rows, metrics, retained, bundle),
              "SELECT k,LISTAGG(v),COUNT(*),LISTAGG(v,'"
                  + separator
                  + "') FILTER(WHERE keep_row),MIN(v),COUNT(DISTINCT v),SUM(n) FROM src GROUP BY"
                  + " k");
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
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void emptyScalarAndGroupedListsKeepReleasedNulls(boolean grouped) {
    var comparison =
        NativeFailureParity.run(
            () -> environment(List.of(), null, false, 3),
            "SELECT "
                + (grouped ? "k," : "")
                + "LISTAGG(v),LISTAGG(v,'::'),COUNT(*) FROM src"
                + (grouped ? " GROUP BY k" : ""));
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
  }

  @ParameterizedTest
  @CsvSource({
    "hashmap,false",
    "hashmap,true",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,false",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,true"
  })
  void twoRestoresKeepOrderedOccurrencesAndSignedRecordCounts(String backend, boolean retained)
      throws Exception {
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 96; i++) {
      int original = i >= 64 ? i - 64 : i;
      rows.add(
          Row.ofKind(
              i >= 64 ? RowKind.DELETE : RowKind.INSERT,
              original % 3,
              original % 7 == 0 ? null : "é🚀" + original % 5,
              original % 11L,
              original % 5 != 0));
    }
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows,
                Types.ROW_NAMED(
                    new String[] {"k", "v", "n", "keep_row"},
                    Types.INT,
                    Types.STRING,
                    Types.LONG,
                    Types.BOOLEAN),
                Schema.newBuilder()
                    .column("k", org.apache.flink.table.api.DataTypes.INT())
                    .column("v", org.apache.flink.table.api.DataTypes.STRING())
                    .column("n", org.apache.flink.table.api.DataTypes.BIGINT())
                    .column("keep_row", org.apache.flink.table.api.DataTypes.BOOLEAN())
                    .build(),
                true,
                24,
                72)
            .withExecutionMetrics()) {
      var comparison =
          NativeFailureParity.runRecovery(
              () -> configure(recovery.get(), retained, 7),
              () -> configure(recovery.get(), retained, 7),
              "SELECT k,LISTAGG(v),LISTAGG(v,'::') FILTER(WHERE keep_row),COUNT(*),SUM(n) FROM"
                  + " recovery_input GROUP BY k");
      assertNull(comparison.host().failure(), comparison::toString);
      assertNull(comparison.nativeRun().failure(), comparison::toString);
      assertEquals(NativeFailureParity.Route.NATIVE, comparison.nativeRun().route());
      assertEquals(
          SqlAuditHarness.materialized(comparison.host().rows()),
          SqlAuditHarness.materialized(comparison.nativeRun().rows()));
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      assertEquals(2, recovery.observations().size());
      for (var proof : recovery.observations()) {
        assertEquals(List.of(24, 72), proof.get("restoredOffsets"));
        assertEquals(java.util.Set.of(24, 72), proof.get("injectedOffsets"));
        assertEquals(0, proof.get("activeSources"));
      }
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarLocalGroupAggregate");
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
    }
  }

  private static StreamTableEnvironment environment(
      List<Row> rows, RecoveryExecutionMetrics metrics, boolean retained, int bundle) {
    var env =
        metrics == null
            ? StreamExecutionEnvironment.getExecutionEnvironment()
            : metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    var source =
        env.fromCollection(
            rows,
            Types.ROW_NAMED(
                new String[] {"k", "v", "n", "keep_row"},
                Types.INT,
                Types.STRING,
                Types.LONG,
                Types.BOOLEAN));
    table.createTemporaryView(
        "src", table.fromChangelogStream(source, Schema.newBuilder().build(), ChangelogMode.all()));
    return (StreamTableEnvironment) configure(table, retained, bundle);
  }

  private static TableEnvironment configure(TableEnvironment table, boolean retained, int bundle) {
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.size", Integer.toString(bundle));
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("table.exec.state.ttl", retained ? "1 h" : "0 ms");
    table.getConfig().set("streamfusion.transpose.batchRows", "2");
    return table;
  }
}

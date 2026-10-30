package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class FlinkCollectionAggregateStringKeySqlHarnessTest {
  private final RecoveryExecutionMetrics metrics = new RecoveryExecutionMetrics();

  @AfterEach
  void closeCluster() {
    metrics.close();
  }

  private static Schema schema() {
    return Schema.newBuilder()
        .column("k", DataTypes.STRING())
        .column("v", DataTypes.BIGINT())
        .build();
  }

  private static List<Row> rows() {
    String first = "collision-10875", second = "collision-18154";
    assertEquals(
        org.apache.flink.table.data.binary.BinaryStringData.fromString(first).hashCode(),
        org.apache.flink.table.data.binary.BinaryStringData.fromString(second).hashCode());
    String[] keys = {
      null, "", first, second, "é🙂".repeat(128) + "\u0000", "\ud800", "\udc00", "?"
    };
    var rows = new ArrayList<Row>();
    var kinds =
        new RowKind[] {
          RowKind.INSERT,
          RowKind.INSERT,
          RowKind.UPDATE_BEFORE,
          RowKind.UPDATE_AFTER,
          RowKind.DELETE,
          RowKind.DELETE,
          RowKind.INSERT
        };
    for (int cycle = 0; cycle < 2; cycle++) {
      for (int phase = 0; phase < kinds.length; phase++) {
        for (String key : keys)
          rows.add(
              Row.ofKind(
                  kinds[phase],
                  key,
                  phase == 1 || phase == 5
                      ? null
                      : phase == 3 || phase == 4 ? 20L : phase == 6 ? 30L : 10L));
      }
    }
    return rows;
  }

  private TableEnvironment environment(int parallelism) {
    var env = metrics.environment(new Configuration(), parallelism, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    table.createTemporaryView(
        "src",
        table.fromChangelogStream(
            tech.streamfusion.compat.FlinkTestSources.fromData(
                env, rows(), Types.ROW_NAMED(new String[] {"k", "v"}, Types.STRING, Types.LONG)),
            schema()));
    return table;
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,1,false",
    "COLLECT,3,false",
    "COLLECT,1,true",
    "ARRAY_AGG,1,false",
    "ARRAY_AGG,3,false",
    "ARRAY_AGG,1,true"
  })
  void characterKeysKeepReleasedUnicodeCollisionsAndOrderedChanges(
      String function, int parallelism, boolean fixed) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    String key = fixed ? "CAST(k AS CHAR(8))" : "k";
    String sql =
        "SELECT "
            + key
            + ","
            + function
            + "(v),"
            + function
            + "(DISTINCT v),COUNT(*) FROM src GROUP BY "
            + key;
    var previous =
        metrics.nativeOperators().stream()
            .map(row -> row.get("jobId"))
            .collect(java.util.stream.Collectors.toSet());
    var runs = NativeFailureParity.run(() -> environment(parallelism), sql);
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(perKey(runs.host().rows()), perKey(runs.nativeRun().rows()));
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    RecoveryExecutionMetrics.assertConsumed(
        metrics.nativeOperators().stream()
            .filter(row -> !previous.contains(row.get("jobId")))
            .collect(java.util.stream.Collectors.toList()),
        "NativeColumnarGroupAggregate");
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,hashmap",
    "COLLECT,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "ARRAY_AGG,hashmap",
    "ARRAY_AGG,tech.streamfusion.state.RocksDBNativeStateBackendFactory"
  })
  void characterKeyStateRestoresAcrossTwoNonemptyPrefixes(String function, String backend)
      throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows(),
                Types.ROW_NAMED(new String[] {"k", "v"}, Types.STRING, Types.LONG),
                schema(),
                true,
                16,
                64)
            .withExecutionMetrics()) {
      String sql =
          "SELECT k,"
              + function
              + "(v),"
              + function
              + "(DISTINCT v),COUNT(*) FROM recovery_input GROUP BY k";
      var runs =
          NativeFailureParity.runRecovery(
              () -> onePhase(recovery.uninterrupted()), () -> onePhase(recovery.get()), sql);
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(perKey(runs.host().rows()), perKey(runs.nativeRun().rows()));
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
      SharedFlinkCluster.assertNativeMemoryReleased("collection-string-key-recovery");
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"COLLECT", "ARRAY_AGG"})
  void serializedKeysFeedCharacterConsumersWithoutChangingCounts(String function) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    var previous =
        metrics.nativeOperators().stream()
            .map(row -> row.get("jobId"))
            .collect(java.util.stream.Collectors.toSet());
    var runs =
        NativeFailureParity.run(
            () -> environment(1),
            "SELECT k,CHAR_LENGTH(k),CAST(" + function + "(v) AS STRING) FROM src GROUP BY k");
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(perKey(runs.host().rows()), perKey(runs.nativeRun().rows()));
    var work =
        metrics.nativeOperators().stream()
            .filter(row -> !previous.contains(row.get("jobId")))
            .collect(java.util.stream.Collectors.toList());
    RecoveryExecutionMetrics.assertConsumed(work, "NativeColumnarGroupAggregate");
    RecoveryExecutionMetrics.assertConsumed(work, "NativeCalc");
  }

  private static TableEnvironment onePhase(TableEnvironment table) {
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    return table;
  }

  private static java.util.Map<Object, List<List<Object>>> perKey(List<List<Object>> rows) {
    var result = new java.util.HashMap<Object, List<List<Object>>>();
    for (var row : rows) result.computeIfAbsent(row.get(1), ignored -> new ArrayList<>()).add(row);
    return result;
  }
}

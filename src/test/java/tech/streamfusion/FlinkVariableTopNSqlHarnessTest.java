package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkVariableTopNSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "k",
        "MOD(k, 3) + 1",
        "CAST(MOD(k, 3) + 1 AS INT)",
        "CAST(MOD(k, 3) + 1 AS SMALLINT)"
      })
  void partitionDerivedBoundsMatchOrderedChangelog(String bound) throws Exception {
    for (boolean outputRank : new boolean[] {false, true}) {
      String sql = query(bound, outputRank);
      Supplier<TableEnvironment> input = () -> environment(false, false);
      String plan = NativePlanner.explain(input.get(), sql);
      assertTrue(plan.contains("NativeColumnarTopN"), plan);
      NativeParity.assertOrderedKindedParity(input, sql);
      NativeParity.assertChangelogParity(input, sql);
    }
  }

  static Stream<Arguments> computedPartitions() {
    return Stream.of(
        Arguments.of("MOD(k, 3)", "MOD(k, 3) + 1"),
        Arguments.of("k + 1", "MOD(k + 1, 3) + 1"),
        Arguments.of("CAST(MOD(k, 3) AS INT)", "CAST(MOD(k, 3) AS INT) + 1"),
        Arguments.of("COALESCE(v, 0)", "MOD(COALESCE(v, 0), 3) + 1"),
        Arguments.of("MOD(k, 3), MOD(id, 2)", "MOD(k, 3) + MOD(id, 2) + 1"));
  }

  @ParameterizedTest
  @MethodSource("computedPartitions")
  void computedPartitionBoundsMatchOrderedChangelog(String partition, String bound)
      throws Exception {
    for (boolean retracting : new boolean[] {false, true}) {
      for (boolean outputRank : new boolean[] {false, true}) {
        String sql =
            query(bound, outputRank).replace("PARTITION BY k", "PARTITION BY " + partition);
        Supplier<TableEnvironment> input =
            () -> retracting ? retractingEnvironment(false) : environment(false, false);
        String plan = NativePlanner.explain(input.get(), sql);
        assertTrue(plan.contains("NativeColumnarTopN"), plan);
        NativeParity.assertOrderedKindedParity(input, sql);
        NativeParity.assertChangelogParity(input, sql);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("computedPartitions")
  void computedPartitionBoundsKeepMiniBatchMaterialization(String partition, String bound)
      throws Exception {
    String sql = query(bound, true).replace("PARTITION BY k", "PARTITION BY " + partition);
    NativeParity.assertChangelogParity(() -> environment(false, true), sql);
    NativeParity.assertChangelogParity(() -> retractingEnvironment(true), sql);
  }

  @ParameterizedTest
  @ValueSource(strings = {"k", "MOD(k, 4) + 1", "id"})
  void changingBoundsAcrossCollapsedPartitionsMatchOrderedChangelog(String bound) throws Exception {
    for (boolean rank : new boolean[] {false, true}) {
      String sql = query(bound, rank).replace("PARTITION BY k", "PARTITION BY MOD(k, 3)");
      String plan = NativePlanner.explain(retractingEnvironment(false), sql);
      assertTrue(plan.contains("NativeColumnarTopN"), plan);
      NativeParity.assertOrderedKindedParity(() -> retractingEnvironment(false), sql);
      NativeParity.assertChangelogParity(() -> retractingEnvironment(true), sql);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory"})
  void computedPartitionBoundsSurviveSqlRecovery(String backend) throws Exception {
    for (boolean grouped : new boolean[] {false, true}) {
      String input =
          grouped ? "(SELECT k, SUM(v) AS v FROM recovery_input GROUP BY k)" : "recovery_input";
      String sql =
          "SELECT k, v, rn FROM (SELECT k, v, MOD(COALESCE(k, 0), 2) + 1 AS rank_end,"
              + " ROW_NUMBER() OVER (PARTITION BY MOD(COALESCE(k, 0), 2) ORDER BY v DESC, k ASC)"
              + " AS rn FROM "
              + input
              + ") WHERE rn <= rank_end";
      try (var recovery = new PortableSqlRecovery(backend)) {
        NativeParity.assertChangelogParity(recovery, sql);
        recovery.verify();
      }
    }
  }

  @Test
  void deterministicUdfBoundsUseFirstBoundState() throws Exception {
    Supplier<TableEnvironment> input =
        () -> {
          TableEnvironment table = retractingEnvironment(false);
          table.createTemporarySystemFunction("custom_bound", BoundFunction.class);
          return table;
        };
    NativeParity.assertOrderedKindedParity(
        input,
        query("MOD(custom_bound(k), 3) + 1", true)
            .replace("PARTITION BY k", "PARTITION BY custom_bound(k)"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory"})
  void changingRetractingBoundsSurviveSqlRecovery(String backend) throws Exception {
    for (boolean rank : new boolean[] {false, true}) {
      String sql = changingGroupedQuery(rank);
      var planInput = environment(false, false);
      planInput.executeSql("CREATE TEMPORARY VIEW recovery_input AS SELECT k, v FROM src");
      String plan = NativePlanner.explain(planInput, sql);
      assertTrue(plan.contains("NativeColumnarTopN"), plan);
      try (var recovery = new PortableSqlRecovery(backend)) {
        NativeParity.assertChangelogParity(recovery, sql);
        recovery.verify();
      }
    }
  }

  @Test
  void changingBoundsOverMiniBatchAggregatesRequireStableInputOrder() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> {
          var table = environment(false, true);
          table.executeSql("CREATE TEMPORARY VIEW recovery_input AS SELECT k, v FROM src");
          return table;
        },
        changingGroupedQuery(true),
        "changing bounds require unchanged upstream mini-batch changelog order");
  }

  private static String changingGroupedQuery(boolean rank) {
    return "SELECT k, v"
        + (rank ? ", rn" : "")
        + " FROM (SELECT k, v, MOD(COALESCE(v, 0), 5) + 1 AS rank_end,"
        + " ROW_NUMBER() OVER (PARTITION BY MOD(COALESCE(k, 0), 2) ORDER BY v DESC, k ASC)"
        + " AS rn FROM (SELECT k, SUM(v) AS v FROM recovery_input GROUP BY k))"
        + " WHERE rn <= rank_end";
  }

  public static class BoundFunction extends ScalarFunction {
    @DataTypeHint("BIGINT NOT NULL")
    public long eval(long value) {
      return value;
    }
  }

  @Test
  void nullableComputedBoundRetainsHostRowAccess() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> environment(false, false),
        query("MOD(v, 3) + 1", true).replace("PARTITION BY k", "PARTITION BY MOD(v, 3)"),
        "nullable variable rank bounds require Flink's row-access semantics");
  }

  @Test
  void largeBoundsKeepFlinkTieAdmission() throws Exception {
    for (boolean outputRank : new boolean[] {false, true}) {
      String sql = query("k", outputRank).replace("v ASC NULLS FIRST, id ASC", "v ASC NULLS FIRST");
      NativeParity.assertOrderedKindedParity(() -> environment(false, false), sql);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"k", "MOD(k, 3) + 1"})
  void miniBatchRetainsPerPartitionBounds(String bound) throws Exception {
    NativeParity.assertChangelogParity(() -> environment(false, true), query(bound, true));
  }

  @Test
  void nullableBoundsRetainHostRowAccess() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> environment(true, false),
        query("k", true),
        "nullable variable rank bounds require Flink's row-access semantics");
  }

  @Test
  void independentlyChangingAppendOnlyBoundsUseTheFirstValue() throws Exception {
    for (boolean rank : new boolean[] {false, true}) {
      NativeParity.assertOrderedKindedParity(() -> environment(false, false), query("id", rank));
      NativeParity.assertChangelogParity(() -> environment(false, true), query("id", rank));
    }
  }

  @Test
  void updatingBoundsUseTheFirstProposal() throws Exception {
    String sql =
        "SELECT k, n, rn FROM (SELECT k, n, ROW_NUMBER() OVER (ORDER BY n DESC) AS rn FROM (SELECT"
            + " k, COUNT(*) AS n FROM src GROUP BY k)) WHERE rn <= k";
    NativeParity.assertOrderedKindedParity(() -> environment(false, false), sql);
    NativeParity.assertChangelogParity(() -> environment(false, false), sql);
    NativeParity.assertFallbackReasonContains(
        () -> environment(false, true), sql,
        "changing bounds require unchanged upstream mini-batch changelog order");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "1",
        "2",
        "id",
        "CAST(MOD(id, 3) AS INT)",
        "CAST(MOD(id, 3) AS SMALLINT)",
        "k",
        "MOD(k, 3) + 1",
        "CAST(MOD(k, 3) + 1 AS INT)",
        "CAST(MOD(k, 3) + 1 AS SMALLINT)"
      })
  void retractingPartitionBoundsMatchOrderedChangelog(String bound) throws Exception {
    for (boolean outputRank : new boolean[] {false, true}) {
      String sql = query(bound, outputRank);
      Supplier<TableEnvironment> input = () -> retractingEnvironment(false);
      String plan = NativePlanner.explain(input.get(), sql);
      assertTrue(plan.contains("NativeColumnarTopN"), plan);
      NativeParity.assertOrderedKindedParity(input, sql);
      NativeParity.assertChangelogParity(input, sql);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"k", "MOD(k, 3) + 1", "id"})
  void retractingMiniBatchKeepsIndependentPartitionBounds(String bound) throws Exception {
    for (boolean outputRank : new boolean[] {false, true}) {
      NativeParity.assertChangelogParity(
          () -> retractingEnvironment(true), query(bound, outputRank));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"k", "id"})
  void retractingTiedSortKeysMatchOrderedChangelog(String bound) throws Exception {
    for (boolean outputRank : new boolean[] {false, true}) {
      NativeParity.assertOrderedKindedParity(
          () -> retractingEnvironment(false), query(bound, outputRank).replace(", id ASC", ""));
    }
  }

  private static TableEnvironment retractingEnvironment(boolean miniBatch) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    if (miniBatch) {
      table.getConfig().set("table.exec.mini-batch.enabled", "true");
      table.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
      table.getConfig().set("table.exec.mini-batch.size", "37");
    }
    List<Row> rows = new ArrayList<>();
    for (long k : new long[] {-4, -2, -1, 0, 1, 2, 3, 200}) {
      rows.add(Row.ofKind(RowKind.INSERT, k, 50L, 1L));
      rows.add(Row.ofKind(RowKind.INSERT, k, 20L, 2L));
      rows.add(Row.ofKind(RowKind.INSERT, k, 30L, 3L));
      rows.add(Row.ofKind(RowKind.INSERT, k, 30L, 3L));
      rows.add(Row.ofKind(RowKind.UPDATE_BEFORE, k, 20L, 2L));
      rows.add(Row.ofKind(RowKind.UPDATE_AFTER, k, 70L, 2L));
      rows.add(Row.ofKind(RowKind.DELETE, k, 50L, 1L));
      rows.add(Row.ofKind(RowKind.INSERT, k, null, 4L));
      rows.add(Row.ofKind(RowKind.INSERT, k, null, 5L));
      rows.add(Row.ofKind(RowKind.DELETE, k, null, 4L));
      rows.add(Row.ofKind(RowKind.DELETE, k, null, 5L));
      rows.add(Row.ofKind(RowKind.DELETE, k, 30L, 3L));
      rows.add(Row.ofKind(RowKind.DELETE, k, 30L, 3L));
      rows.add(Row.ofKind(RowKind.DELETE, k, 70L, 2L));
      rows.add(Row.ofKind(RowKind.INSERT, k, 10L, 6L));
    }
    for (int i = 0; i < 210; i++) rows.add(Row.ofKind(RowKind.INSERT, 200L, 1000L + i, 100L + i));
    for (int i = 0; i < 20; i++) rows.add(Row.ofKind(RowKind.DELETE, 200L, 1000L + i, 100L + i));
    var source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v", "id"}, Types.LONG, Types.LONG, Types.LONG),
            rows.toArray(Row[]::new));
    table.createTemporaryView(
        "src",
        table.fromChangelogStream(
            source,
            Schema.newBuilder()
                .column("k", DataTypes.BIGINT().notNull())
                .column("v", DataTypes.BIGINT())
                .column("id", DataTypes.BIGINT().notNull())
                .build()));
    return table;
  }

  private static String query(String bound, boolean outputRank) {
    return "SELECT k, v, id"
        + (outputRank ? ", rn" : "")
        + " FROM ("
        + "SELECT k, v, id, "
        + bound
        + " AS rank_end, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v ASC NULLS FIRST, id ASC) AS"
        + " rn FROM src) WHERE rn <= rank_end";
  }

  private static TableEnvironment environment(boolean nullable, boolean miniBatch) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    if (miniBatch) {
      table.getConfig().set("table.exec.mini-batch.enabled", "true");
      table.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
      table.getConfig().set("table.exec.mini-batch.size", "37");
    }
    List<Row> rows = new ArrayList<>();
    long id = 0;
    for (long key : new long[] {Long.MIN_VALUE, -4, -3, -2, -1, 0, 1, 2, 3, 200, Long.MAX_VALUE}) {
      for (Long value : new Long[] {50L, 30L, null, 30L, 10L, 60L}) {
        rows.add(Row.of(key, value, id++));
      }
    }
    // Flink uses a 100-row admission threshold for variable bounds, including bounds above 100.
    for (int i = 0; i < 210; i++) {
      rows.add(Row.of(200L, 1000L + i, id++));
    }
    for (int i = 0; i < 110; i++) {
      rows.add(Row.of(200L, 999L - i, id++));
    }
    for (int i = 0; i < 150; i++) {
      rows.add(Row.of(300L, 7L, id++));
    }
    if (nullable) rows.add(Row.of(null, 1L, id));
    var source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v", "id"}, Types.LONG, Types.LONG, Types.LONG),
            rows.toArray(Row[]::new));
    table.createTemporaryView(
        "src",
        table.fromDataStream(
            source,
            Schema.newBuilder()
                .column("k", nullable ? DataTypes.BIGINT() : DataTypes.BIGINT().notNull())
                .column("v", DataTypes.BIGINT())
                .column("id", DataTypes.BIGINT().notNull())
                .build()));
    return table;
  }
}

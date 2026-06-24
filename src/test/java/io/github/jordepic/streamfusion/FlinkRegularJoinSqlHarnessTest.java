package io.github.jordepic.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

/**
 * Regular (non-windowed) INNER equi-join ({@code a JOIN b ON a.k = b.k}) matches the host. The native
 * updating join keeps a per-side keyed multiset and emits the join changelog; over append-only
 * inputs that is insert-only, and over changelog inputs (e.g. two aggregations joined) it retracts a
 * matched pair when either side retracts. The parity harness compares the full set of emitted change
 * rows, so a differing changelog fails.
 */
class FlinkRegularJoinSqlHarnessTest {

  @Test
  void innerJoinOfAppendStreamsMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkRegularJoinSqlHarnessTest::environment,
        "SELECT a.k, a.v, b.w FROM A AS a JOIN B AS b ON a.k = b.k");
  }

  @Test
  void innerJoinOfChangelogStreamsMatchesHost() throws Exception {
    // Both sides are GROUP BY results (changelogs); the join consumes and emits retractions. A
    // two-input join's raw changelog is interleaving-dependent (transient pairings vary run to run),
    // but its net materialized result is deterministic — so compare the collapsed changelog.
    NativeParity.assertChangelogParity(
        FlinkRegularJoinSqlHarnessTest::environment,
        "SELECT la.k, la.sv, rb.sw FROM "
            + "(SELECT k, SUM(v) AS sv FROM A GROUP BY k) la JOIN "
            + "(SELECT k, SUM(w) AS sw FROM B GROUP BY k) rb ON la.k = rb.k");
  }

  @Test
  void leftJoinDoesNotRoute() throws Exception {
    // An outer join is not INNER, so the native join declines it (and its non-deterministic
    // null-padded changelog makes a result comparison unsound — hence a routing-only check).
    assertEquals(
        0,
        substitutions("SELECT a.k, a.v, b.w FROM A AS a LEFT JOIN B AS b ON a.k = b.k"),
        "outer join unexpectedly routed to native");
  }

  /** Installs native substitution, drives the query to completion, and returns the substitution count. */
  private static int substitutions(String sql) throws Exception {
    TableEnvironment tEnv = environment();
    PhysicalPlanScan scan = NativePlanner.install(tEnv);
    try (CloseableIterator<Row> it = tEnv.executeSql(sql).collect()) {
      while (it.hasNext()) {
        it.next();
      }
    }
    return scan.substitutions();
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");

    DataStream<Row> a =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG),
            Row.of(1L, 10L),
            Row.of(1L, 20L),
            Row.of(2L, 30L));
    DataStream<Row> b =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "w"}, Types.LONG, Types.LONG),
            Row.of(1L, 100L),
            Row.of(2L, 200L),
            Row.of(2L, 300L),
            Row.of(3L, 400L));
    tEnv.createTemporaryView(
        "A",
        a,
        Schema.newBuilder().column("k", DataTypes.BIGINT()).column("v", DataTypes.BIGINT()).build());
    tEnv.createTemporaryView(
        "B",
        b,
        Schema.newBuilder().column("k", DataTypes.BIGINT()).column("w", DataTypes.BIGINT()).build());
    return tEnv;
  }
}

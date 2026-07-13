package io.github.jordepic.streamfusion;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

/**
 * Regular (non-windowed) equi-joins match the host — INNER, LEFT/RIGHT/FULL outer, and SEMI/ANTI. The
 * native updating join keeps a per-side keyed multiset and, for the outer/semi/anti families, a
 * per-row match-degree, emitting the join changelog (null-padded outer rows, bare semi/anti rows). An
 * INNER append join is insert-only; outer/anti joins retract null-pads as matches arrive, so their
 * output is a changelog even over append-only inputs. Where the output is a changelog the collapsed
 * (net materialized) result is compared, since a two-input join's raw changelog is
 * interleaving-dependent but its materialization is deterministic.
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
  void uniqueKeyUpdatingJoinUnderMiniBatchMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        () -> {
          TableEnvironment tEnv = environment();
          tEnv.getConfig().set("table.exec.mini-batch.enabled", "true");
          tEnv.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
          tEnv.getConfig().set("table.exec.mini-batch.size", "4");
          return tEnv;
        },
        "SELECT la.k, la.sv, rb.sw FROM "
            + "(SELECT k, SUM(v) AS sv FROM A GROUP BY k) la JOIN "
            + "(SELECT k, SUM(w) AS sw FROM B GROUP BY k) rb ON la.k = rb.k");
  }

  @Test
  void leftJoinMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkRegularJoinSqlHarnessTest::environment,
        "SELECT a.k, a.v, b.w FROM A AS a LEFT JOIN B AS b ON a.k = b.k");
  }

  @Test
  void rightJoinMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkRegularJoinSqlHarnessTest::environment,
        "SELECT a.k, a.v, b.w FROM A AS a RIGHT JOIN B AS b ON a.k = b.k");
  }

  @Test
  void fullJoinMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkRegularJoinSqlHarnessTest::environment,
        "SELECT a.k, a.v, b.w FROM A AS a FULL JOIN B AS b ON a.k = b.k");
  }

  @Test
  void semiJoinMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkRegularJoinSqlHarnessTest::environment,
        "SELECT a.k, a.v FROM A AS a WHERE EXISTS (SELECT 1 FROM B AS b WHERE b.k = a.k)");
  }

  @Test
  void antiJoinMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkRegularJoinSqlHarnessTest::environment,
        "SELECT a.k, a.v FROM A AS a WHERE NOT EXISTS (SELECT 1 FROM B AS b WHERE b.k = a.k)");
  }

  @Test
  void innerJoinWithNonEquiPredicateMatchesHost() throws Exception {
    // A residual non-equi condition (a.v < b.w) gates which same-key pairs match; an INNER append
    // join stays insert-only.
    NativeParity.assertParity(
        FlinkRegularJoinSqlHarnessTest::environment,
        "SELECT a.k, a.v, b.w FROM A AS a JOIN B AS b ON a.k = b.k AND a.v < b.w");
  }

  @Test
  void leftJoinWithNonEquiPredicateMatchesHost() throws Exception {
    // The predicate (a.v > b.w) holds for no pair here, so every left row is null-padded — exercising
    // the degree and the residual predicate together. The output is a changelog.
    NativeParity.assertChangelogParity(
        FlinkRegularJoinSqlHarnessTest::environment,
        "SELECT a.k, a.v, b.w FROM A AS a LEFT JOIN B AS b ON a.k = b.k AND a.v > b.w");
  }

  @Test
  void leftJoinOfChangelogStreamsMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkRegularJoinSqlHarnessTest::environment,
        "SELECT la.k, la.sv, rb.sw FROM "
            + "(SELECT k, SUM(v) AS sv FROM A GROUP BY k) la LEFT JOIN "
            + "(SELECT k, SUM(w) AS sw FROM B GROUP BY k) rb ON la.k = rb.k");
  }

  @Test
  void fullJoinOfChangelogStreamsMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkRegularJoinSqlHarnessTest::environment,
        "SELECT la.k, la.sv, rb.sw FROM "
            + "(SELECT k, SUM(v) AS sv FROM A GROUP BY k) la FULL JOIN "
            + "(SELECT k, SUM(w) AS sw FROM B GROUP BY k) rb ON la.k = rb.k");
  }

  /**
   * A self-join reads the same view twice; sub-plan reuse (enabled under the native planner, with
   * native nodes digest-barriered) shares the rowwise prefix while each branch keeps its own entry
   * transpose. Parity plus the suite's native leak check pin the fan-out down: shared rowwise
   * records must not corrupt either island, and no Arrow batch may be freed twice.
   */
  @Test
  void selfJoinOverSharedRowwisePrefixMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkRegularJoinSqlHarnessTest::environment,
        "SELECT a.k, a.v + b.v FROM A a JOIN A b ON a.k = b.k");
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

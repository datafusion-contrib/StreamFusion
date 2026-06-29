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
 * Parity tests for the non-windowed {@code GROUP BY} aggregate: the native operator emits a
 * changelog ({@code +I}, then {@code -U}/{@code +U} as a key's result changes) that must match the
 * host's exactly. The harness compares the full set of emitted change rows, so a differing changelog
 * (extra, missing, or wrongly-valued retraction) fails.
 */
class FlinkGroupAggregateSqlHarnessTest {

  @Test
  void groupBySumMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkGroupAggregateSqlHarnessTest::environment,
        "SELECT k, SUM(`value`) AS s FROM src GROUP BY k");
  }

  @Test
  void groupByCountMinMaxSumMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkGroupAggregateSqlHarnessTest::environment,
        "SELECT k, COUNT(*) AS c, MIN(`value`) AS mn, MAX(`value`) AS mx, SUM(`value`) AS s "
            + "FROM src GROUP BY k");
  }

  @Test
  void groupByStringKeyMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkGroupAggregateSqlHarnessTest::environment,
        "SELECT s, SUM(`value`) AS sm, COUNT(*) AS c FROM src GROUP BY s");
  }

  @Test
  void globalAggregateMatchesHost() throws Exception {
    // No GROUP BY: a single global group, still a retracting changelog (+I then -U/+U per row).
    NativeParity.assertParity(
        FlinkGroupAggregateSqlHarnessTest::environment,
        "SELECT SUM(`value`) AS s, COUNT(*) AS c FROM src");
  }

  @Test
  void groupByIntAndDoubleMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkGroupAggregateSqlHarnessTest::environment,
        "SELECT k, SUM(qty) AS sq, SUM(price) AS sp FROM src GROUP BY k");
  }

  @Test
  void avgMatchesHost() throws Exception {
    // AVG runs natively as a (sum, count) running state: BIGINT integer division (truncating), INT
    // also integer (cast back to INT), DOUBLE floating — each matching Flink's AvgAggFunction.
    NativeParity.assertParity(
        FlinkGroupAggregateSqlHarnessTest::environment,
        "SELECT k, AVG(`value`) AS a, AVG(qty) AS aq, AVG(price) AS ap FROM src GROUP BY k");
  }

  @Test
  void avgOverRetractingInputMatchesHost() throws Exception {
    // The inner GROUP BY emits a changelog; the outer AVG consumes it, retracting old totals from its
    // running sum/count and adding new ones — the average tracks the live set.
    NativeParity.assertChangelogParity(
        FlinkGroupAggregateSqlHarnessTest::environment,
        "SELECT s, AVG(total) AS avg_total FROM "
            + "(SELECT k, s, SUM(`value`) AS total FROM src GROUP BY k, s) GROUP BY s");
  }

  @Test
  void filteredAggregatesMatchHost() throws Exception {
    // COUNT(*)/SUM/COUNT(DISTINCT) with FILTER (WHERE …) — Nexmark q15/q17's shape: each aggregate
    // folds only the rows whose filter is true (a boolean input column the host computes). The range
    // predicate lowers to a SEARCH the encoder expands.
    NativeParity.assertChangelogParity(
        FlinkGroupAggregateSqlHarnessTest::environment,
        "SELECT k, "
            + "COUNT(*) AS total, "
            + "COUNT(*) FILTER (WHERE `value` >= 2 AND `value` < 4) AS mid, "
            + "SUM(`value`) FILTER (WHERE qty > 15) AS sf, "
            + "COUNT(DISTINCT s) FILTER (WHERE `value` >= 2) AS distinct_s "
            + "FROM src GROUP BY k");
  }

  @Test
  void globalAvgMatchesHost() throws Exception {
    // No GROUP BY: a single global AVG, still a retracting changelog (+I then -U/+U per row).
    NativeParity.assertChangelogParity(
        FlinkGroupAggregateSqlHarnessTest::environment, "SELECT AVG(`value`) AS a FROM src");
  }

  @Test
  void sumOverRetractingInputMatchesHost() throws Exception {
    // The inner GROUP BY emits a changelog; the outer SUM consumes it (retracting old per-(k,s)
    // totals and adding new ones). Both route — the outer is the retract-consuming aggregate.
    NativeParity.assertParity(
        FlinkGroupAggregateSqlHarnessTest::environment,
        "SELECT s, SUM(total) AS st FROM "
            + "(SELECT k, s, SUM(`value`) AS total FROM src GROUP BY k, s) GROUP BY s");
  }

  @Test
  void countOverRetractingInputMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkGroupAggregateSqlHarnessTest::environment,
        "SELECT total, COUNT(*) AS n FROM "
            + "(SELECT k, SUM(`value`) AS total FROM src GROUP BY k) GROUP BY total");
  }

  @Test
  void minMaxOverRetractingInputMatchesHost() throws Exception {
    // MIN/MAX over a changelog: each retracts via a per-key value multiset, so the outer aggregate
    // routes natively too (recovering the next extreme when the current one is retracted).
    NativeParity.assertParity(
        FlinkGroupAggregateSqlHarnessTest::environment,
        "SELECT s, MIN(total) AS mn, MAX(total) AS mx FROM "
            + "(SELECT k, s, SUM(`value`) AS total FROM src GROUP BY k, s) GROUP BY s");
  }

  @Test
  void stateTtlFallsBackToHost() throws Exception {
    // With idle-state TTL on, the host refreshes and expires keys (emitting unchanged updates and
    // deletes) — semantics the append-only native operator does not reproduce, so it stays on host.
    // The minimal source has exactly the grouped/aggregated columns, so the aggregate is the only
    // routable node and a clean fallback means zero substitutions.
    NativeParity.assertFallback(
        () -> {
          TableEnvironment tEnv = minimalEnvironment();
          tEnv.getConfig().set("table.exec.state.ttl", "1 h");
          return tEnv;
        },
        "SELECT k, SUM(`value`) AS s FROM kv GROUP BY k");
  }

  private static TableEnvironment minimalEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "value"}, Types.LONG, Types.LONG),
            Row.of(7L, 1L),
            Row.of(7L, 2L),
            Row.of(9L, 3L));
    tEnv.createTemporaryView(
        "kv",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("value", DataTypes.BIGINT())
            .build());
    return tEnv;
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    // One-phase so the plan is a single GROUP BY aggregate (not a local/global split).
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");

    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"k", "s", "value", "qty", "price"},
                Types.LONG,
                Types.STRING,
                Types.LONG,
                Types.INT,
                Types.DOUBLE),
            Row.of(7L, "a", 1L, 10, 1.5),
            Row.of(7L, "a", 2L, 20, 2.5),
            Row.of(9L, "b", 3L, 30, 3.0),
            Row.of(7L, "a", 4L, 40, 4.5),
            Row.of(9L, "b", 5L, 50, 5.5));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("s", DataTypes.STRING())
            .column("value", DataTypes.BIGINT())
            .column("qty", DataTypes.INT())
            .column("price", DataTypes.DOUBLE())
            .build());
    return tEnv;
  }
}

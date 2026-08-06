package tech.streamfusion;

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
  void avgNarrowTypesMatchesHost() throws Exception {
    // AVG over SMALLINT/TINYINT/FLOAT: the sum widens (bigint for the integers, double for float)
    // and the result casts back to the narrow input type — Flink's AvgAggFunction family.
    NativeParity.assertParity(
        FlinkGroupAggregateSqlHarnessTest::environment,
        "SELECT k, AVG(vs) AS avs, AVG(vt) AS avt, AVG(vf) AS avf FROM src GROUP BY k");
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
  void minMaxStringMatchesHost() throws Exception {
    // MIN/MAX over a string column (Nexmark q16's max(DATE_FORMAT(...))) — byte-lexicographic order,
    // a retracting changelog as the per-key extreme changes.
    NativeParity.assertChangelogParity(
        FlinkGroupAggregateSqlHarnessTest::environment,
        "SELECT k, MIN(s) AS mn, MAX(s) AS mx FROM src GROUP BY k");
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
  void aggregateOverAggregateAtParallelismFourMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkGroupAggregateSqlHarnessTest::parallelEnvironment,
        "SELECT total, COUNT(*) AS n FROM "
            + "(SELECT k, SUM(`value`) AS total FROM src GROUP BY k) GROUP BY total");
  }

  @Test
  void intAggregateKeyAtParallelismFourMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkGroupAggregateSqlHarnessTest::parallelEnvironment,
        "SELECT total, COUNT(*) AS n FROM "
            + "(SELECT k, SUM(qty) AS total FROM src GROUP BY k) GROUP BY total");
  }

  @Test
  void twoPhaseAggregateAtParallelismFourMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkGroupAggregateSqlHarnessTest::parallelTwoPhaseEnvironment,
        "SELECT k, SUM(qty) AS total, COUNT(*) AS n FROM src GROUP BY k");
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
  void stateTtlEmitsUnsuppressedUpdatesAndMatchesHost() throws Exception {
    // With idle-state TTL on (1h — nothing expires in-test), Flink disables the unchanged-result
    // suppression: the 0-value row produces an identical -U/+U pair the TTL-off run would swallow.
    // The kinded compare is the only one that can see such a pair, so this pins the native TTL
    // emission semantics change for change against the host.
    NativeParity.assertKindedParity(
        () -> {
          TableEnvironment tEnv = minimalEnvironment();
          tEnv.getConfig().set("table.exec.state.ttl", "1 h");
          return tEnv;
        },
        "SELECT k, SUM(`value`) AS s FROM kv GROUP BY k");
  }

  @Test
  void stateTtlHintOverridesJobRetention() throws Exception {
    // STATE_TTL('kv' = '1h') with the job retention at 0: the hint alone must switch the operator
    // into TTL emission (unsuppressed -U/+U pairs), matching Flink's hint-over-config precedence.
    NativeParity.assertKindedParity(
        FlinkGroupAggregateSqlHarnessTest::minimalEnvironment,
        "SELECT /*+ STATE_TTL('kv' = '1h') */ k, SUM(`value`) AS s FROM kv GROUP BY k");
    // And the inverse: a zero hint under a job-wide 1h retention turns TTL off for this
    // aggregate — the no-op update is suppressed again.
    NativeParity.assertKindedParity(
        () -> {
          TableEnvironment tEnv = minimalEnvironment();
          tEnv.getConfig().set("table.exec.state.ttl", "1 h");
          return tEnv;
        },
        "SELECT /*+ STATE_TTL('kv' = '0s') */ k, SUM(`value`) AS s FROM kv GROUP BY k");
  }

  private static TableEnvironment minimalEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    // The 0-value row leaves key 7's sum unchanged: suppressed with TTL off, an identical -U/+U
    // pair with TTL on — the emission difference the TTL tests above pin.
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "value"}, Types.LONG, Types.LONG),
            Row.of(7L, 1L),
            Row.of(7L, 2L),
            Row.of(7L, 0L),
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
    return environment(1);
  }

  private static TableEnvironment parallelEnvironment() {
    return environment(4);
  }

  private static TableEnvironment parallelTwoPhaseEnvironment() {
    TableEnvironment tEnv = environment(4);
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    tEnv.getConfig().set("table.exec.mini-batch.enabled", "true");
    tEnv.getConfig().set("table.exec.mini-batch.allow-latency", "10 ms");
    tEnv.getConfig().set("table.exec.mini-batch.size", "2");
    return tEnv;
  }

  private static TableEnvironment environment(int parallelism) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    // One-phase so the plan is a single GROUP BY aggregate (not a local/global split).
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");

    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"k", "s", "value", "qty", "price", "vs", "vt", "vf"},
                Types.LONG,
                Types.STRING,
                Types.LONG,
                Types.INT,
                Types.DOUBLE,
                Types.SHORT,
                Types.BYTE,
                Types.FLOAT),
            Row.of(7L, "a", 1L, 10, 1.5, (short) 100, (byte) 3, 1.25f),
            Row.of(7L, "a", 2L, 20, 2.5, (short) -7, (byte) -2, 2.5f),
            Row.of(9L, "b", 3L, 30, 3.0, (short) 250, (byte) 9, -0.75f),
            Row.of(7L, "a", 4L, 40, 4.5, (short) 42, (byte) 5, 4.5f),
            Row.of(9L, "b", 5L, 50, 5.5, (short) -11, (byte) -4, 5.125f));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("s", DataTypes.STRING())
            .column("value", DataTypes.BIGINT())
            .column("qty", DataTypes.INT())
            .column("price", DataTypes.DOUBLE())
            .column("vs", DataTypes.SMALLINT())
            .column("vt", DataTypes.TINYINT())
            .column("vf", DataTypes.FLOAT())
            .build());
    return tEnv;
  }
}

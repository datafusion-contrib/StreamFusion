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
 * Global {@code FETCH}/{@code LIMIT} matches the host. {@code ORDER BY … LIMIT n}
 * ({@code StreamPhysicalSortLimit}) and plain {@code LIMIT n} ({@code StreamPhysicalLimit}) both lower
 * to a global ROW_NUMBER rank, so they reuse the native columnar Top-N operator with an empty
 * partition key — the sort-limit emits the same insert/delete changelog as the bounded set changes,
 * the plain limit keeps the first n rows by arrival (insert-only). The parity harness compares the
 * full multiset of emitted change rows, so a differing changelog fails.
 */
class FlinkLimitSqlHarnessTest {

  @Test
  void sortLimitAscendingMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkLimitSqlHarnessTest::environment, "SELECT k, v FROM src ORDER BY v LIMIT 2");
  }

  @Test
  void sortLimitDescendingMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkLimitSqlHarnessTest::environment, "SELECT k, v FROM src ORDER BY v DESC LIMIT 2");
  }

  @Test
  void sortLimitMultiKeyMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkLimitSqlHarnessTest::environment, "SELECT k, v FROM src ORDER BY k, v DESC LIMIT 3");
  }

  @Test
  void sortLimitExceedingRowCountMatchesHost() throws Exception {
    // FETCH larger than the input — every row is in the top set.
    NativeParity.assertParity(
        FlinkLimitSqlHarnessTest::environment, "SELECT k, v FROM src ORDER BY v LIMIT 100");
  }

  @Test
  void plainLimitMatchesHost() throws Exception {
    // No ORDER BY — the ranker keeps the first n rows by arrival (insert-only). Deterministic here
    // because the single gather (parallelism 1) feeds host and native the same arrival order.
    NativeParity.assertParity(
        FlinkLimitSqlHarnessTest::environment, "SELECT k, v FROM src LIMIT 3");
  }

  @Test
  void sortLimitWithOffsetMatchesHost() throws Exception {
    // OFFSET 1 LIMIT 2 → the rank window [2, 3]; routed through the retracting ranker. The collapsed
    // result is the 2nd and 3rd smallest v globally (v=3 and v=4).
    NativeParity.assertChangelogParity(
        FlinkLimitSqlHarnessTest::environment, "SELECT k, v FROM src ORDER BY v LIMIT 2 OFFSET 1");
  }

  @Test
  void perOperatorFlagKeepsLimitOnHost() throws Exception {
    System.setProperty("streamfusion.operator.limit.enabled", "false");
    try {
      NativeParity.assertFallbackReasonContains(
          FlinkLimitSqlHarnessTest::environment,
          "SELECT k, v FROM src ORDER BY v LIMIT 2",
          "limit: disabled by config");
    } finally {
      System.clearProperty("streamfusion.operator.limit.enabled");
    }
  }

  @Test
  void stateTtlMatchesHost() throws Exception {
    // A limit lowers to a rank, which now runs its TTL natively too (1h — nothing expires
    // in-test): the changelog is unchanged under TTL, pinning routing and argument threading.
    NativeParity.assertKindedParity(
        () -> {
          TableEnvironment tEnv = environment();
          tEnv.getConfig().set("table.exec.state.ttl", "1 h");
          return tEnv;
        },
        "SELECT k, v FROM src ORDER BY v LIMIT 2");
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG),
            Row.of(1L, 5L),
            Row.of(1L, 3L),
            Row.of(1L, 8L),
            Row.of(1L, 1L),
            Row.of(2L, 9L),
            Row.of(2L, 4L));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder().column("k", DataTypes.BIGINT()).column("v", DataTypes.BIGINT()).build());
    return tEnv;
  }
}

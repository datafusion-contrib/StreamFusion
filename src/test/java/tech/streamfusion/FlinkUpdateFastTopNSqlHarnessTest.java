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
 * Update-fast streaming Top-N: a rank over a unique-keyed changelog whose sort key Flink infers
 * monotonic (here a descending {@code COUNT(*)}), planned WITHOUT retractions — the upstream emits
 * only {@code +I}/{@code +U} and a rank row is replaced by its unique key. The native ranker
 * mirrors Flink's {@code UpdatableTopNFunction} bounded state (and, for {@code rn <= 1}, the
 * {@code FastTop1Function} drop-non-improving semantics); its collapsed changelog must match the
 * host. Counts tie and cross between keys, exercising in-place replacement, re-sorting, entry past
 * a full top-N, and eviction.
 */
class FlinkUpdateFastTopNSqlHarnessTest {

  private static final String RANKED =
      "SELECT g, k, cnt FROM ("
          + "  SELECT g, k, cnt, ROW_NUMBER() OVER (PARTITION BY g ORDER BY cnt DESC) AS rn"
          + "  FROM (SELECT g, k, COUNT(*) AS cnt FROM src GROUP BY g, k)"
          + ") WHERE rn <= 2";

  private static final String RANKED_WITH_NUMBER =
      "SELECT g, k, cnt, rn FROM ("
          + "  SELECT g, k, cnt, ROW_NUMBER() OVER (PARTITION BY g ORDER BY cnt DESC) AS rn"
          + "  FROM (SELECT g, k, COUNT(*) AS cnt FROM src GROUP BY g, k)"
          + ") WHERE rn <= 2";

  private static final String TOP_1 =
      "SELECT g, k, cnt FROM ("
          + "  SELECT g, k, cnt, ROW_NUMBER() OVER (PARTITION BY g ORDER BY cnt DESC) AS rn"
          + "  FROM (SELECT g, k, COUNT(*) AS cnt FROM src GROUP BY g, k)"
          + ") WHERE rn <= 1";

  @Test
  void updateFastRankMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(FlinkUpdateFastTopNSqlHarnessTest::environment, RANKED);
  }

  @Test
  void updateFastRankWithRankNumberMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkUpdateFastTopNSqlHarnessTest::environment, RANKED_WITH_NUMBER);
  }

  /**
   * {@code rn <= 1} routes to Flink's {@code FastTop1Function}, whose semantics differ from the
   * general function: a record that does not strictly improve on the current top-1 is dropped
   * outright, so an equal-count challenger never displaces the incumbent. The tied counts in the
   * data exercise exactly that.
   */
  @Test
  void updateFastTop1MatchesHost() throws Exception {
    NativeParity.assertChangelogParity(FlinkUpdateFastTopNSqlHarnessTest::environment, TOP_1);
  }

  /** The OFFSET window is the one update-fast shape still on the host. */
  @Test
  void updateFastRankWithOffsetFallsBackToHost() throws Exception {
    NativeParity.assertFallbackReasonContains(
        FlinkUpdateFastTopNSqlHarnessTest::environment,
        "SELECT g, k, cnt, rn FROM ("
            + "  SELECT g, k, cnt, ROW_NUMBER() OVER (PARTITION BY g ORDER BY cnt DESC) AS rn"
            + "  FROM (SELECT g, k, COUNT(*) AS cnt FROM src GROUP BY g, k)"
            + ") WHERE rn > 1 AND rn <= 3",
        "update-fast rank with OFFSET");
  }

  @Test
  void stateTtlMatchesHost() throws Exception {
    // With idle-state TTL on (1h — nothing expires in-test) the update-fast rank runs natively
    // with per-row-key entry TTL; without expiry its changelog is unchanged, pinning routing and
    // the TTL argument threading against the host.
    NativeParity.assertChangelogParity(
        () -> {
          TableEnvironment tEnv = environment();
          tEnv.getConfig().set("table.exec.state.ttl", "1 h");
          return tEnv;
        },
        RANKED);
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    // Two groups; counts tie (k=10 and k=30 both reach 2 in group 1) and cross (k=30 passes k=10),
    // so the top-2 re-sorts, a full top-N rejects a low count, and an eviction promotes.
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"g", "k"}, Types.LONG, Types.LONG),
            Row.of(1L, 10L),
            Row.of(1L, 20L),
            Row.of(1L, 10L),
            Row.of(1L, 30L),
            Row.of(1L, 30L),
            Row.of(1L, 30L),
            Row.of(1L, 20L),
            Row.of(1L, 20L),
            Row.of(1L, 20L),
            Row.of(2L, 40L),
            Row.of(2L, 50L),
            Row.of(2L, 50L),
            Row.of(2L, 40L),
            Row.of(2L, 60L),
            Row.of(2L, 60L),
            Row.of(2L, 60L));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("g", DataTypes.BIGINT())
            .column("k", DataTypes.BIGINT())
            .build());
    return tEnv;
  }
}

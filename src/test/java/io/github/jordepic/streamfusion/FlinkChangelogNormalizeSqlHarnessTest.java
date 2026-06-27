package io.github.jordepic.streamfusion;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

/**
 * Changelog normalization matches the host. An upsert source (e.g. upsert-kafka) is planned as a
 * {@code ChangelogNormalize} keyed by the primary key, which keeps the last row per key and emits a
 * regular INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE changelog. The native operator ports Flink's
 * keep-last-on-changelog function, so the collapsed materialized result matches the host.
 */
class FlinkChangelogNormalizeSqlHarnessTest {

  @Test
  void normalizesUpsertStreamMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkChangelogNormalizeSqlHarnessTest::environment, "SELECT f0, f1 FROM up");
  }

  @Test
  void normalizedStreamIntoAggregateMatchesHost() throws Exception {
    // The normalized changelog feeds a native GROUP BY — a full native changelog chain.
    NativeParity.assertChangelogParity(
        FlinkChangelogNormalizeSqlHarnessTest::environment,
        "SELECT f1, COUNT(*) FROM up GROUP BY f1");
  }

  @Test
  void perOperatorFlagKeepsNormalizeOnHost() throws Exception {
    System.setProperty("streamfusion.operator.changelogNormalize.enabled", "false");
    try {
      NativeParity.assertFallbackReasonContains(
          FlinkChangelogNormalizeSqlHarnessTest::environment,
          "SELECT f0, f1 FROM up",
          "changelogNormalize: disabled by config");
    } finally {
      System.clearProperty("streamfusion.operator.changelogNormalize.enabled");
    }
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    // An upsert stream keyed by f0: insert, update (same key), insert another key, then a delete,
    // and a no-op update (same value) to exercise the unchanged-row suppression.
    DataStream<Row> source =
        env.fromData(
            Row.ofKind(RowKind.INSERT, 1L, 10L),
            Row.ofKind(RowKind.UPDATE_AFTER, 1L, 20L),
            Row.ofKind(RowKind.INSERT, 2L, 5L),
            Row.ofKind(RowKind.UPDATE_AFTER, 2L, 5L),
            Row.ofKind(RowKind.DELETE, 1L, 20L),
            Row.ofKind(RowKind.INSERT, 3L, 7L));
    Table table =
        tEnv.fromChangelogStream(
            source,
            Schema.newBuilder()
                .column("f0", DataTypes.BIGINT().notNull())
                .column("f1", DataTypes.BIGINT())
                .primaryKey("f0")
                .build(),
            ChangelogMode.upsert());
    tEnv.createTemporaryView("up", table);
    return tEnv;
  }
}

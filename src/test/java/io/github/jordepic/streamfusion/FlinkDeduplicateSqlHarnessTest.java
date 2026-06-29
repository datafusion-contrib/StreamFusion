package io.github.jordepic.streamfusion;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 * Row-time deduplication: per key the native operator keeps either the minimum-rowtime row
 * (keep-first, {@code ORDER BY rt ASC} — insert-only, emitted on the watermark) or the
 * maximum-rowtime row (keep-last, {@code ORDER BY rt DESC} — a retract changelog, emitted eagerly).
 */
class FlinkDeduplicateSqlHarnessTest {

  private static final String KEEP_FIRST =
      "SELECT k, v, rt FROM ("
          + "SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY rt ASC) AS rn FROM src) WHERE rn = 1";

  private static final String KEEP_LAST =
      "SELECT k, v, rt FROM ("
          + "SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY rt DESC) AS rn FROM src) WHERE rn = 1";

  @Test
  void keepFirstDeduplicationMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkDeduplicateSqlHarnessTest::environment, KEEP_FIRST);
  }

  @Test
  void keepLastDeduplicationMatchesHost() throws Exception {
    // Keep-last keeps the maximum-rowtime row per key and emits a retract changelog as a later row
    // displaces the stored one; the collapsed result is key 1's (v=30, rt=2000) and key 2's
    // (v=50, rt=1500).
    NativeParity.assertChangelogParity(FlinkDeduplicateSqlHarnessTest::environment, KEEP_LAST);
  }

  // Proctime dedup orders by arrival (no rowtime). Only k,v are projected (the PROCTIME() column is
  // wall-clock, hence non-deterministic) so the comparison is deterministic at parallelism 1.
  private static final String KEEP_FIRST_PROCTIME =
      "SELECT k, v FROM ("
          + "SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY pt ASC) AS rn FROM src) WHERE rn = 1";

  private static final String KEEP_LAST_PROCTIME =
      "SELECT k, v FROM ("
          + "SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY pt DESC) AS rn FROM src) WHERE rn = 1";

  @Test
  void keepFirstProctimeDeduplicationMatchesHost() throws Exception {
    // Proctime keep-first emits each key's first-arriving row (insert-only): key 1's (v=30), key 2's
    // (v=50) — by source/arrival order, not rowtime.
    NativeParity.assertParity(
        FlinkDeduplicateSqlHarnessTest::proctimeEnvironment, KEEP_FIRST_PROCTIME);
  }

  @Test
  void keepLastProctimeDeduplicationMatchesHost() throws Exception {
    // Proctime keep-last keeps each key's last-arriving row, emitting a retract changelog; the
    // collapsed result is key 1's (v=25) and key 2's (v=40).
    NativeParity.assertChangelogParity(
        FlinkDeduplicateSqlHarnessTest::proctimeEnvironment, KEEP_LAST_PROCTIME);
  }

  private static TableEnvironment proctimeEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG),
            Row.of(1L, 30L),
            Row.of(2L, 50L),
            Row.of(1L, 20L),
            Row.of(2L, 40L),
            Row.of(1L, 25L));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .columnByExpression("pt", "PROCTIME()")
            .build());
    return tEnv;
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    // Multiple rows per key, out of order, so "first by rowtime" is not "first to arrive": key 1's
    // minimum-rowtime row is (v=20, rt=0); key 2's is (v=40, rt=1000).
    DataStream<Row> source =
        env.fromData(
                Types.ROW_NAMED(new String[] {"k", "v", "ts"}, Types.LONG, Types.LONG, Types.LONG),
                Row.of(1L, 30L, 2000L),
                Row.of(2L, 50L, 1500L),
                Row.of(1L, 20L, 0L),
                Row.of(2L, 40L, 1000L),
                Row.of(1L, 25L, 800L))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner((row, ts) -> (Long) row.getField(2)));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return tEnv;
  }
}

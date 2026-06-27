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
 * Append-only keep-first deduplication on a rowtime order: per key the native operator keeps the
 * minimum-rowtime row and emits it once the watermark reaches it. Keep-last (descending) is a
 * retracting deduplicate and must fall back to the host.
 */
class FlinkDeduplicateSqlHarnessTest {

  private static final String KEEP_FIRST =
      "SELECT k, v, rt FROM ("
          + "SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY rt ASC) AS rn FROM src) WHERE rn = 1";

  @Test
  void keepFirstDeduplicationMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkDeduplicateSqlHarnessTest::environment, KEEP_FIRST);
  }

  @Test
  void keepLastDeduplicationFallsBack() throws Exception {
    // ORDER BY rt DESC is a retracting (keep-last) deduplicate — not implemented; the whole query
    // runs on the host and still matches.
    NativeParity.assertFallback(
        FlinkDeduplicateSqlHarnessTest::environment,
        "SELECT k, v, rt FROM ("
            + "SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY rt DESC) AS rn FROM src) WHERE rn = 1");
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

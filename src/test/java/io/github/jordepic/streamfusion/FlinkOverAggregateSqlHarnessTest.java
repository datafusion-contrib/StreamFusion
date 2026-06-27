package io.github.jordepic.streamfusion;

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
 * Event-time {@code OVER (ORDER BY rt RANGE UNBOUNDED PRECEDING)} running aggregation matches the
 * host: each input row is emitted with the running aggregate over all rows up to its rowtime.
 */
class FlinkOverAggregateSqlHarnessTest {

  @Test
  void runningSumMatchesHost() throws Exception {
    // `ORDER BY rt` with no explicit frame defaults to RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
    NativeParity.assertParity(
        FlinkOverAggregateSqlHarnessTest::environment,
        "SELECT v, SUM(v) OVER (ORDER BY rt) AS total FROM src");
  }

  @Test
  void multipleRunningAggregatesMatchHost() throws Exception {
    NativeParity.assertParity(
        FlinkOverAggregateSqlHarnessTest::environment,
        "SELECT v, SUM(v) OVER w AS s, MIN(v) OVER w AS lo, MAX(v) OVER w AS hi, "
            + "COUNT(v) OVER w AS c FROM src WINDOW w AS (ORDER BY rt)");
  }

  @Test
  void runningAvgMatchesHost() throws Exception {
    // Flink decomposes AVG into $SUM0/COUNT with a divide above; the OVER routes, the divide runs
    // on the host (integer `/` is not admitted), and the result matches.
    NativeParity.assertParity(
        FlinkOverAggregateSqlHarnessTest::environment,
        "SELECT v, AVG(v) OVER (ORDER BY rt) AS mean FROM src");
  }

  @Test
  void rowNumberMatchesHost() throws Exception {
    // ROW_NUMBER() over the ROWS UNBOUNDED PRECEDING frame: a per-partition counter in rowtime
    // order. The source has distinct rowtimes per key, so the numbering is unambiguous.
    NativeParity.assertParity(
        FlinkOverAggregateSqlHarnessTest::environment,
        "SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY rt) AS rn FROM src");
  }

  @Test
  void rankAndDenseRankMatchHost() throws Exception {
    // RANK and DENSE_RANK over (PARTITION BY k ORDER BY rt). Tied rowtimes would share a rank;
    // tie semantics are covered by the native test, here we verify routing + host parity.
    NativeParity.assertParity(
        FlinkOverAggregateSqlHarnessTest::environment,
        "SELECT k, v, RANK() OVER (PARTITION BY k ORDER BY rt) AS rk, "
            + "DENSE_RANK() OVER (PARTITION BY k ORDER BY rt) AS dr FROM src");
  }

  @Test
  void partitionedRunningSumMatchesHost() throws Exception {
    // PARTITION BY k: a running SUM per key, shuffled by the host's keyed exchange.
    NativeParity.assertParity(
        FlinkOverAggregateSqlHarnessTest::environment,
        "SELECT k, v, SUM(v) OVER (PARTITION BY k ORDER BY rt) AS total FROM src");
  }

  @Test
  void booleanPartitionKeyMatchesHost() throws Exception {
    // PARTITION BY a boolean key — exercises the wider partition-key set (the native key path is
    // type-general; boolean/date/timestamp/decimal are admitted, not just bigint/int/string).
    NativeParity.assertParity(
        FlinkOverAggregateSqlHarnessTest::environment,
        "SELECT b, v, SUM(v) OVER (PARTITION BY b ORDER BY rt) AS total FROM src");
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    // Out-of-order within the bound so the running totals exercise rowtime ordering and ties.
    DataStream<Row> source =
        env.fromData(
                Types.ROW_NAMED(
                    new String[] {"k", "v", "ts", "b"},
                    Types.LONG,
                    Types.LONG,
                    Types.LONG,
                    Types.BOOLEAN),
                Row.of(1L, 10L, 0L, false),
                Row.of(2L, 20L, 1000L, true),
                Row.of(1L, 30L, 1000L, false),
                Row.of(2L, 40L, 500L, true),
                Row.of(1L, 50L, 2000L, false))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(java.time.Duration.ofSeconds(5))
                    .withTimestampAssigner((row, ts) -> (Long) row.getField(2)));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .column("ts", DataTypes.BIGINT())
            .column("b", DataTypes.BOOLEAN())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return tEnv;
  }
}

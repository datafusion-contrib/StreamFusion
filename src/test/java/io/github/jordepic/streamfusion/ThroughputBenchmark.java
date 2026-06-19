package io.github.jordepic.streamfusion;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;
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
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * End-to-end throughput comparison of an accelerated operator against stock Flink: the same query
 * runs over a large generated source into a discarding sink, once with native substitution and once
 * without, and reports rows/s for each plus the speedup. Opt-in (it runs millions of rows) — enable
 * with {@code SF_BENCHMARK=true}; numbers are printed to stdout and belong in the readme.
 *
 * <p>This is a whole-job measurement (source, operator, sink, single slot), so it includes fixed
 * per-job overhead that dampens the ratio; it is the honest "vs Flink" number, the complement to the
 * isolated native hot-loop figures from `cargo bench`.
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class ThroughputBenchmark {

  private static final long ROWS = 5_000_000L;
  private static final int WARMUP = 1;
  private static final int RUNS = 3;

  @Test
  void filterThroughput() throws Exception {
    compare(
        "Filter (WHERE v > 50)",
        ThroughputBenchmark::filterEnvironment,
        "CREATE TABLE sink (k BIGINT, v INT, s STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT * FROM f WHERE v > 50");
  }

  @Test
  void tumblingThroughput() throws Exception {
    compare(
        "Tumbling (1s SUM)",
        ThroughputBenchmark::tumblingEnvironment,
        "CREATE TABLE sink (window_start TIMESTAMP_LTZ(3), window_end TIMESTAMP_LTZ(3), total BIGINT)"
            + " WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT window_start, window_end, SUM(v) AS total FROM TABLE(TUMBLE(TABLE g,"
            + " DESCRIPTOR(rt), INTERVAL '1' SECOND)) GROUP BY window_start, window_end");
  }

  @Test
  void parquetSinkThroughput() throws Exception {
    double flink = bestOfSink(false);
    double nativeRun = bestOfSink(true);
    System.out.printf("%n[benchmark] Parquet sink (INSERT) over %,d rows (best of %d)%n", ROWS, RUNS);
    System.out.printf("[benchmark]   Flink : %6.3f s  (%,.0f rows/s)%n", flink, ROWS / flink);
    System.out.printf(
        "[benchmark]   Native: %6.3f s  (%,.0f rows/s)  %.2fx vs Flink%n",
        nativeRun, ROWS / nativeRun, flink / nativeRun);
  }

  private static double bestOfSink(boolean useNative) throws Exception {
    double best = Double.MAX_VALUE;
    for (int run = 0; run < WARMUP + RUNS; run++) {
      double seconds = sinkRunOnce(useNative);
      if (run >= WARMUP) {
        best = Math.min(best, seconds);
      }
    }
    return best;
  }

  /** One INSERT of the generated source into a fresh filesystem Parquet sink, timed to completion. */
  private static double sinkRunOnce(boolean useNative) throws Exception {
    Path directory = Files.createTempDirectory("bench-parquet-sink");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(1000); // both the native and host sinks commit files on checkpoint
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromSequence(0, ROWS - 1)
            .map(i -> Row.of(i, (int) (i % 100)))
            .returns(Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.INT));
    tEnv.createTemporaryView(
        "s",
        source,
        Schema.newBuilder().column("k", DataTypes.BIGINT()).column("v", DataTypes.INT()).build());
    tEnv.executeSql(
        "CREATE TABLE pq (k BIGINT, v INT) WITH ('connector' = 'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");

    PhysicalPlanScan scan = useNative ? NativePlanner.install(tEnv) : null;
    long start = System.nanoTime();
    tEnv.executeSql("INSERT INTO pq SELECT * FROM s").await();
    double seconds = (System.nanoTime() - start) / 1e9;
    if (useNative && scan.substitutions() == 0) {
      throw new IllegalStateException("native sink did not engage; comparison is moot");
    }
    return seconds;
  }

  private static void compare(
      String label, Supplier<TableEnvironment> factory, String sinkDdl, String insertSql)
      throws Exception {
    double flink = bestOf(factory, false, sinkDdl, insertSql);
    double nativeRun = bestOf(factory, true, sinkDdl, insertSql);
    System.out.printf("%n[benchmark] %s over %,d rows (best of %d)%n", label, ROWS, RUNS);
    System.out.printf("[benchmark]   Flink : %6.3f s  (%,.0f rows/s)%n", flink, ROWS / flink);
    System.out.printf(
        "[benchmark]   Native: %6.3f s  (%,.0f rows/s)  %.2fx vs Flink%n",
        nativeRun, ROWS / nativeRun, flink / nativeRun);
  }

  private static double bestOf(
      Supplier<TableEnvironment> factory, boolean useNative, String sinkDdl, String insertSql)
      throws Exception {
    double best = Double.MAX_VALUE;
    for (int run = 0; run < WARMUP + RUNS; run++) {
      double seconds = runOnce(factory, useNative, sinkDdl, insertSql);
      if (run >= WARMUP) {
        best = Math.min(best, seconds);
      }
    }
    return best;
  }

  private static double runOnce(
      Supplier<TableEnvironment> factory, boolean useNative, String sinkDdl, String insertSql)
      throws Exception {
    TableEnvironment tEnv = factory.get();
    PhysicalPlanScan scan = useNative ? NativePlanner.install(tEnv) : null;
    tEnv.executeSql(sinkDdl);
    long start = System.nanoTime();
    tEnv.executeSql(insertSql).await();
    double seconds = (System.nanoTime() - start) / 1e9;
    if (useNative && scan.substitutions() == 0) {
      throw new IllegalStateException("native substitution did not engage; comparison is moot");
    }
    return seconds;
  }

  private static TableEnvironment filterEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromSequence(0, ROWS - 1)
            .map(i -> Row.of(i, (int) (i % 100), "s" + (i % 8)))
            .returns(
                Types.ROW_NAMED(
                    new String[] {"k", "v", "s"}, Types.LONG, Types.INT, Types.STRING));
    tEnv.createTemporaryView(
        "f",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.INT())
            .column("s", DataTypes.STRING())
            .build());
    return tEnv;
  }

  private static TableEnvironment tumblingEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    // ts = row index in millis (monotonic), so each 1-second window holds 1000 rows.
    DataStream<Row> source =
        env.fromSequence(0, ROWS - 1)
            .map(i -> Row.of((long) (i % 100), i))
            .returns(Types.ROW_NAMED(new String[] {"v", "ts"}, Types.LONG, Types.LONG))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forMonotonousTimestamps()
                    .withTimestampAssigner((row, ts) -> (Long) row.getField(1)));
    tEnv.createTemporaryView(
        "g",
        source,
        Schema.newBuilder()
            .column("v", DataTypes.BIGINT())
            .column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return tEnv;
  }
}

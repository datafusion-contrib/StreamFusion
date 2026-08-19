package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

/**
 * Every filesystem sink configuration the native writer cannot honor 1:1 declines with a recorded
 * reason and leaves the sink on the host — the "why didn't my query accelerate?" contract.
 */
class FlinkParquetSinkFallbackTest {

  private static void assertFallsBack(String tableOptions, String columns, String reason) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(
        "CREATE TABLE pq ("
            + columns
            + ") WITH ('connector' = 'filesystem', 'path' = '/tmp/unused', 'format' = 'parquet'"
            + tableOptions
            + ")");
    tEnv.executeSql("CREATE TABLE src (" + columns + ") WITH ('connector' = 'datagen')");

    PhysicalPlanScan scan = NativePlanner.install(tEnv);
    tEnv.explainSql("INSERT INTO pq SELECT * FROM src");
    assertEquals(0, scan.substitutions(), "expected the sink to stay on the host");
    assertTrue(
        scan.fallbackReasons().stream().anyMatch(r -> r.contains(reason)),
        () -> "no fallback reason contained \"" + reason + "\"; reasons=" + scan.fallbackReasons());
  }

  @Test
  void int96TimestampsFallBack() {
    // Flink's default timestamp encoding, which the native writer cannot produce.
    assertFallsBack("", "v INT, ts TIMESTAMP(3)", "INT96");
  }

  @Test
  void localTimezoneTimestampsFallBack() {
    assertFallsBack(
        ", 'parquet.write.int64.timestamp' = 'true'",
        "v INT, ts TIMESTAMP(3)",
        "local timezone");
  }

  @Test
  void autoCompactionFallsBack() {
    assertFallsBack(", 'auto-compaction' = 'true'", "v INT", "compaction");
  }

  @Test
  void unsupportedCompressionFallsBack() {
    assertFallsBack(", 'parquet.compression' = 'LZO'", "v INT", "LZO");
  }

  @Test
  void multithreadedZstdFallsBack() {
    assertFallsBack(
        ", 'parquet.compression' = 'ZSTD', 'parquet.compression.codec.zstd.workers' = '4'",
        "v INT",
        "zstd");
  }

  @Test
  void unknownParquetWriterOptionFallsBack() {
    assertFallsBack(
        ", 'parquet.future-writer-setting' = 'on'", "v INT", "future-writer-setting");
  }

  // A changelog (retracting) input never reaches the matcher: Flink itself rejects update changes
  // into the append-only filesystem sink during validation, so the planner's insert-only guard is
  // defense-in-depth with no SQL-reachable trigger.
}

package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

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

  @Test
  void nullableInputForNotNullColumnFallsBackToFlinksEnforcer() {
    assertConstraintFallsBack(
        "id BIGINT NOT NULL", "id BIGINT", null, null, "not-null-enforcer=ERROR");
  }

  @Test
  void enforcedCharacterLengthFallsBackToFlinksEnforcer() {
    assertConstraintFallsBack(
        "fixed CHAR(3), limited VARCHAR(3)",
        "fixed CHAR(3), limited VARCHAR(3)",
        "table.exec.sink.type-length-enforcer",
        "TRIM_PAD",
        "type-length-enforcer=TRIM_PAD");
  }

  @Test
  void enforcedBinaryLengthFallsBackToFlinksEnforcer() {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.SINK_LENGTH_ERROR,
        "Flink 1.18 only provides IGNORE and TRIM_PAD sink length policies");
    assertConstraintFallsBack(
        "fixed BINARY(3), limited VARBINARY(3)",
        "fixed BINARY(3), limited VARBINARY(3)",
        "table.exec.sink.type-length-enforcer",
        "ERROR",
        "type-length-enforcer=ERROR");
  }

  @Test
  void ignoredCharacterLengthRemainsNative() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(
        "CREATE TABLE pq (fixed CHAR(3), limited VARCHAR(3)) WITH ("
            + "'connector' = 'filesystem', 'path' = '/tmp/unused', 'format' = 'parquet')");
    tEnv.executeSql(
        "CREATE TABLE src (fixed CHAR(3), limited VARCHAR(3)) WITH ('connector' = 'datagen')");

    PhysicalPlanScan scan = NativePlanner.install(tEnv);
    tEnv.explainSql("INSERT INTO pq SELECT * FROM src");

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
  }

  private static void assertConstraintFallsBack(
      String sinkColumns,
      String sourceColumns,
      String configKey,
      String configValue,
      String reason) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    if (configKey != null) {
      tEnv.getConfig().set(configKey, configValue);
    }
    tEnv.executeSql(
        "CREATE TABLE pq ("
            + sinkColumns
            + ") WITH ('connector' = 'filesystem', 'path' = '/tmp/unused', 'format' = 'parquet')");
    tEnv.executeSql(
        "CREATE TABLE src (" + sourceColumns + ") WITH ('connector' = 'datagen')");

    PhysicalPlanScan scan = NativePlanner.install(tEnv);
    tEnv.explainSql("INSERT INTO pq SELECT * FROM src");

    assertEquals(0, scan.substitutions(), "expected the sink to stay on the host");
    assertTrue(
        scan.fallbackReasons().stream().anyMatch(r -> r.contains(reason)),
        () -> "no fallback reason contained \"" + reason + "\"; reasons=" + scan.fallbackReasons());
  }

  // A changelog (retracting) input never reaches the matcher: Flink itself rejects update changes
  // into the append-only filesystem sink during validation, so the planner's insert-only guard is
  // defense-in-depth with no SQL-reachable trigger.
}

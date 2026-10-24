package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class JvmAggregateBenchmark {
  @Test
  void completeJobsRetainKeyedStateAndBothTransposes() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"));
    int rows = Integer.getInteger("jvmagg.rows", 2_000_000);
    int keys = Integer.getInteger("jvmagg.keys", 65_536);
    int values = Integer.getInteger("jvmagg.values", 0);
    int warmups = Integer.getInteger("jvmagg.warmup", 2);
    int runs = Integer.getInteger("jvmagg.runs", 5);
    String keyType = System.getProperty("jvmagg.keyType", "int");
    int prefixChars = Integer.getInteger("jvmagg.keyPrefixChars", 16);
    String prefix = prefixChars <= 0 ? "" : "é" + "x".repeat(prefixChars - 1);
    boolean expectedCache = Boolean.parseBoolean(System.getProperty("jvmagg.expectCache", "true"));
    boolean expectedUdf = Boolean.parseBoolean(System.getProperty("jvmagg.expectNative", "false"));
    for (String shape : System.getProperty("jvmagg.cases", "udaf,builtin").split(",")) {
      boolean udf = shape.equals("udaf");
      boolean collect = shape.equals("collect");
      boolean jvm = udf || collect;
      String sql =
          "SELECT k, "
              + (udf ? "test_aggregate" : collect ? "COLLECT" : "SUM")
              + "(v) FROM src GROUP BY k";
      double[][] elapsed = new double[2][runs];
      for (int trial = 0; trial < warmups + runs; trial++) {
        for (int order = 0; order < 2; order++) {
          try (var metrics = new RecoveryExecutionMetrics()) {
            int engine = (trial + order) & 1;
            String selectedEngine = System.getProperty("jvmagg.engine", "both");
            if (!selectedEngine.equals("both")
                && !selectedEngine.equals(engine == 1 ? "native" : "flink")) continue;
            var env = metrics.environment(new Configuration(), 1, 0);
            env.setParallelism(1);
            var table = StreamTableEnvironment.create(env);
            table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
            var sequence = env.fromSequence(0, Math.max(0, rows - 1)).filter(i -> i < rows);
            org.apache.flink.streaming.api.datastream.DataStream<Row> source;
            switch (keyType) {
              case "string":
                source =
                    sequence
                        .map(i -> Row.of(prefix + Long.toString(i % keys), value(i, keys, values)))
                        .returns(
                            Types.ROW_NAMED(new String[] {"k", "v"}, Types.STRING, Types.LONG));
                break;
              case "bigint":
                source =
                    sequence
                        .map(i -> Row.of(i % keys, value(i, keys, values)))
                        .returns(Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG));
                break;
              case "int":
                source =
                    sequence
                        .map(i -> Row.of((int) (i % keys), value(i, keys, values)))
                        .returns(Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.LONG));
                break;
              default:
                throw new IllegalArgumentException("Unsupported key type: " + keyType);
            }
            table.createTemporaryView("src", source);
            table.createTemporarySystemFunction("test_aggregate", new PortableSqlFixtures.Sum());
            table.createTemporaryTable(
                "sink",
                TableDescriptor.forConnector("blackhole")
                    .schema(
                        Schema.newBuilder()
                            .fromRowDataType(
                                table.sqlQuery(sql).getResolvedSchema().toPhysicalRowDataType())
                            .build())
                    .build());
            String insert = "INSERT INTO sink " + sql;
            var scan = engine == 1 ? NativePlanner.install(table) : null;
            String plan = table.explainSql(insert);
            if (engine == 1) {
              if (!jvm || expectedUdf) {
                assertTrue(
                    plan.contains(jvm ? "NativeJvmGroup" : "NativeColumnarGroupAggregate")
                        && plan.contains("RowDataToArrow")
                        && plan.contains("ArrowToRowData"),
                    plan);
              } else assertEquals(0, scan.substitutions(), plan);
            } else {
              assertFalse(
                  plan.contains("NativeJvmGroup") || plan.contains("NativeColumnarGroupAggregate"),
                  plan);
            }
            long startMillis = System.currentTimeMillis();
            long start = System.nanoTime();
            table.executeSql(insert).await();
            double seconds = (System.nanoTime() - start) / 1e9;
            System.out.printf(
                Locale.ROOT,
                "[jvm-aggregate-trial] shape=%s key_type=%s engine=%s trial=%d warmup=%s rows=%d"
                    + " keys=%d start_ms=%d seconds=%.6f%n",
                shape,
                keyType,
                engine == 0 ? "Flink" : "StreamFusion",
                trial,
                trial < warmups,
                rows,
                keys,
                startMillis,
                seconds);
            if (trial >= warmups) elapsed[engine][trial - warmups] = seconds;
            if (engine == 1
                && jvm
                && expectedUdf
                && expectedCache
                && Integer.getInteger("streamfusion.experimental.jvmAccumulatorCacheEntries", 0)
                    > 0) {
              long hits = metrics.counterValue("jvmAccumulatorCacheHits");
              long bypasses = metrics.counterValue("jvmAccumulatorCacheBypasses");
              int capacity =
                  Integer.getInteger("streamfusion.experimental.jvmAccumulatorCacheEntries", 0);
              if (keys > capacity && rows > capacity) {
                assertTrue(bypasses > 0, "cold cache did not switch to managed state");
              } else {
                assertEquals(0, bypasses, "bounded working set unexpectedly bypassed the cache");
                if (keys < rows) assertTrue(hits > 0, "accumulator cache did not execute");
              }
              if (keys >= rows) assertEquals(0, hits, "distinct keys unexpectedly hit the cache");
              System.out.println("[jvm-aggregate-cache] hits=" + hits + " bypasses=" + bypasses);
            }
            if (engine == 0 || (jvm && !expectedUdf))
              assertTrue(
                  metrics.nativeOperators().isEmpty(),
                  "fallback control executed native operators");
            if (engine == 1 && (!jvm || expectedUdf))
              RecoveryExecutionMetrics.assertConsumed(
                  metrics.nativeOperators(),
                  jvm ? "NativeJvmGroup" : "NativeColumnarGroupAggregate");
          }
        }
      }
      System.out.printf(
          Locale.ROOT,
          "[jvm-aggregate] shape=%s key_type=%s key_prefix_chars=%d rows=%d keys=%d values=%d"
              + " stock=%.6f sf=%.6f stock_trials=%s sf_trials=%s%n",
          shape,
          keyType,
          prefixChars,
          rows,
          keys,
          values,
          median(elapsed[0]),
          median(elapsed[1]),
          Arrays.toString(elapsed[0]),
          Arrays.toString(elapsed[1]));
    }
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PROFILE_JVM_AGG", matches = "true")
  void profileCollect() throws Exception {
    String[] properties = {"jvmagg.engine", "jvmagg.warmup", "jvmagg.runs"};
    String[] previous = Arrays.stream(properties).map(System::getProperty).toArray(String[]::new);
    String profiler = System.getProperty("profile.asprof", "asprof");
    String pid = Long.toString(ProcessHandle.current().pid());
    var directory =
        java.nio.file.Path.of(System.getProperty("profile.outputDir", "target/profiles/jvmagg"));
    java.nio.file.Files.createDirectories(directory);
    var recording =
        directory
            .resolve("collect-keys" + Integer.getInteger("jvmagg.keys", 64) + "-native.jfr")
            .toAbsolutePath();
    boolean started = false;
    try {
      System.setProperty(properties[0], "native");
      System.setProperty(properties[1], "0");
      System.setProperty(properties[2], "1");
      completeJobsRetainKeyedStateAndBothTransposes();
      profileCommand(profiler, "start", "-e", "cpu", "-i", "1ms", "-f", recording.toString(), pid);
      started = true;
      long deadline = System.nanoTime() + Long.getLong("jvmagg.seconds", 8L) * 1_000_000_000L;
      do {
        completeJobsRetainKeyedStateAndBothTransposes();
      } while (System.nanoTime() < deadline);
    } finally {
      try {
        if (started) profileCommand(profiler, "stop", pid);
      } finally {
        for (int i = 0; i < properties.length; i++) {
          if (previous[i] == null) System.clearProperty(properties[i]);
          else System.setProperty(properties[i], previous[i]);
        }
      }
    }
  }

  private static void profileCommand(String... command) throws Exception {
    int exit = new ProcessBuilder(command).inheritIO().start().waitFor();
    if (exit != 0) throw new IllegalStateException("Profiler exited " + exit);
  }

  private static Long value(long row, int keys, int values) {
    return row % 8 == 7 ? null : values > 0 ? (row / keys) % values : row;
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }
}

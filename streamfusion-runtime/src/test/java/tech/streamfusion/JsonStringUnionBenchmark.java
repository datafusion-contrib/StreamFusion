package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class JsonStringUnionBenchmark {
  private static final long ROWS = Long.getLong("jsonunion.rows", 2_000_000L);
  private static final int WARMUP = Integer.getInteger("jsonunion.warmup", 2);
  private static final int RUNS = Integer.getInteger("jsonunion.runs", 5);
  private static final String FIXTURE = System.getProperty("jsonunion.fixture", "unicode");
  private static final boolean EXPECT_NATIVE = Boolean.parseBoolean(
      System.getProperty("jsonunion.expectNative", "true"));

  @Test
  void finalUnionRetainsBothTransposes() throws Exception {
    String sql = "INSERT INTO sink SELECT id, JSON_VALUE(s, '$.v') FROM src WHERE MOD(id, 2) = 0 "
        + "UNION ALL SELECT id, JSON_VALUE(s, '$.v') FROM src WHERE MOD(id, 2) = 1";
    double[][] times = new double[2][RUNS];
    try (var metrics = new RecoveryExecutionMetrics()) {
      for (int trial = 0; trial < WARMUP + RUNS; trial++) {
        for (int turn = 0; turn < 2; turn++) {
          int engine = (trial + turn) % 2;
          var env = metrics.environment(new Configuration(), 1, 0);
          var table = StreamTableEnvironment.create(env);
          table.getConfig().set("streamfusion.transpose.batchRows",
              System.getProperty("jsonunion.batchRows", "1024"));
          table.createTemporaryView("src",
              env.fromSequence(0L, ROWS - 1)
                  .map(id -> Row.of(id.intValue(), document(id)))
                  .returns(Types.ROW_NAMED(new String[] {"id", "s"}, Types.INT, Types.STRING)));
          table.executeSql("CREATE TABLE sink (id INT, v STRING) WITH ('connector'='blackhole')");
          table.getConfig().set("streamfusion.native.enabled", Boolean.toString(engine == 1));
          var scan = NativePlanner.install(table);
          String plan = table.explainSql(sql);
          if (trial == 0 && engine == 1) System.out.println(plan);
          if (engine == 1 && EXPECT_NATIVE) {
            assertTrue(plan.contains("NativeUnion") && plan.contains("NativeCalc")
                && plan.contains("RowDataToArrow") && plan.contains("ArrowToRowData"), plan);
          } else assertFalse(plan.contains("NativeCalc"), plan);
          System.out.printf("[json-union-trial-begin] engine=%d trial=%d%n", engine, trial);
          long start = System.nanoTime();
          var result = table.executeSql(sql);
          result.await();
          double seconds = (System.nanoTime() - start) / 1e9;
          if (engine == 1 && EXPECT_NATIVE) {
            assertTrue(scan.substitutions() > 0 && scan.fallbackReasons().isEmpty(), scan.explainSummary());
            assertTrue(metrics.recordsIn(result.getJobClient().orElseThrow().getJobID().toString(),
                "NativeCalc(") >= ROWS);
          } else assertEquals(0, scan.substitutions());
          if (trial >= WARMUP) times[engine][trial - WARMUP] = seconds;
          System.out.printf("[json-union-trial-end] engine=%d trial=%d seconds=%.9f%n", engine, trial, seconds);
        }
      }
    }
    System.out.printf(Locale.ROOT,
        "[json-string-union] rows=%d fixture=%s native_route=%s stock_trials=%s enabled_trials=%s%n",
        ROWS, FIXTURE, EXPECT_NATIVE, Arrays.toString(times[0]), Arrays.toString(times[1]));
  }

  private static String document(long id) {
    if (FIXTURE.equals("unique")) return "{\"v\":\"value-" + id + "\"}";
    if (FIXTURE.equals("ascii")) return "{\"v\":\"ordinary string value\"}";
    if (FIXTURE.equals("wide")) return "{\"v\":\"ordinary string value\",\"a\":1,\"b\":true,"
        + "\"c\":\"longer unrelated field\",\"d\":null,\"e\":{},\"f\":[],\"g\":123,\"h\":false}";
    if (FIXTURE.equals("raw-unicode")) {
      switch ((int) (id % 4)) {
        case 0: return "{\"v\":\"\u4e2d\u6587\u5b57\u7b26\u4e32\"}";
        case 1: return "{\"v\":\"\ud83d\ude00\ue000\"}";
        case 2: return null;
        default: return "{\"v\":\"ordinary string value\"}";
      }
    }
    switch ((int) (id % 8)) {
      case 0: return "{\"v\":\"\\uD800\"}";
      case 1: return "{\"v\":\"?\"}";
      case 2: return "{\"v\":\"\\uDC00\"}";
      case 3: return "{\"v\":\"\\uD83D\\uDE00\"}";
      case 4: return "{\"v\":\"\"}";
      case 5: return "{\"v\":null}";
      case 6: return null;
      default: return "{\"v\":\"ordinary string value\"}";
    }
  }
}

package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class SharedStatefulScalarBenchmark {
  private static final long ROWS = Long.getLong("sharedscalar.rows", 1_000_000L);
  private static final int WARMUP = Integer.getInteger("sharedscalar.warmup", 2);
  private static final int RUNS = Integer.getInteger("sharedscalar.runs", 3);
  private static final int BATCH_ROWS = Integer.getInteger("sharedscalar.batchRows", 1024);
  private static final int NULL_EVERY = Integer.getInteger("sharedscalar.nullEvery", 0);
  private static final boolean EXPECT_NATIVE =
      Boolean.parseBoolean(System.getProperty("sharedscalar.expectNative", "true"));

  @Test
  void sharedCallsKeepBothTransposesAndOneInstance() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"), "Use -Pbench");
    for (String type : System.getProperty("sharedscalar.types", "INT,BIGINT,STRING").split(",")) {
      String sql = "INSERT INTO sink SELECT id, tick(id), tick(id + 1) FROM src";
      double[][] times = new double[2][RUNS];
      try (var metrics = new RecoveryExecutionMetrics()) {
        for (int trial = 0; trial < WARMUP + RUNS; trial++) {
          for (int turn = 0; turn < 2; turn++) {
            int engine = (trial + turn) % 2;
            var env = metrics.environment(new Configuration(), 1, 0);
            var table = StreamTableEnvironment.create(env);
            table.createTemporaryView(
                "src",
                env.fromSequence(0L, ROWS - 1)
                    .map(i -> Row.of(NULL_EVERY > 0 && i % NULL_EVERY == 0 ? null : i.intValue()))
                    .returns(Types.ROW_NAMED(new String[] {"id"}, Types.INT)));
            table.createTemporarySystemFunction(
                "tick",
                type.equals("INT")
                    ? new IntTick()
                    : type.equals("BIGINT") ? new LongTick() : new TextTick());
            table.executeSql(
                "CREATE TABLE sink (id INT, a "
                    + type
                    + ", b "
                    + type
                    + ") WITH ('connector'='blackhole')");
            table.getConfig().set("streamfusion.native.enabled", Boolean.toString(engine == 1));
            table.getConfig().set("streamfusion.transpose.batchRows", Integer.toString(BATCH_ROWS));
            if (System.getProperty("sharedscalar.enabled") != null)
              table
                  .getConfig()
                  .set(
                      "streamfusion.calc.sharedStatefulUdf.enabled",
                      System.getProperty("sharedscalar.enabled"));
            var scan = NativePlanner.install(table);
            String plan = table.explainSql(sql);
            if (engine == 1 && EXPECT_NATIVE)
              assertTrue(
                  plan.contains("NativeCalc")
                      && plan.contains("RowDataToArrow")
                      && plan.contains("ArrowToRowData"),
                  plan);
            else assertFalse(plan.contains("NativeCalc"), plan);
            long directRowsBefore = metrics.counterValue("directPrimitiveCalcRows");
            System.out.println(
                "[shared-scalar-trial-begin] type="
                    + type
                    + " engine="
                    + engine
                    + " trial="
                    + trial);
            long startMillis = System.currentTimeMillis();
            long start = System.nanoTime();
            var result = table.executeSql(sql);
            result.await();
            double seconds = (System.nanoTime() - start) / 1e9;
            System.out.printf(
                Locale.ROOT,
                "[shared-scalar-trial-end] type=%s engine=%d trial=%d start_ms=%d seconds=%.9f%n",
                type,
                engine,
                trial,
                startMillis,
                seconds);
            if (engine == 1 && EXPECT_NATIVE) {
              if (Boolean.getBoolean("sharedscalar.expectDirect"))
                assertEquals(
                    ROWS,
                    metrics.counterValue("directPrimitiveCalcRows") - directRowsBefore,
                    "complete Calc did not use the direct columnar evaluation path");
              assertTrue(
                  scan.substitutions() > 0 && scan.fallbackReasons().isEmpty(),
                  scan.explainSummary());
              assertEquals(
                  ROWS,
                  metrics.recordsIn(
                      result.getJobClient().orElseThrow().getJobID().toString(),
                      "NativeCalc("));
            } else {
              assertEquals(0, scan.substitutions());
              if (engine == 1)
                assertTrue(
                    scan.fallbackReasons().stream()
                        .anyMatch(reason -> reason.contains("shared stateful scalar UDF")),
                    scan.explainSummary());
            }
            if (trial >= WARMUP) times[engine][trial - WARMUP] = seconds;
          }
        }
      }
      System.out.printf(
          Locale.ROOT,
          "[shared-stateful-scalar] rows=%d type=%s native_route=%s batch_rows=%d null_every=%d stock_trials=%s"
              + " enabled_trials=%s%n",
          ROWS,
          type,
          EXPECT_NATIVE,
          BATCH_ROWS,
          NULL_EVERY,
          Arrays.toString(times[0]),
          Arrays.toString(times[1]));
    }
  }

  public static class IntTick extends ScalarFunction {
    private int count;

    public Integer eval(Integer value) {
      return value == null ? null : value + ++count;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  public static class LongTick extends ScalarFunction {
    private long count;

    public Long eval(Integer value) {
      return value == null ? null : value + ++count;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  public static class TextTick extends ScalarFunction {
    private int count;

    public String eval(Integer value) {
      return value == null ? null : "tick-" + (value + ++count);
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }
}

package tech.streamfusion;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class TypedDistinctBenchmark {
  private static final long ROWS = Long.getLong("distinct.rows", 2_000_000L);
  private static final int WARMUP = Integer.getInteger("distinct.warmup", 2);
  private static final int RUNS = Integer.getInteger("distinct.runs", 5);
  private static final String SQL =
      "INSERT INTO sink SELECT k, COUNT(DISTINCT v) FROM src GROUP BY k";
  private static final int CARDINALITY = Integer.getInteger("distinct.cardinality", 4096);
  private static final int NULL_EVERY = Integer.getInteger("distinct.nullEvery", 7);
  private static final boolean SKEW = Boolean.getBoolean("distinct.skew");

  @Test
  void typedDistinct() throws Exception {
    for (String type :
        System.getProperty("distinct.types", "BIGINT,INT,DECIMAL,STRING8,STRING256").split(",")) {
      for (boolean twoPhase : new boolean[] {false, true}) {
        String plan = NativePlanner.explain(environment(twoPhase, type), SQL);
        if (!plan.contains("NativeColumnarGroupAggregate")
            || !plan.contains("RowDataToArrow")
            || !plan.contains("ArrowToRowData")
            || twoPhase && !plan.contains("NativeColumnarLocalGroupAggregate")) {
          throw new IllegalStateException(
              "Expected native aggregation and both transposes: " + plan);
        }
        double[][] times = new double[2][RUNS];
        for (int trial = 0; trial < WARMUP + RUNS; trial++) {
          for (int turn = 0; turn < 2; turn++) {
            int engine = (trial + turn) % 2;
            var table = environment(twoPhase, type);
            var scan = engine == 1 ? NativePlanner.install(table) : null;
            long start = System.nanoTime();
            table.executeSql(SQL).await();
            double seconds = (System.nanoTime() - start) / 1e9;
            if (scan != null && scan.substitutions() == 0) {
              throw new IllegalStateException("Unexpected fallback: " + scan.fallbackReasons());
            }
            if (trial >= WARMUP) times[engine][trial - WARMUP] = seconds;
          }
        }
        System.out.printf(
            Locale.ROOT,
            "[typed-distinct] phase=%s type=%s cardinality=%d nullEvery=%d skew=%s rows=%d"
                + " Flink=%.6fs Native=%.6fs flink_trials=%s native_trials=%s%n",
            twoPhase ? "two" : "one",
            type,
            CARDINALITY,
            NULL_EVERY,
            SKEW,
            ROWS,
            median(times[0]),
            median(times[1]),
            Arrays.toString(times[0]),
            Arrays.toString(times[1]));
      }
    }
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }

  private static TableEnvironment environment(boolean twoPhase, String type) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table
        .getConfig()
        .set("table.optimizer.agg-phase-strategy", twoPhase ? "TWO_PHASE" : "ONE_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(twoPhase));
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("table.exec.mini-batch.size", "1024");
    org.apache.flink.api.common.typeinfo.TypeInformation<?> info;
    switch (type) {
      case "BIGINT":
        info = Types.LONG;
        break;
      case "INT":
        info = Types.INT;
        break;
      case "SMALLINT":
        info = Types.SHORT;
        break;
      case "TINYINT":
        info = Types.BYTE;
        break;
      case "DECIMAL":
        info = Types.BIG_DEC;
        break;
      case "STRING8":
      case "STRING256":
        info = Types.STRING;
        break;
      default:
        throw new IllegalArgumentException(type);
    }
    org.apache.flink.table.types.DataType logical;
    switch (type) {
      case "BIGINT":
        logical = org.apache.flink.table.api.DataTypes.BIGINT();
        break;
      case "INT":
        logical = org.apache.flink.table.api.DataTypes.INT();
        break;
      case "SMALLINT":
        logical = org.apache.flink.table.api.DataTypes.SMALLINT();
        break;
      case "TINYINT":
        logical = org.apache.flink.table.api.DataTypes.TINYINT();
        break;
      case "DECIMAL":
        logical = org.apache.flink.table.api.DataTypes.DECIMAL(20, 2);
        break;
      default:
        logical = org.apache.flink.table.api.DataTypes.STRING();
        break;
    }
    table.createTemporaryView(
        "src",
        table.fromDataStream(
            env.fromSequence(0, ROWS - 1)
                .map(
                    i -> {
                      long v = i / 64 % CARDINALITY - CARDINALITY / 2;
                      Object value;
                      switch (type) {
                        case "BIGINT":
                          value = v;
                          break;
                        case "INT":
                          value = (int) v;
                          break;
                        case "SMALLINT":
                          value = (short) v;
                          break;
                        case "TINYINT":
                          value = (byte) v;
                          break;
                        case "DECIMAL":
                          value = java.math.BigDecimal.valueOf(v).movePointRight(9);
                          break;
                        default:
                          value = Long.toString(v) + "x".repeat(type.equals("STRING8") ? 8 : 256);
                          break;
                      }
                      return Row.of(
                          SKEW && i % 10 != 0 ? 0 : (int) (i % 64),
                          NULL_EVERY > 0 && i % NULL_EVERY == 0 ? null : value);
                    })
                .returns(Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, info)),
            org.apache.flink.table.api.Schema.newBuilder()
                .column("k", org.apache.flink.table.api.DataTypes.INT())
                .column("v", logical)
                .build()));
    table.executeSql("CREATE TABLE sink (k INT, n BIGINT) WITH ('connector' = 'blackhole')");
    return table;
  }
}

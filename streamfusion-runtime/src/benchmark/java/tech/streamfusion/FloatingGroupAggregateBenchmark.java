package tech.streamfusion;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class FloatingGroupAggregateBenchmark {
  private static final long ROWS = Long.getLong("floating.group.rows", 2_000_000L);
  private static final long SINGLE_ROWS = Long.getLong("floating.group.singleRows", 500_000L);
  private static final int KEYS = Integer.getInteger("floating.group.keys", 64);
  private static final int WARMUP = Integer.getInteger("floating.group.warmup", 2);
  private static final int RUNS = Integer.getInteger("floating.group.runs", 5);
  private static final boolean REQUIRE_NATIVE =
      Boolean.parseBoolean(System.getProperty("floating.group.requireNative", "true"));

  @Test
  void floatingGroups() throws Exception {
    for (String profile :
        System.getProperty(
                "floating.group.profiles",
                "float-reductions-one,float-reductions-two,float-first-last,double-first-last,"
                    + "float-single,double-single,float-single-two,double-single-two,float-distinct-one,"
                    + "float-retract-reductions,float-retract-first-last,double-retract-first-last")
            .split(",")) {
      String query = query(profile);
      String plan = NativePlanner.explain(environment(profile), query);
      if (REQUIRE_NATIVE
          && (!plan.contains("NativeColumnarGroupAggregate")
              || !plan.contains("RowDataToArrow")
              || !plan.contains("ArrowToRowData")
              || (profile.endsWith("-two")
                  && !plan.contains("NativeColumnarLocalGroupAggregate")))) {
        throw new IllegalStateException("Expected native aggregation and both transposes: " + plan);
      }
      double[][] times = new double[2][RUNS];
      for (int trial = 0; trial < WARMUP + RUNS; trial++) {
        for (int turn = 0; turn < 2; turn++) {
          int engine = (trial + turn) % 2;
          TableEnvironment table = environment(profile);
          PhysicalPlanScan scan = engine == 1 ? NativePlanner.install(table) : null;
          long start = System.nanoTime();
          table.executeSql(query).await();
          double seconds = (System.nanoTime() - start) / 1e9;
          if (REQUIRE_NATIVE && scan != null && scan.substitutions() == 0)
            throw new IllegalStateException("Unexpected fallback: " + scan.fallbackReasons());
          if (!REQUIRE_NATIVE && scan != null && scan.substitutions() != 0)
            throw new IllegalStateException("The previous-behavior control must use fallback");
          if (scan != null && trial == 0)
            System.out.printf(
                Locale.ROOT,
                "[floating-group-route] profile=%s substitutions=%d fallback=%s%n",
                profile,
                scan.substitutions(),
                scan.fallbackReasons());
          System.out.printf(
              Locale.ROOT,
              "[floating-group-trial] profile=%s engine=%s warmup=%s trial=%d seconds=%.6f%n",
              profile,
              engine == 0 ? "flink" : "native",
              trial < WARMUP,
              trial,
              seconds);
          if (trial >= WARMUP) times[engine][trial - WARMUP] = seconds;
        }
      }
      System.out.printf(
          Locale.ROOT,
          "[floating-group] profile=%s rows=%d keys=%d flink=%.6f native=%.6f ratio=%.3f"
              + " flink_trials=%s native_trials=%s%n",
          profile,
          rowCount(profile),
          profile.contains("-single") ? rowCount(profile) : KEYS,
          median(times[0]),
          median(times[1]),
          median(times[0]) / median(times[1]),
          Arrays.toString(times[0]),
          Arrays.toString(times[1]));
    }
  }

  private static String query(String profile) {
    String aggregates =
        profile.contains("distinct")
            ? "SUM(DISTINCT v), MIN(v), MAX(v)"
            : profile.contains("reductions")
                ? "SUM(v), MIN(v), MAX(v)"
                : profile.contains("-single") ? "SINGLE_VALUE(v)" : "FIRST_VALUE(v), LAST_VALUE(v)";
    return "INSERT INTO sink SELECT k, " + aggregates + " FROM inputs GROUP BY k";
  }

  private static long rowCount(String profile) {
    return profile.contains("-single") ? SINGLE_ROWS : ROWS;
  }

  private static TableEnvironment environment(String profile) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    boolean twoPhase = profile.endsWith("-two");
    table
        .getConfig()
        .set("table.optimizer.agg-phase-strategy", twoPhase ? "TWO_PHASE" : "ONE_PHASE");
    if (twoPhase) {
      table.getConfig().set("table.exec.mini-batch.enabled", "true");
      table.getConfig().set("table.exec.mini-batch.size", "1024");
      table.getConfig().set("table.exec.mini-batch.allow-latency", "100 ms");
    }
    boolean single = profile.contains("-single");
    boolean wide = profile.startsWith("double-");
    var dataType = wide ? DataTypes.DOUBLE() : DataTypes.FLOAT();
    var stream =
        env.fromSequence(0, rowCount(profile) - 1)
            .map(
                i -> {
                  if (profile.contains("-retract-")) return retractingRow(i, wide);
                  Object value = null;
                  if (i / KEYS % 8 != 0) {
                    if (wide) value = (i % 1024) * 0.25d;
                    else value = (i % 1024) * 0.25f;
                  }
                  return Row.of(single ? i.intValue() : (int) (i % KEYS), value);
                })
            .returns(
                Types.ROW_NAMED(
                    new String[] {"k", "v"}, Types.INT, wide ? Types.DOUBLE : Types.FLOAT));
    var schema = Schema.newBuilder().column("k", DataTypes.INT()).column("v", dataType).build();
    table.createTemporaryView(
        "inputs",
        profile.contains("-retract-")
            ? table.fromChangelogStream(stream, schema)
            : table.fromDataStream(stream, schema));
    String type = wide ? "DOUBLE" : "FLOAT";
    String columns =
        profile.contains("reductions") || profile.contains("distinct")
            ? ", min_v " + type + ", max_v " + type
            : single ? "" : ", last_v " + type;
    table.executeSql(
        "CREATE TABLE sink (k INT, value_v " + type + columns + ") WITH ('connector'='blackhole')");
    return table;
  }

  private static Row retractingRow(long ordinal, boolean wide) {
    long cycle;
    int key;
    RowKind kind;
    if (ordinal < 2L * KEYS) {
      key = (int) (ordinal % KEYS);
      cycle = ordinal / KEYS - 1;
      kind = RowKind.INSERT;
    } else {
      long pair = (ordinal - 2L * KEYS) / 2;
      key = (int) (pair % KEYS);
      boolean before = (ordinal - 2L * KEYS) % 2 == 0;
      cycle = pair / KEYS + (before ? 0 : 1);
      kind = before ? RowKind.UPDATE_BEFORE : RowKind.UPDATE_AFTER;
    }
    Object value = null;
    if (cycle < 0 || cycle % 8 != 0) {
      double numeric = cycle < 0 ? -1000 : (cycle % 1024) * 0.25;
      if (wide) value = numeric;
      else value = (float) numeric;
    }
    return Row.ofKind(kind, key, value);
  }

  private static double median(double[] times) {
    double[] sorted = times.clone();
    Arrays.sort(sorted);
    int middle = sorted.length / 2;
    return sorted.length % 2 == 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
  }
}

package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.annotation.*;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class MapScalarUdfBenchmark {
  @Test
  void completeJobsKeepSourceConstructionAndBothTransposes() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"));
    int count = Integer.getInteger("mapbench.rows", 65536);
    int width = Integer.getInteger("mapbench.width", 64);
    int warmups = Integer.getInteger("mapbench.warmups", 2);
    int runs = Integer.getInteger("mapbench.runs", 5);
    boolean expectedNative =
        Boolean.parseBoolean(System.getProperty("mapbench.expectNative", "true"));
    var samples = new double[2][runs];
    for (int trial = 0; trial < warmups + runs; trial++) {
      for (int order = 0; order < 2; order++) {
        int engine = (trial + order) & 1;
        try (var metrics = new RecoveryExecutionMetrics()) {
          var table = environment(metrics, count, width);
          String query = "SELECT id,map_identity(p) FROM src";
          table.createTemporaryTable(
              "sink",
              TableDescriptor.forConnector("blackhole")
                  .schema(
                      Schema.newBuilder()
                          .fromRowDataType(
                              table.sqlQuery(query).getResolvedSchema().toPhysicalRowDataType())
                          .build())
                  .build());
          var scan = engine == 1 ? NativePlanner.install(table) : null;
          String insert = "INSERT INTO sink " + query;
          String plan = table.explainSql(insert);
          if (engine == 1 && expectedNative)
            assertTrue(
                plan.contains("NativeCalc")
                    && plan.contains("RowDataToArrow")
                    && plan.contains("ArrowToRowData"),
                plan);
          else if (engine == 1) assertEquals(0, scan.substitutions(), plan);
          else assertFalse(plan.contains("NativeCalc"), plan);
          long clock = System.nanoTime();
          table.executeSql(insert).await();
          double seconds = (System.nanoTime() - clock) / 1e9;
          System.out.printf(
              Locale.ROOT,
              "[map-udf-job] engine=%s trial=%d warmup=%s rows=%d width=%d seconds=%.9f%n",
              engine == 0 ? "Flink" : "StreamFusion",
              trial,
              trial < warmups,
              count,
              width,
              seconds);
          if (trial >= warmups) samples[engine][trial - warmups] = seconds;
          if (engine == 1 && expectedNative) {
            RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
            long consumed =
                metrics.nativeOperators().stream()
                    .filter(o -> ((String) o.get("operator")).startsWith("NativeCalc"))
                    .mapToLong(o -> ((Number) o.get("recordsIn")).longValue())
                    .sum();
            assertEquals(count, consumed);
          } else assertTrue(metrics.nativeOperators().isEmpty());
        }
      }
    }
    var stock = samples[0].clone();
    var candidate = samples[1].clone();
    Arrays.sort(stock);
    Arrays.sort(candidate);
    System.out.printf(
        Locale.ROOT,
        "[map-udf-job-summary] rows=%d width=%d stock=%.9f native=%.9f stock_trials=%s"
            + " native_trials=%s%n",
        count,
        width,
        stock[runs / 2],
        candidate[runs / 2],
        Arrays.toString(samples[0]),
        Arrays.toString(samples[1]));
  }

  private static TableEnvironment environment(
      RecoveryExecutionMetrics metrics, int count, int width) {
    var env = metrics.environment(new Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", "1024");
    var type =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("p", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));
    String[] keys = new String[width];
    for (int i = 0; i < width; i++) keys[i] = "key-" + i;
    table.createTemporaryView(
        "src",
        env.fromSequence(0, count - 1)
            .map(
                id -> {
                  if (id % 11 == 0) return Row.of(id, null);
                  var map = new LinkedHashMap<String, Integer>();
                  if (id % 17 != 0)
                    for (int j = 0; j < width; j++)
                      map.put(
                          j == 0 && id % 7 == 0 ? null : keys[j],
                          j % 11 == 0 ? null : Integer.valueOf(id.intValue() + j));
                  return Row.of(id, map);
                })
            .returns(ExternalTypeInfo.<Row>of(type)),
        Schema.newBuilder().fromRowDataType(type).build());
    table.createTemporarySystemFunction("map_identity", new Identity());
    return table;
  }

  @FunctionHint(input = @DataTypeHint("MAP<STRING,INT>"), output = @DataTypeHint("MAP<STRING,INT>"))
  public static class Identity extends ScalarFunction {
    public Map<String, Integer> eval(Map<String, Integer> input) {
      return input;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }
}

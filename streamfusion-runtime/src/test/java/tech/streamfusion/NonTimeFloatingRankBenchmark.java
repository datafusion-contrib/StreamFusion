package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NonTimeFloatingRankBenchmark {
  @Test
  void completeJobsKeepStateEveryRankChangeAndBothTransposes() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"), "Use -Pbench");
    for (String type : System.getProperty("rank.float.types", "FLOAT,DOUBLE").split(","))
      for (String arrival :
          System.getProperty("rank.float.arrivals", "ascending,descending").split(","))
        measure(type, arrival);
  }

  private static void measure(String type, String arrival) throws Exception {
    long rows = Long.getLong("rank.float.rows", 131072);
    int keys = Integer.getInteger("rank.float.keys", 1024);
    int warmups = Integer.getInteger("rank.float.warmup", 2);
    int runs = Integer.getInteger("rank.float.runs", 3);
    String corpus = System.getProperty("rank.float.corpus", "mixed");
    int nullEvery = Integer.getInteger("rank.float.nullEvery", corpus.equals("all_nan") ? 0 : 31);
    boolean expected = Boolean.parseBoolean(System.getProperty("rank.float.expectNative", "true"));
    boolean single = type.equals("FLOAT");
    boolean integer = type.equals("BIGINT");
    if (!single && !integer && !type.equals("DOUBLE")) throw new IllegalArgumentException(type);
    var dataType =
        DataTypes.ROW(
            DataTypes.FIELD("k", DataTypes.BIGINT()),
            DataTypes.FIELD(
                "v",
                integer ? DataTypes.BIGINT() : single ? DataTypes.FLOAT() : DataTypes.DOUBLE()),
            DataTypes.FIELD("id", DataTypes.BIGINT().notNull()));
    String sql =
        "SELECT k, v, id, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v ASC NULLS FIRST) AS rn FROM"
            + " src";
    double[][] times = new double[2][runs];
    for (int trial = 0; trial < warmups + runs; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        int engine = (trial + turn) % 2;
        try (var metrics = new RecoveryExecutionMetrics()) {
          var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
          var table = StreamTableEnvironment.create(env);
          table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
          var source =
              env.fromSequence(0, rows - 1)
                  .map(
                      i -> {
                        long ordinal = i / keys;
                        double base =
                            arrival.equals("descending") ? rows / keys - ordinal : ordinal;
                        Object value = null;
                        if (nullEvery <= 0 || i % nullEvery != 0) {
                          if (integer) value = (long) base;
                          else if (single) {
                            float number = (float) base;
                            if (corpus.equals("all_nan")
                                || corpus.equals("mixed") && ordinal % 17 == 7)
                              number = Float.intBitsToFloat(i % 2 == 0 ? 0x7fc04321 : 0xffc01234);
                            else if (corpus.equals("mixed") && ordinal % 29 == 15)
                              number = i % 2 == 0 ? 0.0f : -0.0f;
                            else if (corpus.equals("mixed") && ordinal % 43 == 22)
                              number = Float.intBitsToFloat(1);
                            else if (corpus.equals("mixed") && ordinal % 53 == 41)
                              number = Float.NEGATIVE_INFINITY;
                            else if (corpus.equals("mixed") && ordinal % 53 == 42)
                              number = Float.POSITIVE_INFINITY;
                            value = number;
                          } else {
                            double number = base;
                            if (corpus.equals("all_nan")
                                || corpus.equals("mixed") && ordinal % 17 == 7)
                              number =
                                  Double.longBitsToDouble(
                                      i % 2 == 0 ? 0x7ff8000000004321L : 0xfff8000000001234L);
                            else if (corpus.equals("mixed") && ordinal % 29 == 15)
                              number = i % 2 == 0 ? 0.0 : -0.0;
                            else if (corpus.equals("mixed") && ordinal % 43 == 22)
                              number = Double.longBitsToDouble(1);
                            else if (corpus.equals("mixed") && ordinal % 53 == 41)
                              number = Double.NEGATIVE_INFINITY;
                            else if (corpus.equals("mixed") && ordinal % 53 == 42)
                              number = Double.POSITIVE_INFINITY;
                            value = number;
                          }
                        }
                        return Row.of(i % keys, value, i);
                      })
                  .returns(ExternalTypeInfo.<Row>of(dataType));
          table.createTemporaryView(
              "src", source, Schema.newBuilder().fromRowDataType(dataType).build());
          if (engine == 1) NativePlanner.install(table);
          var result = table.sqlQuery(sql);
          table.createTemporaryTable(
              "sink",
              TableDescriptor.forConnector("blackhole")
                  .schema(
                      Schema.newBuilder().fromResolvedSchema(result.getResolvedSchema()).build())
                  .build());
          String insert = "INSERT INTO sink " + sql;
          String plan = table.explainSql(insert);
          if (engine == 1 && expected)
            assertTrue(
                plan.contains("NativeColumnarTopN")
                    && plan.contains("RowDataToArrow")
                    && plan.contains("ArrowToRowData"),
                plan);
          if (engine == 1 && !expected) assertFalse(plan.contains("NativeColumnarTopN"), plan);
          long start = System.nanoTime();
          table.executeSql(insert).await();
          double seconds = (System.nanoTime() - start) / 1e9;
          if (engine == 1 && expected)
            RecoveryExecutionMetrics.assertConsumed(
                metrics.nativeOperators(), "NativeColumnarTopN");
          System.out.printf(
              Locale.ROOT,
              "[floating-rank-trial] type=%s arrival=%s corpus=%s rows=%d keys=%d nullEvery=%d"
                  + " engine=%s trial=%d warmup=%s seconds=%.6f%n",
              type,
              arrival,
              corpus,
              rows,
              keys,
              nullEvery,
              engine == 0 ? "Flink" : "StreamFusion",
              trial,
              trial < warmups,
              seconds);
          if (trial >= warmups) times[engine][trial - warmups] = seconds;
        }
      }
    }
    System.out.printf(
        Locale.ROOT,
        "[floating-rank] type=%s arrival=%s corpus=%s rows=%d keys=%d nullEvery=%d host_mean=%.6fs"
            + " native_mean=%.6fs host_trials=%s native_trials=%s%n",
        type,
        arrival,
        corpus,
        rows,
        keys,
        nullEvery,
        Arrays.stream(times[0]).average().orElseThrow(),
        Arrays.stream(times[1]).average().orElseThrow(),
        Arrays.toString(times[0]),
        Arrays.toString(times[1]));
  }
}

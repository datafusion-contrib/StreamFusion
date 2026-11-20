package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

/** Whole-job comparison diagnostic with a row source and row sink. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class PairwiseStringComparisonPipelineBenchmark {
  @ParameterizedTest
  @ValueSource(strings = {"bytes", "utf16", "mixed"})
  void rowSourceToRowSink(String cacheMode) throws Exception {
    int count = Integer.getInteger("string.compare.rows", 2_000_000);
    int width = Integer.getInteger("string.compare.width", 192);
    int warmups = Integer.getInteger("string.compare.warmups", 3);
    int runs = Integer.getInteger("string.compare.runs", 5);
    String variant = System.getProperty("string.compare.variant", "candidate");
    boolean equality = System.getProperty("string.compare.operator", "lt").equals("eq");
    boolean sibling =
        Boolean.parseBoolean(
            System.getProperty("string.compare.withLength", equality ? "false" : "true"));
    for (int trial = 0; trial < warmups + runs; trial++) {
      for (int order = 0; order < 2; order++) {
        boolean nativeRun = (trial + order) % 2 != 0;
        try (var metrics = new RecoveryExecutionMetrics()) {
          var table = source(metrics, count, width, cacheMode, equality);
          table.executeSql("CREATE TABLE output_rows(v BOOLEAN"
              + (sibling ? ", n INT" : "") + ") WITH ('connector'='blackhole')");
          String sql = "INSERT INTO output_rows SELECT " + (equality ? "s = t" : "s < t")
              + (sibling ? ", CHAR_LENGTH(s) + CHAR_LENGTH(t)" : "") + " FROM src";
          boolean candidate = nativeRun && variant.equals("candidate") && (equality || sibling);
          if (nativeRun) {
            var scan = NativePlanner.install(table);
            String plan = table.explainSql(sql);
            if (candidate) {
              assertTrue(plan.contains("pairwiseStringComparison=[true]"), () -> plan + scan.fallbackReasons());
              assertTrue(plan.contains("RowDataToArrow") && plan.contains("ArrowToRowData"), plan);
            }
            if (trial == 0) System.out.println(plan);
          }
          long start = System.nanoTime();
          table.executeSql(sql).await();
          double seconds = (System.nanoTime() - start) / 1e9;
          if (candidate) {
            RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
            System.out.printf(
                Locale.ROOT, "comparison_transposes,%s,%s,%d,%d,%d%n",
                equality ? "eq" : "lt", cacheMode, trial,
                metrics.counterValue("conversionTime"), metrics.counterValue("convertTime"));
          }
          System.out.printf(
              Locale.ROOT, "comparison_pipeline,%s,%s,%s,%d,%d,%d,%s,%.9f%n",
              equality ? "eq" : "lt", nativeRun ? variant : "flink", cacheMode, count, width,
              trial, trial < warmups, seconds);
        }
      }
    }
  }

  private static StreamTableEnvironment source(
      RecoveryExecutionMetrics metrics, int count, int width, String cacheMode, boolean equality) {
    var env = metrics.environment(new Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    var type = DataTypes.ROW(
        DataTypes.FIELD("s", DataTypes.STRING()),
        DataTypes.FIELD("t", DataTypes.STRING()));
    String filler = "\u4e2d".repeat(width / 3);
    String astral = filler + "\ud800\udc00";
    String bmp = filler + "\ue000";
    byte[] astralBytes = astral.getBytes(StandardCharsets.UTF_8);
    byte[] bmpBytes = bmp.getBytes(StandardCharsets.UTF_8);
    var input = env.fromSequence(0, count - 1)
        .<RowData>map(
            id -> {
              boolean swapped = id % 8 == 1;
              boolean rightSwapped = equality && (id & 1) == 0 ? swapped : !swapped;
              boolean leftCached = cacheMode.equals("utf16")
                  || cacheMode.equals("mixed") && (id & 1) == 0;
              boolean rightCached = cacheMode.equals("utf16")
                  || cacheMode.equals("mixed") && (id & 2) == 0;
              return GenericRowData.of(
                  id % 31 == 0
                      ? null
                      : value(swapped, leftCached, astral, bmp, astralBytes, bmpBytes),
                  value(rightSwapped, rightCached, astral, bmp, astralBytes, bmpBytes));
            })
        .returns(InternalTypeInfo.of((RowType) type.getLogicalType()));
    table.createTemporaryView("src", input, Schema.newBuilder().fromRowDataType(type).build());
    return table;
  }

  private static BinaryStringData value(
      boolean astralValue, boolean cached, String astral, String bmp, byte[] astralBytes,
      byte[] bmpBytes) {
    return cached
        ? BinaryStringData.fromString(astralValue ? astral : bmp)
        : BinaryStringData.fromBytes(astralValue ? astralBytes : bmpBytes);
  }
}

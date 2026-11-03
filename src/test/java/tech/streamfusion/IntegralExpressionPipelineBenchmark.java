package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Locale;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

/** Scalar expressions retaining the rowwise source/sink, JNI and both transposes. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class IntegralExpressionPipelineBenchmark {
  @Test
  void boundedRuntimeExpressionWithBothPerimeterTransposes() throws Exception {
    int count = Integer.getInteger("integral.expression.rows", 1_000_000);
    int warmups = Integer.getInteger("integral.expression.warmups", 2);
    int runs = Integer.getInteger("integral.expression.runs", 5);
    boolean previous = Boolean.getBoolean("sf.integral.previous");
    boolean fallback = previous && Boolean.getBoolean("sf.integral.fallbackControl");
    boolean booleanInput = Boolean.getBoolean("sf.integral.booleanInput");
    boolean bigintInput = Boolean.getBoolean("sf.integral.bigintInput");
    int decimalPrecision = Integer.getInteger("sf.integral.decimalPrecision", 0);
    int decimalScale = Integer.getInteger("sf.integral.decimalScale", 0);
    int timestampPrecision = Integer.getInteger("sf.integral.timestampPrecision", -1);
    boolean epochMillisInput = Boolean.getBoolean("sf.integral.epochMillisInput");
    boolean epochSecondsInput = Boolean.getBoolean("sf.integral.epochSecondsInput");
    boolean notNull = Boolean.getBoolean("sf.integral.primaryNotNull");
    String secondary = System.getProperty("sf.integral.secondaryType", "");
    assertTrue(secondary.isEmpty() || secondary.equals("INT") || secondary.equals("BIGINT"));
    var primary = timestampPrecision >= 0 ? DataTypes.TIMESTAMP_LTZ(timestampPrecision)
        : decimalPrecision > 0 ? DataTypes.DECIMAL(decimalPrecision, decimalScale)
        : booleanInput ? DataTypes.BOOLEAN()
        : bigintInput || epochMillisInput || epochSecondsInput ? DataTypes.BIGINT() : DataTypes.INT();
    var inputType = notNull ? primary.notNull() : primary;
    var second = secondary.equals("BIGINT") ? DataTypes.BIGINT() : DataTypes.INT();
    var sourceType = secondary.isEmpty() ? DataTypes.ROW(DataTypes.FIELD("v", inputType))
        : DataTypes.ROW(DataTypes.FIELD("v", inputType),
            DataTypes.FIELD("n", notNull ? second.notNull() : second));
    String expression = System.getProperty("sf.integral.expression", "ABS(v+CAST(1 AS $TYPE))")
        .replace("$TYPE", bigintInput ? "BIGINT" : "INT");
    for (int trial = 0; trial < warmups + runs; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        boolean nativeRun = ((trial + turn) & 1) == 1;
        try (var metrics = new RecoveryExecutionMetrics()) {
          var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
          var table = StreamTableEnvironment.create(env);
          if (timestampPrecision >= 0 || epochMillisInput || epochSecondsInput) table.getConfig().setLocalTimeZone(
              java.time.ZoneId.of(System.getProperty("sf.integral.zone", "UTC")));
          table.getConfig().set("table.exec.legacy-cast-behaviour",
              Boolean.getBoolean("sf.integral.legacy") ? "ENABLED" : "DISABLED");
          BigInteger maximum = BigInteger.TEN.pow(decimalPrecision).subtract(BigInteger.ONE);
          var source = env.fromSequence(0, count - 1).map(i -> {
            Object value;
            if (epochSecondsInput) {
              value = tech.streamfusion.planner.IntegralExpressionBenchmarkFunctions.epochSeconds(i);
            } else if (epochMillisInput) {
              value = tech.streamfusion.planner.IntegralExpressionBenchmarkFunctions.epochMillis(i);
            } else if (timestampPrecision >= 0) {
              value = java.time.Instant.ofEpochMilli(
                  tech.streamfusion.planner.IntegralExpressionBenchmarkFunctions.temporalMillis(i))
                  .plusNanos(timestampPrecision > 3 ? i % 1024 * 7919 % 1000000 : 0);
            } else if (decimalPrecision > 0) {
              BigInteger unscaled = i % 17 == 0 ? maximum : i % 17 == 1 ? maximum.negate()
                  : BigInteger.valueOf((i % 2 == 0 ? -1L : 1L)
                      * (decimalPrecision == 4 ? i * 7919L % 10000 : 9007199254740993L + i * 7919L));
              value = new BigDecimal(unscaled, decimalScale);
            } else if (booleanInput) value = i % 2 == 0;
            else if (bigintInput) value = i % 17 == 0 ? Long.MIN_VALUE
                : (i % 2 == 0 ? -1L : 1L) * (9007199254740993L + i * 7919L);
            else value = i % 17 == 0 ? Integer.MIN_VALUE : (int) ((i % 2 == 0 ? -1 : 1) * i * 7919);
            Object first = !notNull && i % 7 == 0 ? null : value;
            if (secondary.isEmpty()) return Row.of(first);
            long index = i + 3;
            Object other;
            if (secondary.equals("INT")) other = index % 17 == 0 ? Integer.MIN_VALUE
                : (int) ((index % 2 == 0 ? -1L : 1L) * index * 7919L);
            else other = index % 17 == 0 ? Long.MIN_VALUE
                : (index % 2 == 0 ? -1L : 1L) * (9007199254740993L + index * 7919L);
            return Row.of(first, !notNull && i % 11 == 0 ? null : other);
          }).returns(ExternalTypeInfo.<Row>of(sourceType));
          table.createTemporaryView("inputs", source,
              Schema.newBuilder().fromRowDataType(sourceType).build());
          var resultType = table.sqlQuery("SELECT " + expression + " FROM inputs")
              .getResolvedSchema().getColumnDataTypes().get(0).getLogicalType();
          table.executeSql("CREATE TABLE output_rows(v " + resultType.asSerializableString()
              + ") WITH ('connector'='blackhole')");
          String sql = "INSERT INTO output_rows SELECT " + expression + " FROM inputs";
          var scan = nativeRun ? NativePlanner.install(table) : null;
          String plan = table.explainSql(sql);
          if (nativeRun && !fallback) {
            assertTrue(plan.contains("NativeCalc"), plan);
            assertTrue(plan.contains("RowDataToArrow"), plan);
            assertTrue(plan.contains("ArrowToRowData"), plan);
            assertFalse(scan.functionExecutions().isEmpty());
            assertEquals(!previous,
                scan.functionExecutions().stream().allMatch(route -> route.contains("jvm=[]")),
                scan.functionExecutions().toString());
            assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
          } else if (nativeRun) {
            assertFalse(plan.contains("NativeCalc"), plan);
            assertEquals(0, scan.substitutions());
            String expectedReason = System.getProperty(
                "sf.integral.fallbackReason", "unsupported TRY_CAST DECIMAL");
            assertTrue(scan.fallbackReasons().stream()
                .anyMatch(reason -> reason.contains(expectedReason)),
                scan.fallbackReasons().toString());
          }
          long start = System.nanoTime();
          table.executeSql(sql).await();
          double elapsed = (System.nanoTime() - start) / 1e9;
          if (nativeRun && !fallback)
            RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
          if (nativeRun && fallback) assertTrue(metrics.nativeOperators().isEmpty());
          System.out.printf(Locale.ROOT,
              "[integral-expression-pipeline] engine=%s trial=%d warmup=%s rows=%d seconds=%.9f%n",
              nativeRun ? previous ? "previous" : "native" : "stock", trial, trial < warmups, count, elapsed);
        }
      }
    }
  }
}

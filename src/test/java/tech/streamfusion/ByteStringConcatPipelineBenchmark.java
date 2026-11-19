package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

/** Whole-job CONCAT diagnostic, including the row source, both transposes and a row sink. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class ByteStringConcatPipelineBenchmark {
  @Test
  void rowSourceToRowSink() throws Exception {
    run("CONCAT(s, '!')", false);
  }

  @Test
  void rowSourceThroughWatermarkToRowSink() throws Exception {
    run("CONCAT(s, '!')", true);
  }

  @Test
  void numericFilterThroughWatermarkToRowSink() throws Exception {
    run("s", true);
  }

  private static void run(String projection, boolean watermark) throws Exception {
    int count = Integer.getInteger("byte.concat.rows", 2_000_000);
    int width = Integer.getInteger("byte.concat.width", 16);
    boolean unicode = Boolean.getBoolean("byte.concat.unicode");
    int warmups = Integer.getInteger("byte.concat.warmups", 3);
    int runs = Integer.getInteger("byte.concat.runs", 5);
    for (int trial = 0; trial < warmups + runs; trial++) {
      for (int order = 0; order < 2; order++) {
        boolean nativeRun = (trial + order) % 2 != 0;
        try (var metrics = new RecoveryExecutionMetrics()) {
          var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
          var table = StreamTableEnvironment.create(env);
          String prefix = (unicode ? "\u4e2d" : "a").repeat(width);
          if (watermark) {
            var type = DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT()),
                DataTypes.FIELD("s", DataTypes.STRING()),
                DataTypes.FIELD("rt", DataTypes.TIMESTAMP(3)));
            byte[] valid = prefix.getBytes(StandardCharsets.UTF_8);
            byte[] malformed = Arrays.copyOf(valid, Math.max(1, valid.length));
            malformed[0] = (byte) 0xff;
            var input = env.fromSequence(0, count - 1)
                .<RowData>map(i -> GenericRowData.of(
                    i, BinaryStringData.fromBytes(i % 1000 == 0 ? malformed : valid),
                    TimestampData.fromEpochMillis(1000 + i / 1000)))
                .returns(InternalTypeInfo.of((RowType) type.getLogicalType()));
            table.createTemporaryView(
                "src", input, Schema.newBuilder().fromRowDataType(type)
                    .watermark("rt", "rt - INTERVAL '1' SECOND").build());
          } else {
            var input = env.fromSequence(0, count - 1)
                .map(i -> Row.of(prefix + i % 256))
                .returns(Types.ROW_NAMED(new String[] {"s"}, Types.STRING));
            table.createTemporaryView(
                "src", input, Schema.newBuilder().column("s", DataTypes.STRING()).build());
          }
          table.executeSql("CREATE TABLE output_rows(v STRING) WITH ('connector'='blackhole')");
          String sql = "INSERT INTO output_rows SELECT " + projection + " FROM src"
              + (watermark ? " WHERE id < " + count / 2 : "");
          String expectedOperator = projection.equals("s") ? "NativeFilter" : "NativeCalc";
          if (nativeRun) {
            var scan = NativePlanner.install(table);
            String plan = table.explainSql(sql);
            assertTrue(plan.contains(expectedOperator), plan);
            if (watermark) assertTrue(plan.contains("NativeWatermarkAssigner"), plan);
            assertTrue(plan.contains("RowDataToArrow"), plan);
            assertTrue(plan.contains("ArrowToRowData"), plan);
            assertTrue(plan.contains("byteStrings="), plan);
            assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
          }
          long start = System.nanoTime();
          table.executeSql(sql).await();
          double seconds = (System.nanoTime() - start) / 1e9;
          if (nativeRun)
            RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), expectedOperator);
          System.out.printf(
              Locale.ROOT,
              "%s%s,%s,%d,%d,%s,%d,%s,%.9f%n",
              projection.equals("s") ? "byte_filter_pipeline" : "byte_concat_pipeline",
              watermark ? "_watermark" : "",
              nativeRun ? "native" : "flink",
              count,
              width,
              unicode,
              trial,
              trial < warmups,
              seconds);
        }
      }
    }
  }
}

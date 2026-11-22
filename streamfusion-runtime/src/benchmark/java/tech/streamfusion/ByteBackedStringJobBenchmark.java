package tech.streamfusion;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class ByteBackedStringJobBenchmark {
  private static final long ROWS = Long.getLong("byte.strings.rows", 1_000_000L);
  private static final int KEYS = Integer.getInteger("byte.strings.keys", 4096);
  private static final int WIDTH = Integer.getInteger("byte.strings.width", 64);
  private static final int WARMUP = Integer.getInteger("byte.strings.warmup", 2);
  private static final int RUNS = Integer.getInteger("byte.strings.runs", 5);

  @Test
  void compareWithStockFlink() throws Exception {
    for (String workload : System.getProperty("byte.strings.workloads", "group,trim,udf").split(",")) {
      String sql;
      String output;
      switch (workload) {
        case "group":
          sql = "SELECT CONCAT(s, '!'), COUNT(*) FROM src GROUP BY CONCAT(s, '!')";
          output = "v STRING, n BIGINT";
          break;
        case "trim":
          sql = "SELECT TRIM(BOTH chars FROM s) FROM src";
          output = "v STRING";
          break;
        case "udf":
          sql = "SELECT TO_BASE64(emit_bytes(id)) FROM src";
          output = "v STRING";
          break;
        default:
          throw new IllegalArgumentException(workload);
      }
      double[][] seconds = new double[2][RUNS];
      for (int trial = 0; trial < WARMUP + RUNS; trial++) {
        for (int turn = 0; turn < 2; turn++) {
          int engine = (trial + turn) % 2;
          try (var metrics = RecoveryExecutionMetrics.countersOnly()) {
            var table = environment(metrics);
            table.executeSql("CREATE TABLE sink (" + output + ") WITH ('connector' = 'blackhole')");
            var scan = engine == 1 ? NativePlanner.install(table) : null;
            String insert = "INSERT INTO sink " + sql;
            String plan = table.explainSql(insert);
            if (scan != null
                && (!plan.contains("RowDataToArrow") || !plan.contains("ArrowToRowData")
                    || workload.equals("trim") && plan.contains("onDemandUtf8"))) {
              throw new IllegalStateException(plan + scan.fallbackReasons().toString());
            }
            long start = System.nanoTime();
            table.executeSql(insert).await();
            double elapsed = (System.nanoTime() - start) / 1e9;
            if (scan != null) {
              if (!scan.fallbackReasons().isEmpty()) {
                throw new IllegalStateException(scan.fallbackReasons().toString());
              }
              RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
              if (workload.equals("group")) {
                RecoveryExecutionMetrics.assertConsumed(
                    metrics.nativeOperators(), "NativeColumnarGroupAggregate");
              }
            }
            if (trial >= WARMUP) seconds[engine][trial - WARMUP] = elapsed;
          }
        }
      }
      System.out.printf(Locale.ROOT, "[byte-strings] %s rows=%d keys=%d width=%d stock=%s native=%s%n",
          workload, ROWS, KEYS, WIDTH, Arrays.toString(seconds[0]), Arrays.toString(seconds[1]));
    }
  }

  private static StreamTableEnvironment environment(RecoveryExecutionMetrics metrics) {
    var env = metrics.environment(new Configuration(), 1, 0);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    if (Boolean.getBoolean("byte.strings.miniBatch")) {
      table.getConfig().set("table.exec.mini-batch.enabled", "true");
      table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
      table.getConfig().set("table.exec.mini-batch.size", "1024");
    }
    var type = DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.BIGINT()),
        DataTypes.FIELD("s", DataTypes.STRING()),
        DataTypes.FIELD("chars", DataTypes.STRING()));
    var rows = env.fromSequence(0, ROWS - 1)
        .map(id -> (RowData) GenericRowData.of(
            id,
            id % 11 == 0 ? null : BinaryStringData.fromBytes(bytes((int) (id % KEYS))),
            BinaryStringData.fromBytes(new byte[] {' ', 'x'})))
        .returns(InternalTypeInfo.of((RowType) type.getLogicalType()));
    table.createTemporaryView("src", rows, Schema.newBuilder().fromRowDataType(type).build());
    table.createTemporarySystemFunction("emit_bytes", EmitBytes.class);
    return table;
  }

  private static byte[] bytes(int key) {
    byte[] bytes = new byte[Math.max(WIDTH, 16)];
    Arrays.fill(bytes, (byte) 'x');
    byte[] prefix = Integer.toHexString(key).getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(prefix, 0, bytes, 1, prefix.length);
    if (key % 4 == 1) {
      bytes[0] = (byte) ' ';
      bytes[bytes.length - 1] = (byte) ' ';
    }
    if (key % 32 == 0) {
      // A complete malformed three-byte sequence also stays within Flink's byte traversal.
      bytes[bytes.length - 3] = (byte) 0xed;
      bytes[bytes.length - 2] = (byte) 0xa0;
      bytes[bytes.length - 1] = (byte) 0x80;
    }
    return bytes;
  }

  @FunctionHint(output = @DataTypeHint(value = "STRING", bridgedTo = StringData.class))
  public static final class EmitBytes extends ScalarFunction {
    public StringData eval(Long id) {
      return id == null || id % 11 == 0
          ? null
          : BinaryStringData.fromBytes(bytes((int) (id % KEYS)));
    }
  }
}

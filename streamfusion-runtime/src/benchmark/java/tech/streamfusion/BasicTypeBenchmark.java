package tech.streamfusion;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import tech.streamfusion.planner.NativePlanner;

/** Complete row-source/blackhole jobs; bridge and state costs stay inside the measured boundary. */
final class BasicTypeBenchmark {
  static void run(String name, String sql, boolean legacy, boolean twoPhase, String operator) throws Exception {
    run(name, sql, legacy, twoPhase, operator, null, null);
  }

  static void run(String name, String sql, boolean legacy, boolean twoPhase, String operator,
      org.apache.flink.table.types.DataType inputType,
      org.apache.flink.api.common.functions.MapFunction<Long, Row> inputRows) throws Exception {
    if (!"false".equals(System.getProperty("streamfusion.exchange.zeroCopyLocal"))) {
      throw new IllegalStateException("Use -Pbench to retain production Arrow IPC shuffle costs");
    }
    long rows = Long.getLong("basic.rows", 2_000_000L);
    int warmup = Integer.getInteger("basic.warmup", 2);
    int runs = Integer.getInteger("basic.runs", 5);
    boolean expectNative = Boolean.parseBoolean(System.getProperty("basic.expectNative", "true"));
    double[][] times = new double[2][runs];
    try (var metrics = new RecoveryExecutionMetrics()) {
      for (int trial = 0; trial < warmup + runs; trial++) {
        for (int turn = 0; turn < 2; turn++) {
          int engine = (trial + turn) % 2;
          var jobConfig = new Configuration();
          String backend = System.getProperty("basic.backend", "memory");
          if (backend.equals("rocksdb")) {
            jobConfig.setString("state.backend.type", engine == 1
                ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "rocksdb");
          } else if (!backend.equals("memory")) {
            throw new IllegalArgumentException("Unknown basic.backend " + backend);
          }
          var env = metrics.environment(jobConfig, 1, 0);
          var table = StreamTableEnvironment.create(env);
          table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
          table.getConfig().set("table.optimizer.agg-phase-strategy", twoPhase ? "TWO_PHASE" : "ONE_PHASE");
          table.getConfig().set("table.exec.mini-batch.enabled",
              Boolean.toString(twoPhase || Boolean.parseBoolean(System.getProperty("basic.miniBatch", "true"))));
          table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
          table.getConfig().set("table.exec.mini-batch.size", "1024");
          int keys = Integer.getInteger("basic.keys", 64);
          var type = inputType != null ? inputType : DataTypes.ROW(DataTypes.FIELD("k", DataTypes.INT()), DataTypes.FIELD("v", DataTypes.BIGINT()),
              DataTypes.FIELD("d", DataTypes.DECIMAL(7, 2)), DataTypes.FIELD("s", DataTypes.STRING()),
              DataTypes.FIELD("b", DataTypes.BOOLEAN()), DataTypes.FIELD("c", DataTypes.CHAR(2)));
          org.apache.flink.api.common.functions.MapFunction<Long, Row> mapper = inputRows != null ? inputRows : i -> {
            boolean missing = i % 7 == 0;
            return Row.of((int) (i % keys), missing ? null : i % 1000,
                missing ? null : BigDecimal.valueOf(i % 99999, 2),
                missing ? null : (i % 991) + ".25", missing ? null : i % 2 == 0,
                missing ? null : "ab");
          };
          var source = env.fromSequence(0, rows - 1).map(mapper).returns(ExternalTypeInfo.<Row>of(type));
          table.createTemporaryView("src", table.fromDataStream(source, Schema.newBuilder().fromRowDataType(type).build()));
          var scan = engine == 1 ? NativePlanner.install(table) : null;
          var query = table.sqlQuery(sql);
          table.createTemporaryTable("sink", TableDescriptor.forConnector("blackhole")
              .schema(Schema.newBuilder().fromResolvedSchema(query.getResolvedSchema()).build()).build());
          String insert = "INSERT INTO sink " + sql;
          String plan = table.explainSql(insert);
          if (engine == 1 && expectNative) {
            if (!plan.contains(operator) || !plan.contains("RowDataToArrow") || !plan.contains("ArrowToRowData")
                || (twoPhase && !plan.contains("NativeColumnarLocalGroupAggregate"))) {
              throw new IllegalStateException("Expected native execution and both transposes: " + plan);
            }
          } else if (engine == 1 && scan.substitutions() != 0) {
            throw new IllegalStateException("Expected previous behavior to fall back: " + plan);
          }
          long start = System.nanoTime();
          table.executeSql(insert).await();
          double elapsed = (System.nanoTime() - start) / 1e9;
          System.out.printf(Locale.ROOT,
              "[basic-type-trial] case=%s engine=%s trial=%d warmup=%s rows=%d keys=%d backend=%s seconds=%.6f%n",
              name, engine == 0 ? "Flink" : "StreamFusion", trial, trial < warmup, rows, keys, backend, elapsed);
          if (trial >= warmup) times[engine][trial - warmup] = elapsed;
        }
      }
      if (expectNative) {
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), operator);
        if (twoPhase) RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeColumnarLocalGroupAggregate");
      }
    }
    System.out.printf(Locale.ROOT,
        "[basic-type] case=%s rows=%d keys=%d native_expected=%s backend=%s Flink=%.6fs StreamFusion=%.6fs stock_trials=%s sf_trials=%s%n",
        name, rows, Integer.getInteger("basic.keys", 64), expectNative, System.getProperty("basic.backend", "memory"), median(times[0]), median(times[1]),
        Arrays.toString(times[0]), Arrays.toString(times[1]));
  }

  private static double median(double[] samples) {
    double[] sorted = samples.clone();
    Arrays.sort(sorted);
    int middle = sorted.length / 2;
    return sorted.length % 2 == 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
  }

  private BasicTypeBenchmark() {}
}

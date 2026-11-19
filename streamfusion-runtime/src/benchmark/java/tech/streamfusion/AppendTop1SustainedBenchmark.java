package tech.streamfusion;

import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

/** Fixed-window input rates at the rank operator, with the complete row-source/row-sink path. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class AppendTop1SustainedBenchmark {
  private static final String SQL = "SELECT k, v, s, rn FROM (SELECT k, v, s, "
      + "ROW_NUMBER() OVER (PARTITION BY k ORDER BY v DESC, s ASC) AS rn FROM src) WHERE rn <= 1";

  @Test
  void fixedWindowMeanRates() throws Exception {
    if (!"false".equals(System.getProperty("streamfusion.exchange.zeroCopyLocal"))) {
      throw new IllegalStateException("Use -Pbench to retain production Arrow IPC shuffle costs");
    }
    int keys = Integer.getInteger("top1.keys", 64);
    int width = Integer.getInteger("top1.width", 8);
    int warmup = Integer.getInteger("top1.warmupSeconds", 5);
    int seconds = Integer.getInteger("top1.seconds", 10);
    int runs = Integer.getInteger("top1.runs", 3);
    boolean improving = Boolean.parseBoolean(System.getProperty("top1.improving", "false"));
    String backend = System.getProperty("top1.backend", "rocksdb");
    if (keys <= 0 || width < 0 || warmup < 0 || seconds <= 0 || runs <= 0
        || !(backend.equals("memory") || backend.equals("rocksdb"))) {
      throw new IllegalArgumentException("Invalid Top-1 benchmark configuration");
    }
    double[][] rates = new double[2][runs];
    try (var metrics = new RecoveryExecutionMetrics()) {
      for (int trial = 0; trial < runs; trial++) {
        for (int turn = 0; turn < 2; turn++) {
          int engine = (trial + turn) % 2;
          var config = new Configuration();
          if (backend.equals("rocksdb")) {
            config.setString("state.backend.type", engine == 1
                ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "rocksdb");
          }
          var env = metrics.environment(config, 1, 0);
          var table = StreamTableEnvironment.create(env);
          table.getConfig().set("table.exec.mini-batch.enabled", "true");
          table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
          table.getConfig().set("table.exec.mini-batch.size", "1024");
          var type = DataTypes.ROW(DataTypes.FIELD("k", DataTypes.INT()),
              DataTypes.FIELD("v", DataTypes.BIGINT()), DataTypes.FIELD("s", DataTypes.STRING()));
          String payload = "x".repeat(width);
          var input = env.fromSequence(0, Long.MAX_VALUE - 1).map(i -> Row.of((int) (i % keys),
              i % 7 == 0 ? null : (improving ? i / keys : -(i / keys)),
              i % 11 == 0 ? null : payload)).returns(ExternalTypeInfo.<Row>of(type));
          table.createTemporaryView("src", table.fromDataStream(input,
              Schema.newBuilder().fromRowDataType(type).build()));
          if (engine == 1) NativePlanner.install(table);
          var query = table.sqlQuery(SQL);
          table.createTemporaryTable("sink", TableDescriptor.forConnector("blackhole")
              .schema(Schema.newBuilder().fromResolvedSchema(query.getResolvedSchema()).build()).build());
          String insert = "INSERT INTO sink " + SQL;
          String plan = table.explainSql(insert);
          if (engine == 1 && (!plan.contains("NativeColumnarTopN")
              || !plan.contains("RowDataToArrow") || !plan.contains("ArrowToRowData"))) {
            throw new IllegalStateException("Expected native rank and both transposes: " + plan);
          }
          var client = table.executeSql(insert).getJobClient().orElseThrow();
          String jobId = client.getJobID().toString();
          String operator = engine == 1 ? "NativeColumnarTopN" : "Rank";
          try {
            long deadline = System.nanoTime() + 60_000_000_000L;
            while (metrics.recordsIn(jobId, operator) == 0 && System.nanoTime() < deadline) {
              if (client.getJobStatus().get().isGloballyTerminalState()) {
                throw new IllegalStateException("Benchmark terminated before measurement");
              }
              Thread.sleep(100);
            }
            if (metrics.recordsIn(jobId, operator) == 0) {
              throw new IllegalStateException("No input at rank operator: " + plan);
            }
            Thread.sleep(warmup * 1000L);
            long before = metrics.recordsIn(jobId, operator);
            long start = System.nanoTime();
            Thread.sleep(seconds * 1000L);
            long after = metrics.recordsIn(jobId, operator);
            double elapsed = (System.nanoTime() - start) / 1e9;
            if (client.getJobStatus().get() != JobStatus.RUNNING || after <= before) {
              throw new IllegalStateException("Rank stopped consuming during measurement");
            }
            if (engine == 1) metrics.printTop1Cache(jobId);
            rates[engine][trial] = (after - before) / elapsed;
            System.out.printf(Locale.ROOT,
                "[append-top1-window] engine=%s trial=%d backend=%s keys=%d width=%d improving=%s "
                    + "before=%d after=%d seconds=%.6f mean_rows_per_second=%.3f%n",
                engine == 0 ? "Flink" : "StreamFusion", trial, backend, keys, width, improving,
                before, after, elapsed, rates[engine][trial]);
          } finally {
            client.cancel().get();
            try {
              client.getJobExecutionResult().get();
            } catch (ExecutionException cancelled) {
              if (client.getJobStatus().get() != JobStatus.CANCELED) throw cancelled;
            }
          }
        }
      }
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeColumnarTopN");
    }
    System.out.printf(Locale.ROOT,
        "[append-top1-sustained] backend=%s keys=%d width=%d improving=%s warmup_seconds=%d "
            + "window_seconds=%d stock_mean=%.3f sf_mean=%.3f stock_trials=%s sf_trials=%s%n",
        backend, keys, width, improving, warmup, seconds,
        Arrays.stream(rates[0]).average().orElseThrow(), Arrays.stream(rates[1]).average().orElseThrow(),
        Arrays.toString(rates[0]), Arrays.toString(rates[1]));
  }
}

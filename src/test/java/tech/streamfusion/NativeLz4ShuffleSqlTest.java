package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.parallel.Isolated
class NativeLz4ShuffleSqlTest {
  @Test
  void joinsRetainBothInputGatesInTheSameTaskCounters() throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.exchange.zeroCopyLocal", "false");
    config.setString("pipeline.operator-chaining.enabled", "false");
    config.setString("taskmanager.network.blocking-shuffle.compression.enabled", "true");
    config.setString("table.optimizer.join.broadcast-threshold", "-1");
    try (var metrics = RecoveryExecutionMetrics.countersOnly()) {
      metrics.setAsContext(config, 2, 128L << 20);
      var env = BatchSortSqlTest.environment(true, 65536, config);
      String sql = "SELECT COUNT(*) FROM input a JOIN input b ON a.id=b.id";
      assertTrue(env.explainSql(sql).contains("NativeJoin"));
      var result = env.executeSql(sql);
      String job = result.getJobClient().orElseThrow().getJobID().toString();
      assertEquals(List.of("+I[65536]"), BatchSortSqlTest.collect(result));
      long joinTasks =
          metrics.nativeOperators().stream()
              .filter(item -> job.equals(item.get("jobId")))
              .filter(item -> item.get("operator").toString().startsWith("NativeJoin"))
              .count();
      assertTrue(joinTasks > 0);
      assertEquals(2 * joinTasks, metrics.counterValue(job, "nativeLz4InputGates", "NativeJoin"));
      assertTrue(metrics.counterValue(job, "nativeLz4Bytes", "NativeJoin") > 0);
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @Test
  void limitInstallsItsDecoderBeforeReadingWithoutAllocatorInitialization() throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.exchange.zeroCopyLocal", "false");
    config.setString("pipeline.operator-chaining.enabled", "false");
    config.setString("taskmanager.network.blocking-shuffle.compression.enabled", "true");
    try (var metrics = RecoveryExecutionMetrics.countersOnly()) {
      metrics.setAsContext(config, 2, 128L << 20);
      var env = BatchSortSqlTest.environment(true, 65536, config);
      var result = env.executeSql("SELECT 'repeat' AS s FROM input LIMIT 20000");
      String job = result.getJobClient().orElseThrow().getJobID().toString();
      var rows = BatchSortSqlTest.collect(result);
      assertEquals(20000, rows.size());
      assertEquals(1, rows.stream().distinct().count());
      assertTrue(metrics.counterValue(job, "nativeLz4Bytes", "NativeLimit") > 0);
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @Test
  void nativeTasksDecodeStockShuffleBlocksWithoutChangingStockOrDisabledJobs() throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.exchange.zeroCopyLocal", "false");
    config.setString("pipeline.operator-chaining.enabled", "false");
    config.setString("taskmanager.network.blocking-shuffle.compression.enabled", "true");
    String sql =
        "SELECT MOD(id,31) AS k, COUNT(*) AS n, SUM(id) AS total FROM input"
            + " GROUP BY MOD(id,31)";
    String key = "streamfusion.shuffle.nativeLz4.enabled";
    String original = System.getProperty(key);
    try (var metrics = RecoveryExecutionMetrics.countersOnly()) {
      metrics.setAsContext(config, 2, 128L << 20);
      var stock = BatchSortSqlTest.environment(false, 65536, config);
      var expected = rows(stock.executeSql(sql));
      for (boolean enabled : new boolean[] {true, false, true}) {
        System.setProperty(key, Boolean.toString(enabled));
        var env = BatchSortSqlTest.environment(true, 65536, config);
        String plan = env.explainSql(sql);
        assertTrue(plan.contains("NativeAggregate"), plan);
        var result = env.executeSql(sql);
        String job = result.getJobClient().orElseThrow().getJobID().toString();
        assertEquals(expected, rows(result));
        assertEquals(0, metrics.counterValue(job, "nativeLz4FallbackGates"));
        System.out.printf(
            "[native-lz4] enabled=%s gates=%d blocks=%d bytes=%d%n",
            enabled,
            metrics.counterValue(job, "nativeLz4InputGates"),
            metrics.counterValue(job, "nativeLz4Blocks"),
            metrics.counterValue(job, "nativeLz4Bytes"));
        if (enabled) {
          assertTrue(metrics.counterValue(job, "nativeLz4InputGates") > 0);
          assertTrue(metrics.counterValue(job, "nativeLz4Blocks") > 0);
          assertTrue(metrics.counterValue(job, "nativeLz4Bytes") > 0);
        } else {
          assertEquals(0, metrics.counterValue(job, "nativeLz4InputGates"));
          assertEquals(0, metrics.counterValue(job, "nativeLz4Blocks"));
        }
      }
      var stockAgain = stock.executeSql(sql);
      String stockJob = stockAgain.getJobClient().orElseThrow().getJobID().toString();
      assertEquals(expected, rows(stockAgain));
      for (String metric : List.of("nativeLz4InputGates", "nativeLz4Blocks", "nativeLz4Bytes"))
        assertEquals(0, metrics.counterValue(stockJob, metric), metric);
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
      if (original == null) System.clearProperty(key);
      else System.setProperty(key, original);
    }
  }

  private static List<String> rows(org.apache.flink.table.api.TableResult result) throws Exception {
    var rows = BatchSortSqlTest.collect(result);
    rows.sort(String::compareTo);
    return rows;
  }
}

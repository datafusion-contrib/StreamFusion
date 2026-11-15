package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.functions.ScalarFunction;
import org.junit.jupiter.api.Test;

class BatchJoinRecoverySqlTest {
  public static final class FailOnce extends ScalarFunction {
    static final AtomicBoolean FAILED = new AtomicBoolean();

    public Long eval(Long value) {
      if (value != null && value == 1000 && FAILED.compareAndSet(false, true))
        throw new IllegalStateException("retry bounded join after both input partitions completed");
      return value;
    }
  }

  @Test
  void retriesFreshJoinStateFromBothCompletedArrowInputs() throws Exception {
    Configuration config = new Configuration();
    config.setString("restart-strategy.type", "fixed-delay");
    config.setString("restart-strategy.fixed-delay.attempts", "1");
    config.setString("restart-strategy.fixed-delay.delay", "1 ms");
    config.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
    config.setString("table.exec.resource.default-parallelism", "2");
    config.setString("table.optimizer.join.broadcast-threshold", "-1");
    config.setString("pipeline.operator-chaining.enabled", "true");
    BatchReplaySinkFactory.FAILED.set(true);
    FailOnce.FAILED.set(false);
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 128L << 20);
      var env =
          BatchExtremaSqlTest.environment(true, 2051, config, 3, BatchReplaySinkFactory.class);
      env.createTemporarySystemFunction("join_fail_once", FailOnce.class);
      env.executeSql("CREATE TABLE replay(n BIGINT) WITH ('connector'='batch-replay')");
      String sql =
          "INSERT INTO replay SELECT join_fail_once(l.id) FROM extrema l JOIN extrema r ON"
              + " l.id=r.id";
      String plan = env.explainSql(sql);
      assertTrue(plan.contains("NativeJoin") && plan.contains("NativeExchange"), plan);
      var result = env.executeSql(sql);
      result.await();
      var completed = result.getJobClient().orElseThrow().getJobExecutionResult().get();
      List<Long> actual = completed.getAccumulatorResult(BatchReplaySinkFactory.RESULTS);
      actual.sort(Long::compareTo);
      assertEquals(2051, actual.size());
      for (int index = 0; index < actual.size(); index++)
        assertEquals((long) index, actual.get(index));
      assertTrue(FailOnce.FAILED.get());
      assertTrue(
          metrics.attempts("NativeJoin").contains("1"), metrics.nativeOperators().toString());
      assertEquals(java.util.Set.of("0"), metrics.attempts("RowDataToArrow"));
      System.out.printf(
          "[batch-join-replay] sourceAttempts=%s joinAttempts=%s rows=%d%n",
          metrics.attempts("RowDataToArrow"), metrics.attempts("NativeJoin"), actual.size());
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }
}

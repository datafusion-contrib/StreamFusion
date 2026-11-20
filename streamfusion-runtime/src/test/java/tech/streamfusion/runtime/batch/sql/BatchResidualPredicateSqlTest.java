package tech.streamfusion.runtime.batch.sql;

import tech.streamfusion.RecoveryExecutionMetrics;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

class BatchResidualPredicateSqlTest {
  @Test
  void innerJoinKeepsJvmScalarPredicatesAboveTheNativeKeyMatch() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 2, 128L << 20);
      var stock = BatchSqlTestSupport.environment(false, 2051, new Configuration());
      var nativeEnv = BatchSqlTestSupport.environment(true, 2051, new Configuration());
      String sql =
          "SELECT l.id FROM input l JOIN input r ON l.id=r.id "
              + "AND POWER(CAST(l.id AS DOUBLE), 0.5) > CAST(r.id AS DOUBLE) / 10.0";
      String plan = nativeEnv.explainSql(sql);
      assertTrue(plan.contains("NativeJoin") && plan.contains("NativeCalc"), plan);
      var result = nativeEnv.executeSql(sql);
      String job = result.getJobClient().orElseThrow().getJobID().toString();
      var actual = new ArrayList<>(BatchSqlTestSupport.collect(result));
      var expected = new ArrayList<>(BatchSqlTestSupport.collect(stock.executeSql(sql)));
      actual.sort(String::compareTo);
      expected.sort(String::compareTo);
      assertEquals(expected, actual);
      assertTrue(metrics.recordsIn(job, "NativeJoin") > 0);
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }
}

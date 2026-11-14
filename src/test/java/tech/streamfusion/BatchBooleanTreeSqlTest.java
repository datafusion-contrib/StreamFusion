package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

class BatchBooleanTreeSqlTest {
  @Test
  void largePureDisjunctionsKeepNullableSubstringPredicatesWithinNativeDepth() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 2, 128L << 20);
      var stock = BatchSortSqlTest.environment(false, 2051, new Configuration());
      var nativeEnv = BatchSortSqlTest.environment(true, 2051, new Configuration());
      String predicate =
          IntStream.range(10, 75)
              .mapToObj(i -> "SUBSTRING(s,1,2)='" + i + "'")
              .collect(Collectors.joining(" OR "));
      String sql =
          "SELECT id FROM (SELECT id, CASE WHEN MOD(id,31)=0 THEN CAST(NULL AS STRING) "
              + "ELSE CAST(id AS STRING) END s FROM input) WHERE "
              + predicate
              + " ORDER BY id";
      String plan = nativeEnv.explainSql(sql);
      assertTrue(plan.contains("NativeCalc") && plan.contains("NativeSort"), plan);
      var result = nativeEnv.executeSql(sql);
      String job = result.getJobClient().orElseThrow().getJobID().toString();
      assertEquals(
          BatchSortSqlTest.collect(stock.executeSql(sql)), BatchSortSqlTest.collect(result));
      assertTrue(metrics.recordsIn(job, "NativeCalc") > 0);
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }
}

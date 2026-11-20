package tech.streamfusion.runtime.batch.sql;

import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.RecoveryExecutionMetrics;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BatchNullSafePredicateSqlTest {
  @TempDir Path directory;

  @Test
  void nullSafeReferenceComparisonsRetainBothNullAndOneNullCases() throws Exception {
    var config = new Configuration();
    config.setString("parallelism.default", "2");
    config.setString("table.exec.resource.default-parallelism", "2");
    config.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 128L << 20);
      config.setString("streamfusion.native.enabled", "false");
      var stock = NativePlannerTestEnvironment.createBatch(config);
      config.setString("streamfusion.native.enabled", "true");
      var nativeEnv = NativePlannerTestEnvironment.createBatch(config);
      int index = 0;
      for (var fixture :
          List.of(
              List.of("BIGINT", "1", "-2"),
              List.of("BOOLEAN", "true", "false"),
              List.of("DECIMAL(20,3)", "1234567890123.125", "-0.001"),
              List.of("DATE", "1969-12-31", "2000-02-29"),
              List.of("STRING", "😀", "\ue000"))) {
        String a = fixture.get(1), b = fixture.get(2), table = "input" + index++;
        Path file = directory.resolve(table + ".csv");
        Files.writeString(
            file,
            String.join("\n", a + "," + a, a + "," + b, "NULL," + a, b + ",NULL", "NULL,NULL")
                + "\n");
        String ddl =
            "CREATE TABLE "
                + table
                + " (a "
                + fixture.get(0)
                + ",b "
                + fixture.get(0)
                + ") WITH ('connector'='filesystem','path'='"
                + file.toUri()
                + "','format'='csv','csv.null-literal'='NULL')";
        stock.executeSql(ddl);
        nativeEnv.executeSql(ddl);
        String sql =
            "SELECT a,b,a IS NOT DISTINCT FROM b,a IS DISTINCT FROM b FROM "
                + table
                + " ORDER BY a,b";
        String plan = nativeEnv.explainSql(sql);
        assertTrue(plan.contains("NativeCalc") && plan.contains("NativeSort"), plan);
        var result = nativeEnv.executeSql(sql);
        String job = result.getJobClient().orElseThrow().getJobID().toString();
        assertEquals(
            BatchSqlTestSupport.collect(stock.executeSql(sql)),
            BatchSqlTestSupport.collect(result),
            fixture.get(0));
        assertTrue(metrics.recordsIn(job, "NativeCalc") > 0);
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }
}

package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BatchDateComparisonSqlTest {
  @TempDir Path directory;

  @Test
  void dateLeafComparisonsUseNativeDaysAndPreserveNullsAndCalendarBoundaries() throws Exception {
    var dates =
        List.of(
            "NULL",
            "0001-01-01",
            "1582-10-04",
            "1582-10-15",
            "1900-02-28",
            "1900-03-01",
            "1969-12-31",
            "1970-01-01",
            "2000-02-29",
            "9999-12-31");
    var lines = new ArrayList<String>();
    for (int repeat = 0; repeat < 13; repeat++) {
      for (String left : dates) {
        for (String right : dates) lines.add(lines.size() + "," + left + "," + right);
      }
    }
    var file = directory.resolve("dates.csv");
    Files.write(file, lines);
    var config = new Configuration();
    config.setString("streamfusion.transpose.batchRows", "32");
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 128L << 20);
      var stock = BatchSortSqlTest.environment(false, 1, config);
      var nativeEnv = BatchSortSqlTest.environment(true, 1, config);
      String ddl =
          "CREATE TABLE dates(id BIGINT,d1 DATE,d2 DATE) WITH ('connector'='filesystem',"
              + "'path'='"
              + file.toUri()
              + "','format'='csv','csv.null-literal'='NULL')";
      stock.executeSql(ddl);
      nativeEnv.executeSql(ddl);
      for (String sql :
          List.of(
              "SELECT id,d1=d2,d1<>d2,d1<d2,d1<=d2,d1>d2,d1>=d2,"
                  + "d1<DATE '1970-01-01',DATE '1970-01-01'>=d2 FROM dates ORDER BY id",
              "SELECT id FROM dates WHERE d1>=DATE '1582-10-04' AND d1<DATE '2000-03-01'"
                  + " AND d2<>DATE '1970-01-01' ORDER BY id")) {
        String plan = nativeEnv.explainSql(sql);
        assertTrue(plan.contains("NativeCalc"), plan);
        assertFalse(plan.contains("jvmExpressions"), plan);
        var result = nativeEnv.executeSql(sql);
        String job = result.getJobClient().orElseThrow().getJobID().toString();
        assertEquals(
            BatchSortSqlTest.collect(stock.executeSql(sql)), BatchSortSqlTest.collect(result));
        assertTrue(metrics.recordsIn(job, "NativeCalc") >= lines.size());
      }
      String mixed = "SELECT id,CAST(d1 AS TIMESTAMP(3))>=CAST(d2 AS TIMESTAMP(3))"
          + " FROM dates ORDER BY id";
      String mixedPlan = nativeEnv.explainSql(mixed);
      assertTrue(mixedPlan.contains("jvmExpressions"), mixedPlan);
      assertTrue(mixedPlan.contains("jvmExpressionDetails"), mixedPlan);
      assertEquals(BatchSortSqlTest.collect(stock.executeSql(mixed)),
          BatchSortSqlTest.collect(nativeEnv.executeSql(mixed)));
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }
}

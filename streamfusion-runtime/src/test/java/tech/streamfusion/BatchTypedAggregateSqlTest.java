package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

class BatchTypedAggregateSqlTest {
  @Test
  void optimizerSharedDecimalSumAndAverageKeepStickyOverflowAndNullGroups() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 2, 128L << 20);
      for (String phase : List.of("ONE_PHASE", "TWO_PHASE", "ONE_PHASE_SORT", "TWO_PHASE_SORT")) {
        Configuration config = new Configuration();
        config.setString("table.optimizer.agg-phase-strategy", phase.replace("_SORT", ""));
        if (phase.endsWith("_SORT")) config.setString("table.exec.disabled-operators", "HashAgg");
        var stock = BatchSortSqlTest.environment(false, 9, config);
        var nativeEnv = BatchSortSqlTest.environment(true, 9, config);
        String sql =
            "SELECT k,SUM(v),AVG(v),COUNT(v) FROM (SELECT MOD(id,3) AS k,"
                + " CASE WHEN MOD(id,3)=1 THEN CAST(NULL AS DECIMAL(38,2))"
                + " WHEN id<3 THEN CAST('999999999999999999999999999999999999.99' AS DECIMAL(38,2))"
                + " ELSE CAST(1 AS DECIMAL(38,2)) END AS v FROM input) t GROUP BY k ORDER BY k";
        String plan = nativeEnv.explainSql(sql);
        assertTrue(plan.contains("NativeAggregate"), plan);
        var result = nativeEnv.executeSql(sql);
        String job = result.getJobClient().orElseThrow().getJobID().toString();
        assertEquals(
            BatchSortSqlTest.collect(stock.executeSql(sql)), BatchSortSqlTest.collect(result));
        assertTrue(metrics.recordsIn(job, "NativeAggregate") >= 9);
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @Test
  void decimalAveragesAndTypedKeysMatchStockAcrossLocalFinalArrowStages() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 2, 128L << 20);
      for (String phase : List.of("ONE_PHASE", "TWO_PHASE", "ONE_PHASE_SORT", "TWO_PHASE_SORT")) {
        Configuration config = new Configuration();
        config.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
        config.setString("table.exec.resource.default-parallelism", "2");
        config.setString("table.optimizer.agg-phase-strategy", phase.replace("_SORT", ""));
        if (phase.endsWith("_SORT")) config.setString("table.exec.disabled-operators", "HashAgg");
        var stock = BatchSortSqlTest.environment(false, 2051, config);
        var nativeEnv = BatchSortSqlTest.environment(true, 2051, config);
        String input =
            " FROM (SELECT CASE MOD(id,5) WHEN 0 THEN CAST(NULL AS STRING)"
                + " WHEN 1 THEN '' WHEN 2 THEN 'λ' WHEN 3 THEN '😀' ELSE '' END AS k,"
                + " CASE WHEN MOD(id,13)=0 THEN CAST(NULL AS DECIMAL(15,2))"
                + " ELSE CAST(id-1000 AS DECIMAL(15,2))/CAST(3 AS DECIMAL(2,0)) END AS v,"
                + " CASE WHEN MOD(id,7)=0 THEN CAST(NULL AS DATE)"
                + " WHEN MOD(id,2)=0 THEN DATE '1969-12-31' ELSE DATE '2026-11-09' END AS d,"
                + " CASE WHEN MOD(id,11)=0 THEN CAST(NULL AS BOOLEAN) ELSE MOD(id,2)=0 END AS b"
                + " FROM input) t";
        for (String keys : List.of("k", "d,b", "CAST(v AS DECIMAL(8,0))")) {
          String sql =
              "SELECT "
                  + keys
                  + ",SUM(v),AVG(v),COUNT(v),COUNT(*)"
                  + input
                  + " GROUP BY "
                  + keys
                  + " ORDER BY "
                  + keys;
          String plan = nativeEnv.explainSql(sql);
          assertTrue(plan.contains("NativeAggregate") && plan.contains("NativeSort"), plan);
          var result = nativeEnv.executeSql(sql);
          String job = result.getJobClient().orElseThrow().getJobID().toString();
          var actual = BatchSortSqlTest.collect(result);
          assertEquals(BatchSortSqlTest.collect(stock.executeSql(sql)), actual, phase + keys);
          assertTrue(metrics.recordsIn(job, "NativeAggregate") >= 2051);
        }
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }
}

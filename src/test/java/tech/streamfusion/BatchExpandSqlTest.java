package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

class BatchExpandSqlTest {
  @Test
  void groupingSetsKeepNullsDuplicatesFiltersAndGroupingIdentifiers() throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.transpose.batchRows", "1024");
    config.setString("pipeline.operator-chaining.enabled", "false");
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 128L << 20);
      var stock = BatchSortSqlTest.environment(false, 4099, config);
      var nativeEnv = BatchSortSqlTest.environment(true, 4099, config);
      for (String filter : List.of("", " WHERE id < 0")) {
        String sql =
            "SELECT k, s, GROUPING(k), GROUPING(s), COUNT(*), SUM(id) FROM (SELECT id, CASE WHEN"
                + " MOD(id,17)=0 THEN CAST(NULL AS INT) ELSE CAST(MOD(id,7) AS INT) END k, CASE"
                + " WHEN MOD(id,11)=0 THEN CAST(NULL AS STRING) ELSE CAST(MOD(id,3) AS STRING) END"
                + " s FROM input"
                + filter
                + ") GROUP BY GROUPING SETS ((k,s),(k),(s),())";
        String plan = nativeEnv.explainSql(sql);
        assertTrue(plan.contains("NativeExpand"), plan);
        var result = nativeEnv.executeSql(sql);
        String job = result.getJobClient().orElseThrow().getJobID().toString();
        var actual = new ArrayList<>(BatchSortSqlTest.collect(result));
        var expected = new ArrayList<>(BatchSortSqlTest.collect(stock.executeSql(sql)));
        actual.sort(String::compareTo);
        expected.sort(String::compareTo);
        assertEquals(expected, actual);
        if (filter.isEmpty()) assertTrue(metrics.recordsIn(job, "NativeExpand") > 0);
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }
}

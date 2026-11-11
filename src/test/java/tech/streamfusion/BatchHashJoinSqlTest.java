package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

class BatchHashJoinSqlTest {
  @Test
  void boundedHashAndBroadcastJoinsKeepNullsDuplicatesAndOuterRows() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 2, 128L << 20);
      Configuration config = new Configuration();
      config.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
      config.setString("table.exec.resource.default-parallelism", "2");
      config.setString("pipeline.operator-chaining.enabled", "false");
      var stock = BatchSortSqlTest.environment(false, 1037, config);
      var nativeEnv = BatchSortSqlTest.environment(true, 1037, config);
      for (var env : List.of(stock, nativeEnv)) {
        env.executeSql(
            "CREATE VIEW l AS SELECT CASE WHEN MOD(id,17)=0 THEN CAST(NULL AS INT)"
                + " ELSE CAST(MOD(id,31) AS INT) END AS k,id AS v FROM input WHERE id<521");
        env.executeSql(
            "CREATE VIEW r AS SELECT CASE WHEN MOD(id,13)=0 THEN CAST(NULL AS INT)"
                + " ELSE CAST(MOD(id,37) AS INT) END AS k,id AS v FROM input WHERE id<307");
      }
      for (String mode : List.of("hash", "broadcast")) {
        for (var env : List.of(stock, nativeEnv))
          env.getConfig()
              .set(
                  "table.optimizer.join.broadcast-threshold",
                  mode.equals("hash") ? "-1" : "10485760");
        for (String kind : List.of("INNER", "LEFT", "RIGHT", "FULL")) {
          String sql =
              "SELECT "
                  + (mode.equals("broadcast") && !kind.equals("FULL") ? "/*+ BROADCAST(r) */ " : "")
                  + "l.k,l.v,r.k,r.v FROM l "
                  + kind
                  + " JOIN r ON l.k=r.k ORDER BY l.v,r.v";
          System.out.println("[batch-join-check] " + mode + kind);
          String plan = nativeEnv.explainSql(sql);
          assertTrue(
              plan.contains("NativeJoin"),
              tech.streamfusion.planner.NativePlanner.explain(nativeEnv, sql));
          var result = nativeEnv.executeSql(sql);
          String job = result.getJobClient().orElseThrow().getJobID().toString();
          var actual = BatchSortSqlTest.collect(result);
          assertEquals(BatchSortSqlTest.collect(stock.executeSql(sql)), actual, mode + kind);
          assertTrue(metrics.recordsIn(job, "NativeJoin") > 0);
        }
      }
      for (String sql :
          List.of(
              "SELECT l.k,l.v,r.k,r.v FROM l FULL JOIN r ON l.k IS NOT DISTINCT FROM r.k ORDER BY"
                  + " l.v,r.v",
              "SELECT l.k,l.v,r.k,r.v FROM l LEFT JOIN (SELECT * FROM r WHERE v<0) r ON l.k=r.k"
                  + " ORDER BY l.v,r.v",
              "SELECT l.k,l.v FROM l WHERE EXISTS (SELECT * FROM r WHERE l.k=r.k) ORDER BY l.v",
              "SELECT l.k,l.v FROM l WHERE NOT EXISTS (SELECT * FROM r WHERE l.k=r.k) ORDER BY l.v",
              "SELECT l.k,l.v FROM l WHERE NOT EXISTS (SELECT * FROM r WHERE v<0 AND l.k=r.k) ORDER"
                  + " BY l.v")) {
        String plan = nativeEnv.explainSql(sql);
        assertTrue(
            plan.contains("NativeJoin"),
            tech.streamfusion.planner.NativePlanner.explain(nativeEnv, sql));
        var result = nativeEnv.executeSql(sql);
        String job = result.getJobClient().orElseThrow().getJobID().toString();
        var actual = BatchSortSqlTest.collect(result);
        assertEquals(BatchSortSqlTest.collect(stock.executeSql(sql)), actual, sql);
        assertTrue(metrics.recordsIn(job, "NativeJoin") > 0);
      }

      for (String algorithm : List.of("hash", "nested", "merge")) {
        for (var env : List.of(stock, nativeEnv))
          env.getConfig()
              .set(
                  "table.exec.disabled-operators",
                  algorithm.equals("nested")
                      ? "HashJoin,SortMergeJoin"
                      : algorithm.equals("merge")
                          ? "HashJoin,NestedLoopJoin"
                          : "NestedLoopJoin,SortMergeJoin");
        for (String kind : List.of("INNER", "LEFT", "RIGHT", "FULL")) {
          String sql =
              "SELECT l.k,l.v,r.k,r.v FROM l "
                  + kind
                  + " JOIN r ON l.k=r.k AND l.v<r.v ORDER BY l.v,r.v";
          String plan = nativeEnv.explainSql(sql);
          assertTrue(
              plan.contains("NativeJoin"),
              tech.streamfusion.planner.NativePlanner.explain(nativeEnv, sql));
          var result = nativeEnv.executeSql(sql);
          String job = result.getJobClient().orElseThrow().getJobID().toString();
          var actual = BatchSortSqlTest.collect(result);
          assertEquals(BatchSortSqlTest.collect(stock.executeSql(sql)), actual, algorithm + kind);
          assertTrue(metrics.recordsIn(job, "NativeJoin") > 0);
        }
      }

    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }
}

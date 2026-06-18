package io.github.jordepic.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

class NativePlannerTest {

  @Test
  void hookScansOptimizedPhysicalPlan() throws Exception {
    TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    PhysicalPlanScan scan = NativePlanner.install(tEnv);

    TableResult result =
        tEnv.executeSql("SELECT c0 * 2 AS doubled FROM (VALUES (3), (4), (5)) AS t(c0)");
    try (CloseableIterator<Row> rows = result.collect()) {
      while (rows.hasNext()) {
        rows.next();
      }
    }

    assertFalse(scan.operatorTypes().isEmpty(), "hook never ran");
    assertTrue(
        scan.operatorTypes().stream().anyMatch(name -> name.contains("Calc")),
        "expected a projection node in the plan, saw: " + scan.operatorTypes());
  }
}

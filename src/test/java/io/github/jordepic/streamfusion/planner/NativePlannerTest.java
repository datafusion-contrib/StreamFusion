package io.github.jordepic.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
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

  @Test
  void substitutesNativeOperatorForDoublingProjection() throws Exception {
    TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    PhysicalPlanScan scan = NativePlanner.install(tEnv);

    List<Integer> result =
        collectInts(tEnv, "SELECT c0 * 2 AS doubled FROM (VALUES (3), (4), (5)) AS t(c0)");

    assertTrue(scan.substitutions() > 0, "native operator was not substituted in");
    assertEquals(List.of(6, 8, 10), result);
  }

  @Test
  void leavesUnsupportedProjectionToHostEngine() throws Exception {
    TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    PhysicalPlanScan scan = NativePlanner.install(tEnv);

    List<Integer> result =
        collectInts(tEnv, "SELECT c0 * 3 AS tripled FROM (VALUES (3), (4), (5)) AS t(c0)");

    assertEquals(0, scan.substitutions(), "non-matching projection should not be substituted");
    assertEquals(List.of(9, 12, 15), result);
  }

  private static List<Integer> collectInts(TableEnvironment tEnv, String sql) throws Exception {
    TableResult result = tEnv.executeSql(sql);
    List<Integer> values = new ArrayList<>();
    try (CloseableIterator<Row> rows = result.collect()) {
      while (rows.hasNext()) {
        values.add((Integer) rows.next().getField(0));
      }
    }
    values.sort(null);
    return values;
  }
}

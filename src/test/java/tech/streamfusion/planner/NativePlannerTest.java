package tech.streamfusion.planner;

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

    // ABS is not an admitted expression op, so the whole projection falls back to the host.
    List<Integer> result =
        collectInts(tEnv, "SELECT ABS(c0) AS a FROM (VALUES (3), (4), (5)) AS t(c0)");

    assertEquals(0, scan.substitutions(), "an unsupported projection should not be substituted");
    assertEquals(List.of(3, 4, 5), result);
  }

  /**
   * A self-join reads the same table twice. Sub-plan reuse stays enabled under the native planner,
   * scoped by digest barriers: the rowwise prefix (the scan) merges — the plan shows a {@code
   * Reused} reference — while each branch keeps its own native island (two entry transposes), so no
   * Arrow batch ever fans out to two consumers.
   */
  @Test
  void reusesRowwisePrefixButNeverNativeNodes() {
    TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    createSequenceTable(tEnv);
    String explain =
        NativePlanner.explain(
            tEnv, "SELECT a.k, a.v + b.v FROM src a JOIN src b ON a.k = b.k");
    assertTrue(
        explain.contains("Reused(reference_id="),
        "the shared rowwise scan should be merged by sub-plan reuse:\n" + explain);
    int transposes = countOccurrences(explain, "RowDataToArrow");
    assertTrue(
        transposes >= 2,
        "each branch must keep its own entry transpose (no shared Arrow edge), saw "
            + transposes
            + ":\n"
            + explain);
  }

  /**
   * The mini-batch assigner is a pass-through that must not hide a rowwise input from the calc's
   * projection pruning: with mini-batch on, the entry transpose below the assigner still converts
   * only the read columns (here 2 of 3 — the wide unread string never becomes an Arrow column).
   * Regression test for the unpruned-transpose tax measured at 7x on Nexmark q3.
   */
  @Test
  void prunesEntryTransposeThroughMiniBatchAssigner() {
    TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    tEnv.getConfig().set("table.exec.mini-batch.enabled", "true");
    tEnv.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
    tEnv.getConfig().set("table.exec.mini-batch.size", "100");
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    tEnv.executeSql(
        "CREATE TABLE wide (k BIGINT, v BIGINT, unread STRING) WITH ('connector' = 'datagen', "
            + "'number-of-rows' = '10')");
    String explain = NativePlanner.explain(tEnv, "SELECT k, SUM(v) FROM wide GROUP BY k");
    assertTrue(
        explain.contains("NativeMiniBatchAssigner"),
        "the mini-batch assigner should be native:\n" + explain);
    assertTrue(
        explain.contains("RowDataToArrow(fields=[2]"),
        "the entry transpose should be pruned to the 2 read columns:\n" + explain);
  }

  private static void createSequenceTable(TableEnvironment tEnv) {
    tEnv.executeSql(
        "CREATE TABLE src (k BIGINT, v BIGINT) WITH ('connector' = 'datagen', "
            + "'number-of-rows' = '100', "
            + "'fields.k.kind' = 'sequence', 'fields.k.start' = '0', 'fields.k.end' = '99', "
            + "'fields.v.kind' = 'sequence', 'fields.v.start' = '100', 'fields.v.end' = '199')");
  }

  private static int countOccurrences(String haystack, String needle) {
    int count = 0;
    for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + 1)) {
      count++;
    }
    return count;
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

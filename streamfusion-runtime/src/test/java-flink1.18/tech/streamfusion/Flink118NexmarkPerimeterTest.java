package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.junit.jupiter.api.Test;
import tech.streamfusion.planner.NativePlanner;

/** Verifies the existing benchmark plans without changing its queries or source fixtures. */
class Flink118NexmarkPerimeterTest {
  @Test
  void generatorComparisonsRetainBothTransposes() throws Exception {
    Set<String> selected = Set.of("q0", "q1", "q2", "q3", "q5", "q7", "q8", "q19", "q20", "q21");
    var field = NexmarkMatrixBenchmark.class.getDeclaredField("ALL_QUERIES");
    field.setAccessible(true);
    int verified = 0;
    for (Object query : (Object[]) field.get(null)) {
      String label = (String) value(query, "label");
      if (!selected.contains(label)) continue;
      var environment = NexmarkBenchmark.environment(2_000_000);
      environment.createTemporarySystemFunction(
          "count_char", NexmarkMatrixBenchmark.CountChar.class);
      String[] setup = (String[]) value(query, "setup");
      if (setup != null) {
        for (String statement : setup) environment.executeSql(statement);
      }
      environment.executeSql(
          ((String) value(query, "sinkDdl"))
              .replace("%TS%", "TIMESTAMP(3)")
              .replace("%WTS%", "TIMESTAMP(3)"));
      var scan = NativePlanner.install(environment);
      String plan = environment.explainSql((String) value(query, "insertSql"));
      assertTrue(scan.substitutions() > 0, label + ": " + scan.explainSummary());
      assertTrue(plan.contains("RowDataToArrow"), label + ": " + plan);
      assertTrue(plan.contains("ArrowToRowData"), label + ": " + plan);
      verified++;
    }
    org.junit.jupiter.api.Assertions.assertEquals(selected.size(), verified);
  }

  private static Object value(Object query, String name) throws Exception {
    var field = query.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(query);
  }
}

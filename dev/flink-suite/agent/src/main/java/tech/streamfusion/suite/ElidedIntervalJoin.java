package tech.streamfusion.suite;

import java.util.List;
import java.util.Map;

/** Submitted-graph proof for the pinned impossible-interval LEFT join program. */
final class ElidedIntervalJoin {
  static final String CONTRACT = "@elided:IntervalJoin";
  static final String TEST =
      "org.apache.flink.table.planner.plan.nodes.exec.stream.IntervalJoinRestoreTest#testRestore";
  static final String VARIANT = "program=interval_join_negative_interval";

  private ElidedIntervalJoin() {}

  static boolean valid(Map<String, Object> graph) {
    if (!ElidedTemporalSort.valid(graph)) return false;
    for (Object value : (List<?>) graph.get("nodes")) {
      String operator = ((Map<?, ?>) value).get("operator").toString();
      if (operator.contains("Join") || operator.contains("CoProcess")) return false;
    }
    return true;
  }
}

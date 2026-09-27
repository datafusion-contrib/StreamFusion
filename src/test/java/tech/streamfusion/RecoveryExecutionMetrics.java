package tech.streamfusion;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.util.TestStreamEnvironment;

/** Keep removed attempt metrics until the scenario has recorded execution and cleanup evidence. */
final class RecoveryExecutionMetrics implements AutoCloseable {
  private final InMemoryReporter reporter = InMemoryReporter.createWithRetainedMetrics();
  private MiniClusterResource cluster;

  TestStreamEnvironment environment(Configuration jobConfig, int parallelism, long budget) {
    if (cluster == null) {
      Configuration config = reporter.addToConfiguration(new Configuration(jobConfig));
      config.set(
          TaskManagerOptions.TASK_OFF_HEAP_MEMORY,
          budget > 0 ? new MemorySize(budget) : MemorySize.parse("48g"));
      cluster =
          new MiniClusterResource(
              new MiniClusterResourceConfiguration.Builder()
                  .setConfiguration(config)
                  .setNumberTaskManagers(1)
                  .setNumberSlotsPerTaskManager(8)
                  .build());
      try {
        cluster.before();
      } catch (Exception failure) {
        throw new IllegalStateException("cannot start recovery metrics cluster", failure);
      }
    }
    return new TestStreamEnvironment(
        cluster.getMiniCluster(), jobConfig, parallelism, List.of(), List.of());
  }

  List<Map<String, Object>> nativeOperators() {
    List<Map<String, Object>> observations = new ArrayList<>();
    reporter
        .getMetricsByGroup()
        .forEach(
            (group, metrics) -> {
              if (!(group instanceof org.apache.flink.metrics.groups.OperatorMetricGroup)) return;
              var variables = group.getAllVariables();
              String operator = variables.getOrDefault("<operator_name>", "");
              if (!operator.startsWith("Native")) return;
              if (!(metrics.get("numRecordsIn") instanceof Counter input)
                  || !(metrics.get("numRecordsOut") instanceof Counter output)) return;
              observations.add(
                  Map.of(
                      "operator",
                      operator,
                      "jobId",
                      variables.getOrDefault("<job_id>", ""),
                      "subtask",
                      variables.getOrDefault("<subtask_index>", ""),
                      "attempt",
                      variables.getOrDefault("<task_attempt_num>", ""),
                      "recordsIn",
                      input.getCount(),
                      "recordsOut",
                      output.getCount()));
            });
    observations.sort(java.util.Comparator.comparing(Object::toString));
    return observations;
  }

  static void assertConsumed(List<Map<String, Object>> observations, String planOperator) {
    String metricOperator = planOperator.replace("Aggregate", "Agg") + "ExecNode";
    long consumed =
        observations.stream()
            .filter(row -> row.get("operator").toString().startsWith(metricOperator))
            .mapToLong(row -> ((Number) row.get("recordsIn")).longValue())
            .sum();
    org.junit.jupiter.api.Assertions.assertTrue(
        consumed > 0, () -> "no consumed rows for " + metricOperator + ": " + observations);
  }

  @Override
  public void close() {
    try {
      if (cluster != null) cluster.after();
    } finally {
      reporter.close();
    }
  }
}

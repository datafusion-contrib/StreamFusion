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
  private final InMemoryReporter reporter;
  private MiniClusterResource cluster;

  RecoveryExecutionMetrics() {
    this(InMemoryReporter.createWithRetainedMetrics());
  }

  private RecoveryExecutionMetrics(InMemoryReporter reporter) {
    this.reporter = reporter;
  }

  static RecoveryExecutionMetrics countersOnly() {
    return new RecoveryExecutionMetrics(
        new org.apache.flink.runtime.testutils.RetainedCounterReporter());
  }

  void setAsContext(Configuration config, int parallelism, long budget) {
    environment(config, parallelism, budget);
    tech.streamfusion.compat.FlinkTestContexts.setForLocalTests(
        cluster.getMiniCluster(), parallelism);
  }

  TestStreamEnvironment environment(Configuration jobConfig, int parallelism, long budget) {
    if (cluster == null) {
      long started = CiFixtureTimings.start();
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
      } finally {
        CiFixtureTimings.record("recovery-metrics.cluster-start", started);
      }
    }
    return new TestStreamEnvironment(
        cluster.getMiniCluster(), jobConfig, parallelism, List.of(), List.of());
  }

  void printTop1Cache(String jobId) {
    reporter
        .getMetricsByGroup()
        .forEach(
            (group, metrics) -> {
              if (!jobId.equals(group.getAllVariables().get("<job_id>"))) return;
              if (!group
                  .getAllVariables()
                  .getOrDefault("<operator_name>", "")
                  .startsWith("NativeColumnarTopN")) return;
              metrics.forEach(
                  (name, metric) -> {
                    if (name.startsWith("topn.cache.")
                        && metric instanceof org.apache.flink.metrics.Gauge) {
                      System.out.println(
                          "[append-top1-cache] job="
                              + jobId
                              + " metric="
                              + name
                              + " value="
                              + ((org.apache.flink.metrics.Gauge<?>) metric).getValue());
                    }
                  });
            });
  }

  long recordsIn(String jobId, String operatorPrefix) {
    long[] total = {0};
    reporter
        .getMetricsByGroup()
        .forEach(
            (group, metrics) -> {
              if (!(group instanceof org.apache.flink.metrics.groups.OperatorMetricGroup)) return;
              var variables = group.getAllVariables();
              if (jobId.equals(variables.get("<job_id>"))
                  && variables.getOrDefault("<operator_name>", "").startsWith(operatorPrefix)
                  && metrics.get("numRecordsIn") instanceof Counter) {
                total[0] += ((Counter) metrics.get("numRecordsIn")).getCount();
              }
            });
    return total[0];
  }

  List<String> taskNames(String operatorPrefix) {
    return reporter.getMetricsByGroup().keySet().stream()
        .filter(group -> group instanceof org.apache.flink.metrics.groups.OperatorMetricGroup)
        .map(org.apache.flink.metrics.MetricGroup::getAllVariables)
        .filter(
            variables -> variables.getOrDefault("<operator_name>", "").startsWith(operatorPrefix))
        .map(variables -> variables.getOrDefault("<task_name>", ""))
        .distinct()
        .collect(java.util.stream.Collectors.toList());
  }

  java.util.Set<String> subtasks(String operatorPrefix) {
    return reporter.getMetricsByGroup().keySet().stream()
        .filter(group -> group instanceof org.apache.flink.metrics.groups.OperatorMetricGroup)
        .map(org.apache.flink.metrics.MetricGroup::getAllVariables)
        .filter(
            variables -> variables.getOrDefault("<operator_name>", "").startsWith(operatorPrefix))
        .map(variables -> variables.getOrDefault("<subtask_index>", ""))
        .collect(java.util.stream.Collectors.toSet());
  }

  java.util.Set<String> attempts(String operatorPrefix) {
    return reporter.getMetricsByGroup().keySet().stream()
        .filter(group -> group instanceof org.apache.flink.metrics.groups.OperatorMetricGroup)
        .map(org.apache.flink.metrics.MetricGroup::getAllVariables)
        .filter(
            variables -> variables.getOrDefault("<operator_name>", "").startsWith(operatorPrefix))
        .map(variables -> variables.getOrDefault("<task_attempt_num>", ""))
        .collect(java.util.stream.Collectors.toSet());
  }

  long counterValue(String name) {
    long[] total = {0};
    reporter
        .getMetricsByGroup()
        .forEach(
            (group, metrics) -> {
              if (metrics.get(name) instanceof Counter)
                total[0] += ((Counter) metrics.get(name)).getCount();
            });
    return total[0];
  }

  long counterValue(String jobId, String name) {
    return counterValue(jobId, name, "");
  }

  long counterValue(String jobId, String name, String operatorPrefix) {
    long[] total = {0};
    reporter
        .getMetricsByGroup()
        .forEach(
            (group, metrics) -> {
              if (jobId.equals(group.getAllVariables().get("<job_id>"))
                  && group
                      .getAllVariables()
                      .getOrDefault("<operator_name>", "")
                      .startsWith(operatorPrefix)
                  && metrics.get(name) instanceof Counter)
                total[0] += ((Counter) metrics.get(name)).getCount();
            });
    return total[0];
  }

  Map<String, Long> countersBySubtask(String jobId, String name) {
    Map<String, Long> counts = new java.util.TreeMap<>();
    reporter
        .getMetricsByGroup()
        .forEach(
            (group, metrics) -> {
              var variables = group.getAllVariables();
              if (jobId.equals(variables.get("<job_id>")) && metrics.get(name) instanceof Counter)
                counts.merge(
                    variables.getOrDefault("<subtask_index>", ""),
                    ((Counter) metrics.get(name)).getCount(),
                    Long::sum);
            });
    return counts;
  }

  long completedCheckpoints(String jobId) {
    long[] completed = {0};
    reporter
        .getMetricsByGroup()
        .forEach(
            (group, metrics) -> {
              if (!jobId.equals(group.getAllVariables().get("<job_id>"))) return;
              var metric = metrics.get("numberOfCompletedCheckpoints");
              if (metric instanceof org.apache.flink.metrics.Gauge) {
                completed[0] =
                    Math.max(
                        completed[0],
                        ((Number) ((org.apache.flink.metrics.Gauge<?>) metric).getValue())
                            .longValue());
              }
            });
    return completed[0];
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
              Object outputCandidate;
              Object inputCandidate;
              if (!((inputCandidate = metrics.get("numRecordsIn")) instanceof Counter)
                  || !((outputCandidate = metrics.get("numRecordsOut")) instanceof Counter)) return;
              Counter input = ((Counter) inputCandidate);

              Counter output = ((Counter) outputCandidate);

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
    String metricOperator =
        planOperator.equals("NativeCalc") || planOperator.equals("NativeFilter")
            ? planOperator + "("
            : planOperator.replace("Aggregate", "Agg") + "ExecNode";
    long consumed =
        observations.stream()
            .filter(
                row -> {
                  String operator = row.get("operator").toString();
                  return operator.startsWith(metricOperator)
                      || (planOperator.equals("NativeCalc")
                          && operator.startsWith("NativeCalcExecNode["));
                })
            .mapToLong(row -> ((Number) row.get("recordsIn")).longValue())
            .sum();
    org.junit.jupiter.api.Assertions.assertTrue(
        consumed > 0, () -> "no consumed rows for " + metricOperator + ": " + observations);
  }

  @Override
  public void close() {
    long started = CiFixtureTimings.start();
    try {
      if (cluster != null) cluster.after();
    } finally {
      reporter.close();
      CiFixtureTimings.record("recovery-metrics.cluster-close", started);
    }
  }
}

package tech.streamfusion.doris;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;

/** Retains operator counters after bounded jobs finish, with an explicitly configured cluster. */
final class DorisExecutionMetrics implements AutoCloseable {
  private final InMemoryReporter reporter = InMemoryReporter.createWithRetainedMetrics();
  private MiniClusterResource cluster;

  StreamExecutionEnvironment environment(int parallelism) throws Exception {
    if (cluster == null) {
      var config = reporter.addToConfiguration(new Configuration());
      config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(256));
      cluster =
          new MiniClusterResource(
              new MiniClusterResourceConfiguration.Builder()
                  .setConfiguration(config)
                  .setNumberTaskManagers(1)
                  .setNumberSlotsPerTaskManager(8)
                  .build());
      cluster.before();
    }
    return new TestStreamEnvironment(
        cluster.getMiniCluster(), new Configuration(), parallelism, List.of(), List.of());
  }

  long recordsIn(String job, String operator) {
    long[] total = {0};
    reporter
        .getMetricsByGroup()
        .forEach(
            (group, metrics) -> {
              var variables = group.getAllVariables();
              if (group instanceof OperatorMetricGroup
                  && job.equals(variables.get("<job_id>"))
                  && variables.getOrDefault("<operator_name>", "").contains(operator)
                  && metrics.get("numRecordsIn") instanceof Counter)
                total[0] += ((Counter) metrics.get("numRecordsIn")).getCount();
            });
    return total[0];
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

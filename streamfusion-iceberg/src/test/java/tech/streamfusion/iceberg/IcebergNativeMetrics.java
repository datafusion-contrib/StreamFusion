package tech.streamfusion.iceberg;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;

/** Retains the actual worker counters after local streaming jobs shut down. */
public final class IcebergNativeMetrics implements MetricReporterFactory {
  private static final ConcurrentHashMap<String, List<Counter>> COUNTERS =
      new ConcurrentHashMap<>();

  static void reset() {
    COUNTERS.clear();
  }

  static long count(String name) {
    return COUNTERS.getOrDefault(name, List.of()).stream().mapToLong(Counter::getCount).sum();
  }

  @Override
  public MetricReporter createMetricReporter(Properties properties) {
    return new MetricReporter() {
      public void open(MetricConfig config) {}

      public void close() {}

      public void notifyOfAddedMetric(Metric metric, String name, MetricGroup group) {
        if (metric instanceof Counter)
          COUNTERS
              .computeIfAbsent(name, ignored -> new CopyOnWriteArrayList<>())
              .add((Counter) metric);
      }

      public void notifyOfRemovedMetric(Metric metric, String name, MetricGroup group) {}
    };
  }
}

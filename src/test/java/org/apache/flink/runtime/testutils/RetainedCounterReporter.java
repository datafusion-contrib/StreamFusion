package org.apache.flink.runtime.testutils;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;

/** Completed benchmark counters without gauges that retain closed operators and writer buffers. */
public final class RetainedCounterReporter extends InMemoryReporter {
  public RetainedCounterReporter() {
    super(true);
  }

  @Override
  public void notifyOfAddedMetric(Metric metric, String name, MetricGroup group) {
    if (metric instanceof Counter) super.notifyOfAddedMetric(metric, name, group);
  }
}

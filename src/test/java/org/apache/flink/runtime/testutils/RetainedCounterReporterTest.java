package org.apache.flink.runtime.testutils;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.Test;

class RetainedCounterReporterTest {
  @Test
  void retainsCompletedCountersWithoutOperatorCapturingGauges() {
    var reporter = new RetainedCounterReporter();
    try {
      var group = new UnregisteredMetricsGroup();
      var counter = new SimpleCounter();
      counter.inc(17);
      reporter.notifyOfAddedMetric(counter, "rows", group);
      reporter.notifyOfAddedMetric((Gauge<Long>) () -> 64L << 20, "writerBytes", group);
      reporter.notifyOfRemovedMetric(counter, "rows", group);
      assertEquals(17, ((SimpleCounter) reporter.getMetricsByGroup(group).get("rows")).getCount());
      assertFalse(reporter.getMetricsByGroup(group).containsKey("writerBytes"));
    } finally {
      reporter.close();
    }
  }
}

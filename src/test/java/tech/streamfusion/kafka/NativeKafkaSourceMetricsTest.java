package tech.streamfusion.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("streamfusion-kafka")
class NativeKafkaSourceMetricsTest {

  @Test
  void publishesTheAbsorbedTransientErrorCount() {
    TestingMetricGroup group = new TestingMetricGroup();
    NativeKafkaSourceMetrics metrics =
        new NativeKafkaSourceMetrics(InternalSourceReaderMetricGroup.mock(group));

    assertThat(group.gaugeValue("transientConsumerErrors")).isEqualTo(0L);

    metrics.recordTransientErrors(7);
    assertThat(group.gaugeValue("transientConsumerErrors")).isEqualTo(7L);

    // The native count is cumulative over the consumer's lifetime; the gauge tracks it as-is.
    metrics.recordTransientErrors(7);
    metrics.recordTransientErrors(12);
    assertThat(group.gaugeValue("transientConsumerErrors")).isEqualTo(12L);
  }

  /** Flattens the group hierarchy so any registered gauge is reachable by name. */
  private static final class TestingMetricGroup extends UnregisteredMetricsGroup {
    private final Map<String, Gauge<?>> gauges = new HashMap<>();

    @Override
    public MetricGroup addGroup(String name) {
      return this;
    }

    @Override
    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
      gauges.put(name, gauge);
      return gauge;
    }

    long gaugeValue(String name) {
      return (Long) gauges.get(name).getValue();
    }
  }
}

package io.github.jordepic.streamfusion.operator;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.jordepic.streamfusion.operator.MiniBatchMetrics.FlushReason;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.Test;

class MiniBatchMetricsTest {

  @Test
  void recordsBundleShapeAndFlushReason() {
    TestingMetricGroup group = new TestingMetricGroup();
    MiniBatchMetrics metrics = new MiniBatchMetrics(group);

    metrics.onPhysicalBatch();
    metrics.onSlice(3, true);
    metrics.onPhysicalBatch();
    metrics.onPhysicalBatchSplit();
    metrics.onSlice(1, true);
    metrics.onFlush(FlushReason.COUNT, 2, 2, 128);

    assertThat(group.counterValue("miniBatchInputRows")).isEqualTo(4);
    assertThat(group.counterValue("miniBatchInputBatches")).isEqualTo(2);
    assertThat(group.counterValue("miniBatchBundles")).isEqualTo(1);
    assertThat(group.counterValue("miniBatchCountFlushes")).isEqualTo(1);
    assertThat(group.counterValue("miniBatchPhysicalBatchSplits")).isEqualTo(1);
    assertThat(group.counterValue("miniBatchOutputRows")).isEqualTo(2);
    assertThat(group.gaugeValue("miniBatchCurrentRows")).isEqualTo(0L);
    assertThat(group.gaugeValue("miniBatchLastRows")).isEqualTo(4L);
    assertThat(group.gaugeValue("miniBatchLastPhysicalBatches")).isEqualTo(2L);
    assertThat(group.gaugeValue("miniBatchPeakTransientBytes")).isEqualTo(128L);
  }

  @Test
  void doesNotCountEmptyFlushesAsBundles() {
    TestingMetricGroup group = new TestingMetricGroup();
    MiniBatchMetrics metrics = new MiniBatchMetrics(group);

    metrics.onFlush(FlushReason.WATERMARK, 0, 0, 0);

    assertThat(group.counterValue("miniBatchBundles")).isZero();
    assertThat(group.counterValue("miniBatchWatermarkFlushes")).isZero();
  }

  private static final class TestingMetricGroup extends UnregisteredMetricsGroup {
    private final Map<String, Counter> counters = new HashMap<>();
    private final Map<String, Gauge<?>> gauges = new HashMap<>();

    @Override
    public Counter counter(String name) {
      return counter(name, new SimpleCounter());
    }

    @Override
    public <C extends Counter> C counter(String name, C counter) {
      counters.put(name, counter);
      return counter;
    }

    @Override
    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
      gauges.put(name, gauge);
      return gauge;
    }

    long counterValue(String name) {
      return counters.get(name).getCount();
    }

    Object gaugeValue(String name) {
      return gauges.get(name).getValue();
    }
  }
}

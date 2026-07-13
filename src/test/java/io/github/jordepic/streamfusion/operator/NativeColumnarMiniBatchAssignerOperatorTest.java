package io.github.jordepic.streamfusion.operator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class NativeColumnarMiniBatchAssignerOperatorTest {

  @Test
  void emitsMarkersFromProcessingTime() throws Exception {
    NativeColumnarMiniBatchAssignerOperator operator =
        new NativeColumnarMiniBatchAssignerOperator(100);

    try (OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
        new OneInputStreamOperatorTestHarness<>(operator)) {
      harness.open();

      harness.setProcessingTime(99);
      assertThat(harness.getOutput()).isEmpty();
      harness.setProcessingTime(100);

      assertThat(harness.getOutput()).containsExactly(new Watermark(100));
    }
  }

  @Test
  void ignoresUpstreamWatermarksExceptForEndOfInput() throws Exception {
    NativeColumnarMiniBatchAssignerOperator operator =
        new NativeColumnarMiniBatchAssignerOperator(100);

    try (OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
        new OneInputStreamOperatorTestHarness<>(operator)) {
      harness.open();

      harness.processWatermark(new Watermark(2));
      harness.processWatermark(new Watermark(99));
      assertThat(harness.getOutput()).isEmpty();

      harness.processWatermark(Watermark.MAX_WATERMARK);
      harness.processWatermark(Watermark.MAX_WATERMARK);

      assertThat(harness.getOutput()).containsExactlyElementsOf(List.of(Watermark.MAX_WATERMARK));
    }
  }
}

package io.github.jordepic.streamfusion.operator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class NativeColumnarRowTimeMiniBatchAssignerOperatorTest {

  @Test
  void forwardsOnlyWatermarksThatCrossTheEventTimeInterval() throws Exception {
    NativeColumnarRowTimeMiniBatchAssignerOperator operator =
        new NativeColumnarRowTimeMiniBatchAssignerOperator(5);

    try (OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
        new OneInputStreamOperatorTestHarness<>(operator)) {
      harness.open();

      for (long watermark : List.of(2L, 3L, 4L, 5L, 6L, 10L, 12L, 15L, 16L, 19L, 20L, 21L)) {
        harness.processWatermark(new Watermark(watermark));
      }

      assertThat(harness.getOutput())
          .containsExactly(
              new Watermark(4),
              new Watermark(10),
              new Watermark(15),
              new Watermark(19));

      operator.finish();
      assertThat(harness.getOutput().poll()).isEqualTo(new Watermark(4));
      assertThat(harness.getOutput().poll()).isEqualTo(new Watermark(10));
      assertThat(harness.getOutput().poll()).isEqualTo(new Watermark(15));
      assertThat(harness.getOutput().poll()).isEqualTo(new Watermark(19));
      assertThat(harness.getOutput().poll()).isEqualTo(new Watermark(21));
    }
  }

  @Test
  void forwardsTheTerminalWatermark() throws Exception {
    NativeColumnarRowTimeMiniBatchAssignerOperator operator =
        new NativeColumnarRowTimeMiniBatchAssignerOperator(50);

    try (OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
        new OneInputStreamOperatorTestHarness<>(operator)) {
      harness.open();

      harness.processWatermark(new Watermark(2));
      harness.processWatermark(Watermark.MAX_WATERMARK);

      assertThat(harness.getOutput()).containsExactly(Watermark.MAX_WATERMARK);
    }
  }
}

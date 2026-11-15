package tech.streamfusion.operator;

import java.time.Duration;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

/**
 * The native source's per-split watermark strategy, reproducing Flink's pushed-down SQL watermark
 * (`WATERMARK FOR rt AS rt [- INTERVAL const]`, periodic emit). The source operator runs one
 * generator per split and combines them with min + idleness — Flink's own machinery, driven by the
 * batch record metadata the emitter supplies (each per-partition batch's max watermark candidate).
 * The generator mirrors the semantics of Flink's {@code
 * GeneratedWatermarkGeneratorSupplier.DefaultWatermarkGenerator}: watermark = max(rowtime -
 * interval), starting at {@code Long.MIN_VALUE}, emitted unconditionally on the periodic tick (the
 * pipeline's auto-watermark interval).
 */
public final class NativeSourceWatermarks {

  private NativeSourceWatermarks() {}

  public static WatermarkStrategy<ArrowBatch> strategy(long idleTimeoutMillis) {
    WatermarkStrategy<ArrowBatch> strategy =
        WatermarkStrategy.forGenerator(context -> new MaxCandidateGenerator());
    return idleTimeoutMillis > 0
        ? strategy.withIdleness(Duration.ofMillis(idleTimeoutMillis))
        : strategy;
  }

  /**
   * Evaluate before reducing: calendar month-end clamping can reverse timestamp order. Rowtime is a
   * timestamp or a BIGINT carrying epoch millis for {@code TO_TIMESTAMP_LTZ(col, 3)}.
   */
  static Summary summarize(
      VectorSchemaRoot root, int index, WatermarkExpression.Evaluator expression) {
    if (index < 0) {
      return new Summary(Long.MIN_VALUE, Long.MIN_VALUE);
    }
    FieldVector vector = root.getVector(index);
    WatermarkExpression.Values rowtimes = WatermarkExpression.timestampValues(vector);
    int rows = root.getRowCount();
    long max = Long.MIN_VALUE;
    long candidate = Long.MIN_VALUE;
    try (WatermarkExpression.Values candidates = expression.evaluate(root)) {
      for (int i = 0; i < rows; i++) {
        if (!vector.isNull(i)) {
          max = Math.max(max, rowtimes.getMillis(i));
        }
        if (!candidates.isNull(i)) {
          candidate = Math.max(candidate, candidates.getMillis(i));
        }
      }
    }
    return new Summary(max, candidate);
  }

  /**
   * Source-local values that remain readable after downstream releases the batch's Arrow buffers.
   */
  static final class Summary {
    final long maxRowtimeMillis;
    final long maxWatermarkMillis;

    Summary(long maxRowtimeMillis, long maxWatermarkMillis) {
      this.maxRowtimeMillis = maxRowtimeMillis;
      this.maxWatermarkMillis = maxWatermarkMillis;
    }
  }

  private static final class MaxCandidateGenerator implements WatermarkGenerator<ArrowBatch> {

    private long currentWatermark = Long.MIN_VALUE;

    @Override
    public void onEvent(ArrowBatch batch, long timestamp, WatermarkOutput output) {
      currentWatermark = Math.max(currentWatermark, batch.sourceWatermarkMillis());
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
      output.emitWatermark(new Watermark(currentWatermark));
    }
  }
}

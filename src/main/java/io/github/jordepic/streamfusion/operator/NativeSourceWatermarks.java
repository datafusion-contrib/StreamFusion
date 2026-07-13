package io.github.jordepic.streamfusion.operator;

import java.time.Duration;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

/**
 * The native source's per-split watermark strategy, reproducing Flink's pushed-down SQL watermark
 * (`WATERMARK FOR rt AS rt [- INTERVAL const]`, periodic emit). The source operator runs one generator
 * per split and combines them with min + idleness — Flink's own machinery, driven by the batch record
 * timestamps the emitter supplies (each per-partition batch's max rowtime, equivalent to feeding every
 * row since the delay is constant and the generator keeps a max). The generator mirrors the semantics
 * of Flink's {@code GeneratedWatermarkGeneratorSupplier.DefaultWatermarkGenerator}: watermark =
 * max(rowtime) - delay, starting at {@code Long.MIN_VALUE}, emitted unconditionally on the periodic
 * tick (the pipeline's auto-watermark interval).
 */
public final class NativeSourceWatermarks {

  private NativeSourceWatermarks() {}

  public static WatermarkStrategy<ArrowBatch> strategy(long delayMillis, long idleTimeoutMillis) {
    WatermarkStrategy<ArrowBatch> strategy =
        WatermarkStrategy.forGenerator(context -> new MaxRowtimeGenerator(delayMillis));
    return idleTimeoutMillis > 0
        ? strategy.withIdleness(Duration.ofMillis(idleTimeoutMillis))
        : strategy;
  }

  /**
   * Max of a batch's rowtime column in epoch millis, or {@code Long.MIN_VALUE} when every value is
   * null — the Java analog of the native {@code max_rowtime_millis}, for readers whose batches are
   * already imported (the Kafka split reader reads the decoded root directly; Fluss computes it
   * natively before export). The rowtime is either a timestamp column or a BIGINT already holding
   * epoch millis (the {@code TO_TIMESTAMP_LTZ(col, 3)} computed-rowtime idiom).
   */
  public static long maxRowtimeMillis(VectorSchemaRoot root, int index) {
    FieldVector vector = root.getVector(index);
    int rows = root.getRowCount();
    long max = Long.MIN_VALUE;
    if (vector instanceof BigIntVector) {
      BigIntVector epochMillis = (BigIntVector) vector;
      for (int i = 0; i < rows; i++) {
        if (!epochMillis.isNull(i)) {
          max = Math.max(max, epochMillis.get(i));
        }
      }
      return max;
    }
    TimeStampVector timestamps = (TimeStampVector) vector;
    for (int i = 0; i < rows; i++) {
      if (!timestamps.isNull(i)) {
        max = Math.max(max, timestamps.get(i));
      }
    }
    if (max == Long.MIN_VALUE) {
      return max;
    }
    // Floor division is monotonic, so converting the max equals the max of conversions (and floors
    // pre-epoch values toward the earlier millisecond, matching the native implementation).
    switch (((ArrowType.Timestamp) timestamps.getField().getType()).getUnit()) {
      case SECOND:
        return max * 1_000L;
      case MILLISECOND:
        return max;
      case MICROSECOND:
        return Math.floorDiv(max, 1_000L);
      case NANOSECOND:
      default:
        return Math.floorDiv(max, 1_000_000L);
    }
  }

  private static final class MaxRowtimeGenerator implements WatermarkGenerator<ArrowBatch> {

    private final long delayMillis;
    private long currentWatermark = Long.MIN_VALUE;

    MaxRowtimeGenerator(long delayMillis) {
      this.delayMillis = delayMillis;
    }

    @Override
    public void onEvent(ArrowBatch batch, long maxRowtimeMillis, WatermarkOutput output) {
      // Long.MIN_VALUE is the no-timestamp sentinel (a batch whose rowtimes were all null).
      if (maxRowtimeMillis == Long.MIN_VALUE) {
        return;
      }
      long watermark = maxRowtimeMillis - delayMillis;
      if (watermark > currentWatermark) {
        currentWatermark = watermark;
      }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
      output.emitWatermark(new Watermark(currentWatermark));
    }
  }
}

package io.github.jordepic.streamfusion.kafka;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import java.time.Duration;
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
public final class NativeKafkaWatermarks {

  private NativeKafkaWatermarks() {}

  public static WatermarkStrategy<ArrowBatch> strategy(long delayMillis, long idleTimeoutMillis) {
    WatermarkStrategy<ArrowBatch> strategy =
        WatermarkStrategy.forGenerator(context -> new MaxRowtimeGenerator(delayMillis));
    return idleTimeoutMillis > 0
        ? strategy.withIdleness(Duration.ofMillis(idleTimeoutMillis))
        : strategy;
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

package tech.streamfusion.compat;

import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;

/** Forwards host control events without changing the record representation. */
public abstract class FlinkOutput<T> implements Output<StreamRecord<T>> {
  private final Output<?> delegate;

  protected FlinkOutput(Output<?> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void emitWatermark(Watermark watermark) {
    delegate.emitWatermark(watermark);
  }

  @Override
  public void emitWatermarkStatus(WatermarkStatus status) {
    delegate.emitWatermarkStatus(status);
  }

  @Override
  public <X> void collect(OutputTag<X> tag, StreamRecord<X> record) {
    delegate.collect(tag, record);
  }

  @Override
  public void emitLatencyMarker(LatencyMarker marker) {
    delegate.emitLatencyMarker(marker);
  }

  @Override
  public void close() {}
}

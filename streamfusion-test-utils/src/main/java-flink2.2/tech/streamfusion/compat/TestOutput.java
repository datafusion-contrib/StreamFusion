package tech.streamfusion.compat;

public abstract class TestOutput<T> implements org.apache.flink.streaming.api.operators.Output<T> {
  @Override
  public void emitWatermark(org.apache.flink.runtime.event.WatermarkEvent event) {}

  @Override
  public void emitRecordAttributes(
      org.apache.flink.streaming.runtime.streamrecord.RecordAttributes attributes) {}
}

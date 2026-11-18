package tech.streamfusion.compat;

public final class WatermarkTestOutputs {
  private WatermarkTestOutputs() {}

  public static void register(
      org.apache.flink.api.common.eventtime.WatermarkOutputMultiplexer multiplexer, String id) {
    multiplexer.registerNewOutput(id, watermark -> {});
  }
}

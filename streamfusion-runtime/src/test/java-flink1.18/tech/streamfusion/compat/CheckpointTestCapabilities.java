package tech.streamfusion.compat;

public final class CheckpointTestCapabilities {
  private CheckpointTestCapabilities() {}

  public static final boolean PER_EDGE_ALIGNMENT = false;

  public static void configure(
      org.apache.flink.streaming.api.environment.StreamExecutionEnvironment env,
      boolean recoverable) {
    if (PER_EDGE_ALIGNMENT || recoverable) env.getCheckpointConfig().enableUnalignedCheckpoints();
  }
}

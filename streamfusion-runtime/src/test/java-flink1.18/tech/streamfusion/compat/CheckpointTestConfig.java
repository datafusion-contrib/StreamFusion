package tech.streamfusion.compat;

import org.apache.flink.configuration.Configuration;

public final class CheckpointTestConfig {
  private CheckpointTestConfig() {}

  public static void retainOnCancellation(Configuration config) {
    config.set(
        org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
            .EXTERNALIZED_CHECKPOINT,
        org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
            .RETAIN_ON_CANCELLATION);
  }

  public static void restore(Configuration config, String path) {
    config.set(org.apache.flink.runtime.jobgraph.SavepointConfigOptions.SAVEPOINT_PATH, path);
  }
}

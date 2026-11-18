package tech.streamfusion.compat;

import org.apache.flink.configuration.Configuration;

public final class CheckpointTestConfig {
  private CheckpointTestConfig() {}

  public static void retainOnCancellation(Configuration config) {
    config.set(
        org.apache.flink.configuration.CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
        org.apache.flink.configuration.ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
  }

  public static void restore(Configuration config, String path) {
    config.set(org.apache.flink.configuration.StateRecoveryOptions.SAVEPOINT_PATH, path);
  }
}

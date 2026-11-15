package tech.streamfusion.compat;

import org.apache.flink.runtime.checkpoint.FullyFinishedOperatorState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.jobgraph.OperatorID;

public final class CheckpointMetadataCompat {
  private CheckpointMetadataCompat() {}

  private static final String CHECKPOINT_DIRECTORY = "streamfusion.canonical.checkpoint-directory";

  public static void resetStorage(
      org.apache.flink.streaming.api.environment.StreamExecutionEnvironment environment,
      org.apache.flink.configuration.Configuration config) {
    String directory = config.getString(CHECKPOINT_DIRECTORY, "");
    if (!directory.isEmpty()) {
      config.set(
          org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY, directory);
      config.removeKey(CHECKPOINT_DIRECTORY);
    }
    var storage = environment.getCheckpointConfig().getCheckpointStorage();
    if (storage != null)
      environment
          .getCheckpointConfig()
          .setCheckpointStorage(tech.streamfusion.state.CanonicalCheckpointStorage.unwrap(storage));
  }

  public static void installStorage(
      org.apache.flink.streaming.api.environment.StreamExecutionEnvironment environment,
      org.apache.flink.configuration.Configuration config,
      tech.streamfusion.state.OperatorIdentityProjection projection) {
    var checkpoint = environment.getCheckpointConfig();
    checkpoint.configure(config);
    var storage = checkpoint.getCheckpointStorage();
    if (storage == null)
      storage =
          new tech.streamfusion.state.CanonicalCheckpointStorageFactory()
              .createFromConfig(config, Thread.currentThread().getContextClassLoader());
    if (storage != null) {
      checkpoint.setCheckpointStorage(
          new tech.streamfusion.state.CanonicalCheckpointStorage(
              tech.streamfusion.state.CanonicalCheckpointStorage.unwrap(storage), projection));
      // The 1.18 executor configures this again after planning. Keep its materialized
      // provider rather than replacing our wrapper from the directory option.
      config
          .getOptional(org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY)
          .ifPresent(directory -> config.setString(CHECKPOINT_DIRECTORY, directory));
      config.removeConfig(
          org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY);
    }
  }

  public static OperatorState operator(OperatorState original, OperatorID id, boolean finished) {
    return finished
        ? new FullyFinishedOperatorState(
            id, original.getParallelism(), original.getMaxParallelism())
        : new OperatorState(id, original.getParallelism(), original.getMaxParallelism());
  }

  public static OperatorState helper(
      OperatorID id, int parallelism, int maximum, boolean finished) {
    return finished
        ? new FullyFinishedOperatorState(id, parallelism, maximum)
        : new OperatorState(id, parallelism, maximum);
  }

  public static org.apache.flink.runtime.state.CheckpointStorage storage(
      org.apache.flink.configuration.Configuration config, ClassLoader loader) throws Exception {
    return org.apache.flink.runtime.state.CheckpointStorageLoader.load(
        null,
        null,
        new org.apache.flink.runtime.state.hashmap.HashMapStateBackend(),
        config,
        loader,
        null);
  }
}

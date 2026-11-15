package tech.streamfusion.compat;

import org.apache.flink.runtime.checkpoint.FullyFinishedOperatorState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.jobgraph.OperatorID;

public final class CheckpointMetadataCompat {
  private CheckpointMetadataCompat() {}

  public static void resetStorage(
      org.apache.flink.streaming.api.environment.StreamExecutionEnvironment environment,
      org.apache.flink.configuration.Configuration config) {}

  public static void installStorage(
      org.apache.flink.streaming.api.environment.StreamExecutionEnvironment environment,
      org.apache.flink.configuration.Configuration config,
      tech.streamfusion.state.OperatorIdentityProjection projection) {}

  public static OperatorState operator(OperatorState original, OperatorID id, boolean finished) {
    return finished
        ? new FullyFinishedOperatorState(
            original.getOperatorName().orElse(null),
            original.getOperatorUid().orElse(null),
            id,
            original.getParallelism(),
            original.getMaxParallelism())
        : new OperatorState(
            original.getOperatorName().orElse(null),
            original.getOperatorUid().orElse(null),
            id,
            original.getParallelism(),
            original.getMaxParallelism());
  }

  public static OperatorState helper(
      OperatorID id, int parallelism, int maximum, boolean finished) {
    return finished
        ? new FullyFinishedOperatorState("native boundary", null, id, parallelism, maximum)
        : new OperatorState("native boundary", null, id, parallelism, maximum);
  }

  public static org.apache.flink.runtime.state.CheckpointStorage storage(
      org.apache.flink.configuration.Configuration config, ClassLoader loader) throws Exception {
    return org.apache.flink.runtime.state.CheckpointStorageLoader.load(
        null,
        new org.apache.flink.runtime.state.hashmap.HashMapStateBackend(),
        config,
        new org.apache.flink.configuration.Configuration(),
        loader,
        null);
  }
}

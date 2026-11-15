package tech.streamfusion.compat;

public interface StreamStateHandleCompat extends org.apache.flink.runtime.state.StreamStateHandle {
  org.apache.flink.runtime.state.StreamStateHandle handleDelegate();

  @Override
  default java.util.Optional<org.apache.flink.core.fs.Path> maybeGetPath() {
    return handleDelegate().maybeGetPath();
  }

  @Override
  default void collectSizeStats(
      org.apache.flink.runtime.state.StateObject.StateObjectSizeStatsCollector collector) {
    handleDelegate().collectSizeStats(collector);
  }
}

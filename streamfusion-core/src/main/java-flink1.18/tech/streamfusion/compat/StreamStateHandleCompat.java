package tech.streamfusion.compat;

public interface StreamStateHandleCompat extends org.apache.flink.runtime.state.StreamStateHandle {
  org.apache.flink.runtime.state.StreamStateHandle handleDelegate();
}

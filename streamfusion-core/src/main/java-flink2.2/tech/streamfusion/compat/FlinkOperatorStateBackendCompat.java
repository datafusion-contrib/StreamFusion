package tech.streamfusion.compat;

import org.apache.flink.runtime.state.OperatorStateBackend;

/** Keeps released operator-state APIs behind the common snapshot lifecycle. */
public abstract class FlinkOperatorStateBackendCompat implements OperatorStateBackend {
  protected final OperatorStateBackend delegate;

  protected FlinkOperatorStateBackendCompat(OperatorStateBackend delegate) {
    this.delegate = delegate;
  }

  @Override
  public <K, V> org.apache.flink.api.common.state.BroadcastState<K, V> getBroadcastState(
      org.apache.flink.api.common.state.v2.MapStateDescriptor<K, V> descriptor) throws Exception {
    return delegate.getBroadcastState(descriptor);
  }

  @Override
  public <S> org.apache.flink.api.common.state.v2.ListState<S> getListState(
      org.apache.flink.api.common.state.v2.ListStateDescriptor<S> descriptor) throws Exception {
    return delegate.getListState(descriptor);
  }

  @Override
  public <S> org.apache.flink.api.common.state.v2.ListState<S> getUnionListState(
      org.apache.flink.api.common.state.v2.ListStateDescriptor<S> descriptor) throws Exception {
    return delegate.getUnionListState(descriptor);
  }
}

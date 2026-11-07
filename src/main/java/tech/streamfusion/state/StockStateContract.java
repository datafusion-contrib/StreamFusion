package tech.streamfusion.state;

import java.io.Serializable;
import java.util.Map;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.data.RowData;

/** Released Flink state schema and direct native transfer for one qualified operator shape. */
public interface StockStateContract extends Serializable {
  TypeSerializer<RowData> keySerializer();

  Map<String, StateDescriptor.Type> stateDescriptors();

  default TypeSerializer<?> namespaceSerializer(String stateName) {
    return VoidNamespaceSerializer.INSTANCE;
  }

  default java.util.Set<String> timerQueues() {
    return java.util.Set.of();
  }

  default Map<String, StockStateInventory.Timer> timerSchema() {
    Map<String, StockStateInventory.Timer> result = new java.util.HashMap<>();
    timerQueues()
        .forEach(
            name ->
                result.put(
                    name, StockStateInventory.Timer.consumed(VoidNamespaceSerializer.INSTANCE)));
    return result;
  }

  default void initializeOperatorState(OperatorStateStore store, boolean restored)
      throws Exception {}

  default void snapshotOperatorState(long watermark) throws Exception {}

  default long restoredWatermark() {
    return Long.MIN_VALUE;
  }

  default void configureNativeState(long nativeHandle, boolean persistent) throws Exception {}

  void exportState(
      CheckpointableKeyedStateBackend<RowData> backend, long nativeHandle, boolean persistent)
      throws Exception;

  void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long nativeHandle, boolean persistent)
      throws Exception;

  void clear(CheckpointableKeyedStateBackend<RowData> backend) throws Exception;
}

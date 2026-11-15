package tech.streamfusion.state;

import java.util.Map;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/** Proctime bare-update mode registers the released row schema but never retains stock rows. */
public final class StockEmptyDedupStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private final StockDedupStateContract schema;

  public StockEmptyDedupStateContract(
      TypeSerializer<RowData> keys, RowType keyType, RowType rowType, long retention) {
    schema = new StockDedupStateContract(keys, keyType, rowType, false, -1, retention);
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return schema.keySerializer();
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    return schema.stateDescriptors();
  }

  @Override
  public void exportState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    schema.clear(backend);
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    schema.requireEmptyStockState(backend);
    schema.importState(backend, handle, persistent);
  }
}

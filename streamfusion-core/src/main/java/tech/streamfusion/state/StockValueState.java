package tech.streamfusion.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.ttl.TtlStateFactory.TtlSerializer;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.table.data.RowData;

/** Reads and writes released TTL envelopes without refreshing their original timestamps. */
final class StockValueState<T> {
  private final ValueState<T> plain;
  private final ValueState<TtlValue<T>> expiring;

  StockValueState(
      CheckpointableKeyedStateBackend<RowData> backend,
      String name,
      TypeSerializer<T> serializer,
      boolean ttl)
      throws Exception {
    plain =
        ttl
            ? null
            : backend.getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new ValueStateDescriptor<>(name, serializer));
    // Deliberately use the raw released serializer without enabling the API's clock wrapper.
    expiring =
        !ttl
            ? null
            : backend.getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new ValueStateDescriptor<>(
                    name, new TtlSerializer<>(LongSerializer.INSTANCE, serializer)));
  }

  TtlValue<T> value() throws Exception {
    if (expiring != null) return expiring.value();
    T value = plain.value();
    return value == null ? null : new TtlValue<>(value, 0);
  }

  void update(T value, long timestamp) throws Exception {
    if (expiring != null) expiring.update(new TtlValue<>(value, timestamp));
    else plain.update(value);
  }

  void clear() {
    if (expiring != null) expiring.clear();
    else plain.clear();
  }
}

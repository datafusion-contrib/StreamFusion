package tech.streamfusion.state;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.ttl.TtlStateFactory.TtlSerializer;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.table.data.RowData;

/** Visits released map entries without expiry filtering or timestamp refresh. */
final class StockMapState<K, V> {
  private final MapState<K, V> plain;
  private final MapState<K, TtlValue<V>> expiring;

  StockMapState(
      CheckpointableKeyedStateBackend<RowData> backend,
      String name,
      TypeSerializer<K> keys,
      TypeSerializer<V> values,
      boolean ttl)
      throws Exception {
    plain =
        ttl
            ? null
            : backend.getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new MapStateDescriptor<>(name, keys, values));
    expiring =
        !ttl
            ? null
            : backend.getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new MapStateDescriptor<>(
                    name, keys, new TtlSerializer<>(LongSerializer.INSTANCE, values)));
  }

  Iterable<Map.Entry<K, TtlValue<V>>> entries() throws Exception {
    if (expiring != null) {
      var entries = expiring.entries();
      return entries == null ? Collections.emptyList() : entries;
    }
    var entries = plain.entries();
    if (entries == null) return Collections.emptyList();
    return () ->
        new Iterator<Map.Entry<K, TtlValue<V>>>() {
          private final Iterator<Map.Entry<K, V>> iterator = entries.iterator();

          @Override
          public boolean hasNext() {
            return iterator.hasNext();
          }

          @Override
          public Map.Entry<K, TtlValue<V>> next() {
            var entry = iterator.next();
            return new SimpleImmutableEntry<>(entry.getKey(), new TtlValue<>(entry.getValue(), 0));
          }
        };
  }

  void put(K key, V value, long written) throws Exception {
    if (expiring != null) expiring.put(key, new TtlValue<>(value, written));
    else plain.put(key, value);
  }

  void clear() {
    if (expiring != null) expiring.clear();
    else plain.clear();
  }
}

package tech.streamfusion.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.table.data.RowData;

/** Serializer, key-group and lifetime ownership for one native canonical capture. */
public final class StockSnapshotContext {
  private final TypeSerializer<RowData> keys;
  private final int maximum;
  private final KeyGroupRange groups;
  private final org.apache.flink.util.function.SupplierWithException<AutoCloseable, Exception>
      retain;

  public StockSnapshotContext(TypeSerializer<RowData> keys, int maximum, KeyGroupRange groups) {
    this(keys, maximum, groups, () -> null);
  }

  public StockSnapshotContext(
      TypeSerializer<RowData> keys,
      int maximum,
      KeyGroupRange groups,
      org.apache.flink.util.function.SupplierWithException<AutoCloseable, Exception> retain) {
    this.retain = retain;
    this.keys = keys;
    this.maximum = maximum;
    this.groups = groups;
  }

  <V> SavepointResources<RowData> nativeValue(
      String state,
      TypeSerializer<V> values,
      boolean ttl,
      org.apache.flink.util.function.SupplierWithException<Long, Exception> capture,
      org.apache.flink.table.types.logical.RowType transfer,
      StockNativeValueSnapshot.Decoder<V> decoder)
      throws Exception {
    long cursor = 0;
    AutoCloseable lease = null;
    try {
      lease = retain.get();
      cursor = capture.get();
      if (cursor == 0)
        throw new IllegalStateException("Native snapshot capture returned no cursor");
      return new SavepointResources<>(
          new StockNativeValueSnapshot<>(
              keys, maximum, groups, state, values, ttl, cursor, transfer, decoder, lease),
          org.apache.flink.runtime.state.SnapshotExecutionType.ASYNCHRONOUS);
    } catch (Exception | Error failure) {
      try {
        tech.streamfusion.Native.closeCanonicalSnapshot(cursor);
      } catch (Throwable cleanup) {
        failure.addSuppressed(cleanup);
      }
      try {
        if (lease != null) lease.close();
      } catch (Throwable cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }
}

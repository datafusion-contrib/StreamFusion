package tech.streamfusion.state;

import java.nio.file.Path;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.table.data.RowData;

/** The fixed ownership and spill location for one canonical capture. */
public final class StockSnapshotContext {
  private final TypeSerializer<RowData> keys;
  private final int maximum;
  private final KeyGroupRange groups;
  private final Path directory;
  private final org.apache.flink.util.function.SupplierWithException<AutoCloseable, Exception>
      retain;

  public StockSnapshotContext(
      TypeSerializer<RowData> keys, int maximum, KeyGroupRange groups, Path directory) {
    this(keys, maximum, groups, directory, () -> null);
  }

  public StockSnapshotContext(
      TypeSerializer<RowData> keys,
      int maximum,
      KeyGroupRange groups,
      Path directory,
      org.apache.flink.util.function.SupplierWithException<AutoCloseable, Exception> retain) {
    this.retain = retain;
    this.keys = keys;
    this.maximum = maximum;
    this.groups = groups;
    this.directory = directory;
  }

  public <V> SavepointResources<RowData> value(
      String state, TypeSerializer<V> values, boolean ttl, StockValueSnapshot.Export<V> exporter)
      throws Exception {
    return StockValueSnapshot.capture(
        keys, maximum, groups, directory, state, values, ttl, exporter);
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
      if (cursor == 0) throw new IllegalStateException("Native snapshot capture returned no cursor");
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

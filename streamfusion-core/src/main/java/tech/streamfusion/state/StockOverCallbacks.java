package tech.streamfusion.state;

import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.Native;

final class StockOverCallbacks {
  static final byte CLEANUP = 0,
      LAST_TRIGGER = 1,
      EVENT = 2,
      PROCESSING = 3,
      OWNER = 4,
      MAP_ORDER = 5;
  private static final String[] FIELDS = {"__over_callback_kind", "__over_callback_time"};

  private StockOverCallbacks() {}

  static final class Writer extends StockKeyedLongRecords.Writer {
    Writer(long handle, RowType keys, boolean validating) {
      super(handle, keys, FIELDS, validating, Native::importOverCanonicalCallbacks);
    }
  }

  static void export(
      CheckpointableKeyedStateBackend<RowData> backend,
      long handle,
      RowType keys,
      StockKeyedLongRecords.Receiver receiver) {
    StockKeyedLongRecords.export(
        backend,
        handle,
        keys,
        FIELDS,
        Native::exportOverCanonicalCallbacks,
        (owner, kind, timestamp) -> {
          if (kind < CLEANUP || kind > PROCESSING)
            throw new IllegalArgumentException("Unknown OVER callback kind");
          receiver.accept(owner, kind, timestamp);
        });
  }
}

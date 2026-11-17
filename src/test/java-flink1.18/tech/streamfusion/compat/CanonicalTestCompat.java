package tech.streamfusion.compat;

public final class CanonicalTestCompat {
  private CanonicalTestCompat() {}

  public static org.apache.flink.runtime.checkpoint.OperatorSubtaskState finishSnapshot(
      org.apache.flink.streaming.api.operators.OperatorSnapshotFutures futures) throws Exception {
    return new org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer(futures)
        .getJobManagerOwnedState();
  }

  public static boolean flink22() {
    return false;
  }

  public static org.apache.flink.runtime.state.StateBackend rocksBackend() {
    return new org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend();
  }

  public static org.apache.flink.runtime.state.StateBackend rawStateBackend() {
    var backend = new org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend();
    backend.setRocksDBOptions(new RawStateRocksDBOptions());
    return backend;
  }

  public static void rawStateConfiguration(org.apache.flink.configuration.Configuration config) {
    config.setString("state.backend.rocksdb.options-factory", RawStateRocksDBOptions.class.getName());
  }

  public static final class RawStateRocksDBOptions
      implements org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public org.rocksdb.DBOptions createDBOptions(
        org.rocksdb.DBOptions options, java.util.Collection<AutoCloseable> handles) {
      return options;
    }

    @Override
    public org.rocksdb.ColumnFamilyOptions createColumnOptions(
        org.rocksdb.ColumnFamilyOptions options, java.util.Collection<AutoCloseable> handles) {
      return options.setDisableAutoCompactions(true);
    }
  }

  public static org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec
      joinSide(int shape, org.apache.flink.table.runtime.keyselector.RowDataKeySelector selector) {
    return shape == 0
        ? org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec
            .withUniqueKeyContainedByJoinKey(selector.getProducedType(), selector)
        : shape == 1
            ? org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec
                .withUniqueKey(selector.getProducedType(), selector)
            : org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec
                .withoutUniqueKey();
  }

  public static org.apache.flink.table.runtime.operators.deduplicate
          .ProcTimeDeduplicateKeepLastRowFunction
      keepLast(
          org.apache.flink.table.runtime.typeutils.InternalTypeInfo<
                  org.apache.flink.table.data.RowData>
              type,
          long ttl,
          boolean updateBefore,
          boolean insert,
          boolean insertOnly,
          org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser equaliser) {
    return new org.apache.flink.table.runtime.operators.deduplicate
        .ProcTimeDeduplicateKeepLastRowFunction(
        type, ttl, updateBefore, insert, insertOnly, equaliser);
  }

  public static org.apache.flink.streaming.api.functions.KeyedProcessFunction<
          org.apache.flink.table.data.RowData,
          org.apache.flink.table.data.RowData,
          org.apache.flink.table.data.RowData>
      bufferedDedup(
          org.apache.flink.table.runtime.typeutils.InternalTypeInfo<
                  org.apache.flink.table.data.RowData>
              type,
          long retention,
          int timeColumn) {
    throw new UnsupportedOperationException("Flink 1.18 uses eager event-time first-row state");
  }

  public static Object ticker(java.util.function.LongSupplier clock) {
    return new org.apache.flink.shaded.guava31.com.google.common.base.Ticker() {
      @Override
      public long read() {
        return clock.getAsLong();
      }
    };
  }
}

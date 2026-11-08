package tech.streamfusion.compat;

public final class CanonicalTestCompat {
  private CanonicalTestCompat() {}

  public static boolean flink22() {
    return true;
  }

  public static org.apache.flink.runtime.state.StateBackend rocksBackend() {
    return new org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend();
  }

  public static org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec
      joinSide(int shape, org.apache.flink.table.runtime.keyselector.RowDataKeySelector selector) {
    return shape == 0
        ? org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec
            .withUniqueKeyContainedByJoinKey(selector.getProducedType(), selector)
        : shape == 1
            ? org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec
                .withUniqueKey(selector.getProducedType(), selector)
            : org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec
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
        type, ttl, updateBefore, insert, insertOnly, equaliser, null);
  }

  public static Object ticker(java.util.function.LongSupplier clock) {
    return new org.apache.flink.shaded.guava33.com.google.common.base.Ticker() {
      @Override
      public long read() {
        return clock.getAsLong();
      }
    };
  }
}

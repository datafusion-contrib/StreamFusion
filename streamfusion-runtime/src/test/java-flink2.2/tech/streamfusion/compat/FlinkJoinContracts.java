package tech.streamfusion.compat;

import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.StreamingSemiAntiJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

public final class FlinkJoinContracts {
  private FlinkJoinContracts() {}

  public static org.apache.flink.runtime.state.StateBackend backend(boolean nativeState) {
    var delegate = new org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend();
    return nativeState
        ? new tech.streamfusion.state.RocksDBNativeStateBackend(
            new org.apache.flink.configuration.Configuration(), delegate)
        : delegate;
  }

  public static TwoInputStreamOperator<RowData, RowData, RowData> operator(
      int kind, InternalTypeInfo<RowData> type, GeneratedJoinCondition condition, boolean unique) {
    var side =
        unique
            ? JoinInputSideSpec.withUniqueKeyContainedByJoinKey(
                InternalTypeInfo.<RowData>of(
                    org.apache.flink.table.types.logical.RowType.of(
                        new org.apache.flink.table.types.logical.BigIntType())),
                r -> org.apache.flink.table.data.GenericRowData.of(r.getLong(0)))
            : JoinInputSideSpec.withoutUniqueKey();
    return kind < 4
        ? new StreamingJoinOperator(
            type,
            type,
            condition,
            side,
            side,
            kind == 1 || kind == 3,
            kind == 2 || kind == 3,
            new boolean[] {true},
            0,
            0)
        : new StreamingSemiAntiJoinOperator(
            kind == 5, type, type, condition, side, side, new boolean[] {true}, 0, 0);
  }
}

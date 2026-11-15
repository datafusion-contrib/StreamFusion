package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountCoBundleTrigger;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.MiniBatchStreamingJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

/** Released-stock behavior only; this supplies no native interoperability credit. */
class CanonicalMiniBatchResidualBaselineTest {
  private static final RowType INPUT = RowType.of(new BigIntType(), new BigIntType());

  @Test
  void immediateStockJoinEmitsBothUnmatchedRowsWhenAnUpdateStopsMatching() throws Exception {
    assertEquals(Map.of("1|15|null|null", 1L, "null|null|1|110", 1L), run(false));
  }

  @Test
  void releasedMiniBatchSuppressionOmitsTheNewlyUnmatchedOppositeRow() throws Exception {
    assertEquals(Map.of("1|15|null|null", 1L), run(true));
  }

  private static Map<String, Long> run(boolean miniBatch) throws Exception {
    var type = InternalTypeInfo.<RowData>of(INPUT);
    var selector =
        KeySelectorUtil.getRowDataSelector(
            CanonicalMiniBatchResidualBaselineTest.class.getClassLoader(), new int[] {0}, type);
    var side =
        JoinInputSideSpec.withUniqueKeyContainedByJoinKey(selector.getProducedType(), selector);
    var condition =
        new GeneratedJoinCondition("unused", "", new Object[0]) {
          @Override
          public JoinCondition newInstance(ClassLoader loader) {
            return new Residual();
          }
        };
    TwoInputStreamOperator<RowData, RowData, RowData> operator =
        miniBatch
            ? MiniBatchStreamingJoinOperator.newMiniBatchStreamJoinOperator(
                FlinkJoinType.FULL,
                type,
                type,
                condition,
                side,
                side,
                true,
                true,
                new boolean[] {true},
                0,
                0,
                new CountCoBundleTrigger<>(100))
            : new StreamingJoinOperator(
                type, type, condition, side, side, true, true, new boolean[] {true}, 0, 0);
    try (var harness =
        new KeyedTwoInputStreamOperatorTestHarness<>(
            operator, selector, selector, selector.getProducedType(), 128, 1, 0)) {
      harness.setStateBackend(new HashMapStateBackend());
      harness.setup(
          new RowDataSerializer(
              RowType.of(new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType())));
      harness.open();
      harness.processElement2(record(RowKind.INSERT, 110));
      harness.processElement1(record(RowKind.INSERT, 10));
      operator.prepareSnapshotPreBarrier(1);
      harness.processElement1(record(RowKind.UPDATE_BEFORE, 10));
      harness.processElement1(record(RowKind.UPDATE_AFTER, 15));
      operator.prepareSnapshotPreBarrier(2);
      Map<String, Long> result = new HashMap<>();
      for (RowData row : harness.extractOutputValues()) {
        var key = new StringBuilder();
        for (int i = 0; i < row.getArity(); i++) {
          if (i > 0) key.append('|');
          key.append(row.isNullAt(i) ? "null" : row.getLong(i));
        }
        result.merge(
            key.toString(),
            row.getRowKind() == RowKind.DELETE || row.getRowKind() == RowKind.UPDATE_BEFORE
                ? -1L
                : 1L,
            Long::sum);
      }
      result.values().removeIf(count -> count == 0);
      return result;
    }
  }

  private static StreamRecord<RowData> record(RowKind kind, long value) {
    return new StreamRecord<>(
        new RowDataSerializer(INPUT).toBinaryRow(GenericRowData.ofKind(kind, 1L, value)).copy());
  }

  public static final class Residual extends AbstractRichFunction implements JoinCondition {
    @Override
    public boolean apply(RowData left, RowData right) {
      return left.getLong(1) < right.getLong(1) && left.getLong(1) % 3 != 0;
    }
  }
}

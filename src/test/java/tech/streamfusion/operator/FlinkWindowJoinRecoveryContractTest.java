package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.window.WindowJoinOperatorBuilder;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class FlinkWindowJoinRecoveryContractTest {
  @Test
  void restoredWindowJoinUsesReplayedWatermarks() throws Exception {
    OperatorSubtaskState snapshot;
    try (var before = harness()) {
      before.open();
      before.processElement1(new StreamRecord<>(row(10, 0, 1000)));
      before.processElement2(new StreamRecord<>(row(100, 0, 1000)));
      before.processElement1(new StreamRecord<>(row(20, 1000, 2000)));
      before.processElement2(new StreamRecord<>(row(200, 1000, 2000)));
      before.processWatermark1(new Watermark(1000));
      before.processWatermark2(new Watermark(1000));
      assertEquals(List.of(List.of(10L, 100L)), pairs(before));
      snapshot = before.snapshot(1, 1);
    }
    try (var restored = harness()) {
      restored.initializeState(snapshot);
      restored.open();
      restored.processElement1(new StreamRecord<>(row(30, 0, 1000)));
      restored.processElement2(new StreamRecord<>(row(300, 0, 1000)));
      restored.processWatermark1(new Watermark(1000));
      restored.processWatermark2(new Watermark(1000));
      assertEquals(List.of(List.of(30L, 300L)), pairs(restored));
      restored.processElement1(new StreamRecord<>(row(40, 0, 1000)));
      restored.processElement2(new StreamRecord<>(row(400, 0, 1000)));
      restored.processWatermark1(new Watermark(2000));
      restored.processWatermark2(new Watermark(2000));
      assertEquals(List.of(List.of(20L, 200L)), pairs(restored));
      restored.processWatermark1(new Watermark(3000));
      restored.processWatermark2(new Watermark(3000));
      assertEquals(List.of(), pairs(restored));
    }
  }

  private static KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
      harness() throws Exception {
    RowType type = RowType.of(new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType());
    String code =
        "public class RecoveryJoinCondition extends"
            + " org.apache.flink.api.common.functions.AbstractRichFunction implements"
            + " org.apache.flink.table.runtime.generated.JoinCondition { public"
            + " RecoveryJoinCondition(Object[] refs) {} public boolean"
            + " apply(org.apache.flink.table.data.RowData a, org.apache.flink.table.data.RowData b)"
            + " { return true; }}";
    var operator =
        WindowJoinOperatorBuilder.builder()
            .leftSerializer(new RowDataSerializer(type))
            .rightSerializer(new RowDataSerializer(type))
            .generatedJoinCondition(
                new GeneratedJoinCondition("RecoveryJoinCondition", code, new Object[0]))
            .leftWindowEndIndex(3)
            .rightWindowEndIndex(3)
            .filterNullKeys(new boolean[] {true})
            .joinType(FlinkJoinType.INNER)
            .withShiftTimezone(ZoneOffset.UTC)
            .build();
    RowDataSerializer keySerializer = new RowDataSerializer(RowType.of(new BigIntType()));
    return new KeyedTwoInputStreamOperatorTestHarness<>(
        operator,
        row -> keySerializer.toBinaryRow(GenericRowData.of(row.getLong(0))).copy(),
        row -> keySerializer.toBinaryRow(GenericRowData.of(row.getLong(0))).copy(),
        InternalTypeInfo.of(RowType.of(new BigIntType())));
  }

  private static RowData row(long value, long start, long end) {
    return GenericRowData.of(1L, value, start, end);
  }

  private static List<List<Long>> pairs(
      KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> harness) {
    List<List<Long>> rows = new ArrayList<>();
    for (Object event; (event = harness.getOutput().poll()) != null; ) {
      if (event instanceof StreamRecord<?>) {
        StreamRecord<?> record = ((StreamRecord<?>) event);

        RowData row = (RowData) record.getValue();
        rows.add(List.of(row.getLong(1), row.getLong(5)));
      }
    }
    return rows;
  }
}

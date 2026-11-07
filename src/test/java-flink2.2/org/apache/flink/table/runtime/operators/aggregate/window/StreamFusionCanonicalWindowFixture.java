package org.apache.flink.table.runtime.operators.aggregate.window;

import java.time.Duration;
import java.time.ZoneId;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

/** Reuses the released upstream window aggregate function without changing its implementation. */
public final class StreamFusionCanonicalWindowFixture {
  private StreamFusionCanonicalWindowFixture() {}

  public static OneInputStreamOperator<RowData, RowData> session(
      RowType input, PagedTypeSerializer<RowData> keys, long gap, ZoneId zone) {
    return WindowAggOperatorBuilder.builder()
        .inputSerializer(new RowDataSerializer(input))
        .keySerializer(keys)
        .shiftTimeZone(zone)
        .assigner(
            org.apache.flink.table.runtime.operators.window.tvf.unslicing.UnsliceAssigners.session(
                2, zone, Duration.ofMillis(gap)))
        .countStarIndex(-1)
        .aggregate(
            WindowAggOperatorTestBase.createGeneratedAggsHandle(
                new UnslicingWindowAggOperatorTest.UnslicingSumAndCountAggsFunction()),
            new RowDataSerializer(new BigIntType(), new BigIntType()))
        .build();
  }

  public static OneInputStreamOperator<RowData, RowData> tumbling(
      RowType input, PagedTypeSerializer<RowData> keys, long size, ZoneId zone) {
    var assigner = SliceAssigners.tumbling(2, zone, Duration.ofMillis(size));
    return WindowAggOperatorBuilder.builder()
        .inputSerializer(new RowDataSerializer(input))
        .keySerializer(keys)
        .shiftTimeZone(zone)
        .assigner(assigner)
        .countStarIndex(-1)
        .aggregate(
            WindowAggOperatorTestBase.createGeneratedAggsHandle(
                new SlicingWindowAggOperatorTest.SlicingSumAndCountAggsFunction(assigner)),
            new RowDataSerializer(new BigIntType(), new BigIntType()))
        .build();
  }
}

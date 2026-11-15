package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.aggregate.RecordCounter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class GroupAggregateAppendExtremaStockContractTest {
  static Stream<Arguments> cases() {
    return Stream.of("extrema", "mixed-extrema")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of(0L, 1000L)
                                .flatMap(
                                    ttl ->
                                        Stream.of(false, true)
                                            .map(
                                                global ->
                                                    Arguments.of(mode, rocks, ttl, global)))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void releasedAppendExtremaUseNullableScalarsWithoutRetractionViews(
      String mode, boolean rocks, long ttl, boolean global) throws Exception {
    try (var fixture =
        new GroupAggregateStockTestFixture(mode, rocks, ttl, true, true, false, global)) {
      assertEquals(
          RecordCounter.of(-1).getClass(),
          GroupAggregateStockTestFixture.field(fixture.function, "recordCounter").getClass());
      assertTrue(
          fixture.accumulatorType.getChildren().stream()
              .allMatch(type -> type.getTypeRoot() == LogicalTypeRoot.BIGINT));
      fixture.harness.open();
      Long[] input = {null, 7L, 4L, -11L, Long.MIN_VALUE, Long.MAX_VALUE};
      Long[] minimum = {null, 7L, 4L, -11L, Long.MIN_VALUE, Long.MIN_VALUE};
      Long[] maximum = {null, 7L, 7L, 7L, 7L, Long.MAX_VALUE};
      long[] sums = {0, 7, 11, 0, Long.MIN_VALUE, -1};
      for (int i = 0; i < input.length; i++) {
        long time = 1000 + i * 100;
        fixture.harness.setProcessingTime(time);
        fixture.harness.setStateTtlProcessingTime(time);
        fixture.harness.processElement(
            new StreamRecord<>(
                global ? GenericRowData.of(input[i]) : GenericRowData.of(1L, input[i])));
        Object raw = fixture.rawState().value();
        if (ttl > 0) {
          assertInstanceOf(TtlValue.class, raw);
          assertEquals(time, ((TtlValue<?>) raw).getLastAccessTimestamp());
          raw = ((TtlValue<?>) raw).getUserValue();
        }
        RowData accumulator = (RowData) raw;
        Long[] expected =
            mode.equals("extrema")
                ? new Long[] {minimum[i], maximum[i]}
                : new Long[] {(long) i + 1, (long) i, sums[i], minimum[i], maximum[i]};
        assertEquals(expected.length, accumulator.getArity());
        for (int field = 0; field < expected.length; field++) {
          assertEquals(expected[field] == null, accumulator.isNullAt(field), "field " + field);
          if (expected[field] != null)
            assertEquals(expected[field].longValue(), accumulator.getLong(field));
        }
      }
    }
  }
}

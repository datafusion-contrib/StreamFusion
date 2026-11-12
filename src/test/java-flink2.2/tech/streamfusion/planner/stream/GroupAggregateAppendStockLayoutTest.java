package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.CanonicalGroupArithmeticInitializationTest.*;

import java.util.ArrayList;
import java.util.stream.Stream;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.aggregate.RecordCounter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class GroupAggregateAppendStockLayoutTest {
  static Stream<Arguments> cases() {
    return Stream.of("arithmetic", "hidden", "count", "star")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        rocks -> Stream.of(0L, 1000L).map(ttl -> Arguments.of(mode, rocks, ttl))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void releasedAppendLayoutsOmitInputCountersAndRetainInitialization(
      String mode, boolean rocks, long ttl) throws Exception {
    try (var stock = new GroupAggregateStockTestFixture(mode, rocks, ttl, true, true)) {
      stock.harness.open();
      assertEquals(
          RecordCounter.of(-1).getClass(),
          GroupAggregateStockTestFixture.field(stock.function, "recordCounter").getClass());
      assertTrue(
          stock.accumulatorType.getChildren().stream()
              .allMatch(type -> type.is(LogicalTypeRoot.BIGINT)));
      long time = 1000;
      for (Long value : new Long[] {null, 7L, 4L, -11L}) {
        stockStep(stock, row(1L, value, 0), time);
        stock
            .harness
            .getOperator()
            .<RowData>getKeyedStateBackend()
            .setCurrentKey(stock.selector.getKey(row(1L, null, 0)));
        Object raw = ((ValueState<?>) stock.rawState()).value();
        if (ttl > 0) {
          assertEquals(time, ((TtlValue<?>) raw).getLastAccessTimestamp());
          raw = ((TtlValue<?>) raw).getUserValue();
        }
        RowData accumulator = (RowData) raw;
        assertNotNull(accumulator);
        var fields = new ArrayList<Long>();
        for (int i = 0; i < accumulator.getArity(); i++)
          fields.add(accumulator.isNullAt(i) ? null : accumulator.getLong(i));
        int step = (int) ((time - 1000) / 100);
        Long[][] expected =
            mode.equals("arithmetic")
                ? new Long[][] {{1L, 0L, 0L}, {2L, 1L, 7L}, {3L, 2L, 11L}, {4L, 3L, 0L}}
                : mode.equals("hidden")
                    ? new Long[][] {{null, 0L, 0L}, {7L, 7L, 1L}, {11L, 11L, 2L}, {0L, 0L, 3L}}
                    : mode.equals("count")
                        ? new Long[][] {{0L}, {1L}, {2L}, {3L}}
                        : new Long[][] {{1L}, {2L}, {3L}, {4L}};
        assertEquals(
            java.util.Arrays.asList(expected[step]), fields, "actual released physical slots");
        time += 100;
      }
      assertEquals(1, stock.rawStateBytes().size());
    }
  }
}

package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.CanonicalUnboundedOverScalarTest.raw;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalOverDistinctAsyncTest {
  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    CanonicalOverDistinctValueFixture.countTypes()
        .forEach(
            type -> {
              for (boolean rows : new boolean[] {false, true})
                for (boolean rocks : new boolean[] {false, true})
                  for (String key : new String[] {"scalar", "global", "row", "row-string"})
                    cases.add(Arguments.of(type, rows, rocks, key));
            });
    return tech.streamfusion.CanonicalTestMatrix.select(cases.stream());
  }

  @ParameterizedTest
  @MethodSource("cases")
  void foldsAndNullableMembershipsFinishAfterLiveChangesAndOwnerClose(
      String type, boolean rows, boolean rocks, String key) throws Exception {
    boolean represented = key.startsWith("row");
    boolean composite = key.equals("row-string");
    var plan =
        new CanonicalUnboundedOverScalarTest.Plan(
            type,
            rows,
            key.equals("global"),
            0,
            represented ? CanonicalRowKeyFixture.sql(true) : "BIGINT",
            composite,
            false,
            !CanonicalOverDistinctValueFixture.integral(type),
            true);
    var expected = new OperatorSubtaskState[2];
    var captures = new CanonicalOverSnapshotTestSupport.Capture[2];
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var reference = plan.stock(!rocks);
        var owner = plan.nativeHarness(rocks)) {
      try {
        reference.open();
        owner.open();
        var operator = (NativeOverAggregateOperator) owner.getOperator();
        assertEquals(rocks, operator.directRocksDBState());
        for (int stage = 0; stage < 3; stage++) {
          var input =
              List.of(
                  row(plan, represented, stage % 2 == 0, 1L, 7L),
                  row(plan, represented, stage % 2 != 0, 1L, (long) stage + 10),
                  row(plan, represented, false, 2L, null),
                  row(plan, represented, true, null, stage == 0 ? null : (long) stage),
                  row(plan, represented, true, 9L, stage == 2 ? 1L : null));
          assertEquals(plan.stockInput(reference, input), plan.nativeInput(owner, input));
          if (stage < 2) {
            expected[stage] = CanonicalOverSnapshotTestSupport.snapshot(reference, stage + 10, 0);
            captures[stage] = CanonicalOverSnapshotTestSupport.capture(owner, stage + 10);
          }
        }
        operator.closeHandle();
        operator.handle = 0;
        for (int stage = 1; stage >= 0; stage--) {
          var saved = captures[stage].finish();
          try (var stock = plan.stock(rocks);
              var returned = plan.stock(!rocks)) {
            stock.initializeState(expected[stage]);
            returned.initializeState(saved);
            stock.open();
            returned.open();
            assertEquals(raw(stock), raw(returned));
            var suffix =
                List.of(
                    row(plan, represented, false, 1L, 7L),
                    row(plan, represented, true, 1L, 20L),
                    row(plan, represented, false, 2L, null),
                    row(plan, represented, true, 2L, 3L),
                    row(plan, represented, false, null, 1L),
                    row(plan, represented, true, 9L, null),
                    row(plan, represented, false, 9L, 1L));
            assertEquals(plan.stockInput(stock, suffix), plan.stockInput(returned, suffix));
          } finally {
            saved.discardState();
          }
        }
      } finally {
        for (var capture : captures) if (capture != null) capture.close();
      }
    } finally {
      for (var state : expected) if (state != null) state.discardState();
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private static RowData row(
      CanonicalUnboundedOverScalarTest.Plan plan,
      boolean represented,
      boolean generic,
      Long key,
      Long value) {
    return represented
        ? CanonicalUnboundedOverRowKeyStateTest.row(
            plan, true, RowKind.UPDATE_AFTER, false, generic, key, value)
        : plan.row(key, value);
  }
}

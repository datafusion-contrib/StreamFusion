package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.CanonicalUnboundedOverScalarTest.raw;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalDecimalOverDistinctStateTest {
  private final List<OperatorSubtaskState> snapshots = new ArrayList<>();

  @AfterEach
  void releaseSnapshots() throws Exception {
    for (var saved : snapshots) saved.discardState();
  }

  static Stream<Arguments> cases() {
    return Stream.of(2, 20)
        .flatMap(
            scale ->
                Stream.of(false, true)
                    .flatMap(
                        stock ->
                            Stream.of(false, true)
                                .flatMap(
                                    nativeStore ->
                                        Stream.of(false, true)
                                            .map(
                                                first ->
                                                    Arguments.of(
                                                        scale, stock, nativeStore, first)))));
  }

  private OperatorSubtaskState snapshot(AbstractStreamOperatorTestHarness<?> harness)
      throws Exception {
    var saved = CanonicalOverSnapshotTestSupport.snapshot(harness, 1, 0);
    snapshots.add(saved);
    return saved;
  }

  @ParameterizedTest
  @MethodSource("cases")
  void nullableOverflowHistorySurvivesEmptyReturnsAndNativeRecovery(
      int scale, boolean stockStore, boolean nativeStore, boolean first) throws Exception {
    var plan =
        new CanonicalUnboundedOverScalarTest.Plan(
            "DECIMAL(38," + scale + ")", true, false, 0, "BIGINT", false, false, false, true);
    var maximum =
        decimal(
            plan, scale, new BigDecimal(BigInteger.TEN.pow(38).subtract(BigInteger.ONE), scale));
    var unit = decimal(plan, scale, BigDecimal.ONE.scaleByPowerOfTen(-scale));
    var prefix = List.of(maximum, unit, plan.row(1L, null), plan.row(2L, null));
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var reference = plan.stock(stockStore);
        var owner = plan.nativeHarness(nativeStore)) {
      reference.open();
      var expected = plan.stockInput(reference, prefix);
      if (first) {
        owner.open();
        assertEquals(expected, plan.nativeInput(owner, prefix));
      } else {
        owner.initializeState(snapshot(reference));
        owner.open();
      }
      try (var returned = plan.stock(!stockStore)) {
        returned.initializeState(snapshot(owner));
        returned.open();
        assertEquals(
            raw(reference), raw(returned), "Overflow NULL and view bits are raw stock state");
        var suffix = List.of(maximum, unit, plan.row(1L, 7L), plan.row(2L, 3L));
        expected = plan.stockInput(reference, suffix);
        assertEquals(expected, plan.stockInput(returned, suffix));
        assertEquals(expected, plan.nativeInput(owner, suffix));
      }
      owner.prepareSnapshotPreBarrier(2);
      var checkpoint = owner.snapshot(2, 0);
      snapshots.add(checkpoint);
      try (var recovered = plan.nativeHarness(nativeStore);
          var returned = plan.stock(stockStore)) {
        recovered.initializeState(checkpoint);
        recovered.open();
        returned.initializeState(snapshot(recovered));
        returned.open();
        assertEquals(
            raw(reference),
            raw(returned),
            "Native recovery retains SUM restart and latched AVG overflow");
        var suffix = List.of(unit, plan.row(1L, 8L), plan.row(2L, 3L), plan.row(2L, null));
        expected = plan.stockInput(reference, suffix);
        assertEquals(expected, plan.stockInput(returned, suffix));
        assertEquals(expected, plan.nativeInput(recovered, suffix));
      }
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private static RowData decimal(
      CanonicalUnboundedOverScalarTest.Plan plan, int scale, BigDecimal value) {
    var row = (GenericRowData) plan.row(1L, null);
    var decimal = DecimalData.fromBigDecimal(value, 38, scale);
    assertNotNull(decimal);
    row.setField(1, decimal);
    return row;
  }
}

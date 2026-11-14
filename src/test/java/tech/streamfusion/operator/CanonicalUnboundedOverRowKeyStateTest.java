package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.CanonicalUnboundedOverScalarTest.raw;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalUnboundedOverRowKeyStateTest {
  private final List<OperatorSubtaskState> snapshots = new ArrayList<>();

  @org.junit.jupiter.api.AfterEach
  void releaseSnapshots() throws Exception {
    for (var state : snapshots) state.discardState();
  }

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (boolean nested : new boolean[] {false, true})
      for (RowKind kind : RowKind.values())
        for (boolean padding : new boolean[] {false, true})
          for (boolean rocks : new boolean[] {false, true})
            for (boolean rows : new boolean[] {false, true})
              for (long ttl : new long[] {0, 1000})
                for (boolean extrema : new boolean[] {false, true})
                  for (boolean composite : new boolean[] {false, true})
                    cases.add(
                        Arguments.of(nested, kind, padding, rocks, rows, ttl, extrema, composite));
    return tech.streamfusion.CanonicalTestMatrix.select(cases.stream());
  }

  private OperatorSubtaskState snapshot(
      org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness<?> harness)
      throws Exception {
    harness.prepareSnapshotPreBarrier(1);
    var saved =
        harness
            .snapshotWithLocalState(
                1,
                0,
                org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                    org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
            .getJobManagerOwnedState();
    snapshots.add(saved);
    return saved;
  }

  @ParameterizedTest
  @MethodSource("cases")
  void originalRowKeysRetainRawFoldsAcrossLiveFormsAndNativeRecovery(
      boolean nested,
      RowKind kind,
      boolean padding,
      boolean rocks,
      boolean rows,
      long ttl,
      boolean extrema,
      boolean composite)
      throws Exception {
    var plan =
        new CanonicalUnboundedOverScalarTest.Plan(
            extrema ? "VARCHAR(100)" : "BIGINT",
            rows,
            false,
            ttl,
            CanonicalRowKeyFixture.sql(nested),
            composite,
            extrema);
    try (var reference = plan.stock(!rocks);
        var owner = plan.nativeHarness(rocks)) {
      reference.open();
      reference.setStateTtlProcessingTime(1000);
      var prefix =
          List.of(
              row(plan, nested, kind, padding, false, 1L, 7L),
              row(plan, nested, kind, padding, true, 2L, 3L),
              row(plan, nested, kind, padding, false, null, null));
      plan.stockInput(reference, prefix);
      owner.initializeState(snapshot(reference));
      owner.open();
      try (var returned = plan.stock(rocks)) {
        returned.initializeState(snapshot(owner));
        returned.open();
        assertEquals(raw(reference), raw(returned));
      }
      var suffix =
          List.of(
              row(plan, nested, kind, padding, true, 1L, 1L),
              row(plan, nested, kind, padding, false, 1L, -2L),
              row(plan, nested, kind, padding, false, 2L, 4L),
              row(plan, nested, kind, padding, true, null, 5L));
      reference.setStateTtlProcessingTime(1500);
      owner.setProcessingTime(1500);
      assertEquals(plan.stockInput(reference, suffix), plan.nativeInput(owner, suffix));
      owner.prepareSnapshotPreBarrier(3);
      var checkpoint = owner.snapshot(3, 1600);
      snapshots.add(checkpoint);
      try (var recovered = plan.nativeHarness(rocks);
          var returned = plan.stock(rocks)) {
        recovered.initializeState(checkpoint);
        recovered.open();
        returned.initializeState(snapshot(recovered));
        returned.open();
        assertEquals(raw(reference), raw(returned));
        for (long now : new long[] {1800, 2800}) {
          reference.setStateTtlProcessingTime(now);
          returned.setStateTtlProcessingTime(now);
          recovered.setProcessingTime(now);
          var future =
              List.of(
                  row(plan, nested, kind, padding, false, 1L, 2L),
                  row(plan, nested, kind, padding, true, 1L, null),
                  row(plan, nested, kind, padding, false, 2L, 8L),
                  row(plan, nested, kind, padding, true, null, -1L));
          var expected = plan.stockInput(reference, future);
          assertEquals(expected, plan.stockInput(returned, future));
          assertEquals(expected, plan.nativeInput(recovered, future));
        }
      }
    }
  }

  private static RowData row(
      CanonicalUnboundedOverScalarTest.Plan plan,
      boolean nested,
      RowKind kind,
      boolean padding,
      boolean generic,
      Long id,
      Long value) {
    var row = new GenericRowData(plan.input.getFieldCount());
    row.setField(
        0,
        generic
            ? CanonicalRowKeyFixture.logicalKey(nested, id, kind)
            : CanonicalRowKeyFixture.key(nested, id, kind, padding));
    row.setField(1, CanonicalOverValueFixture.number(plan.input.getTypeAt(1), value));
    if (plan.partitions.length > 1)
      row.setField(
          2, id == null ? null : org.apache.flink.table.data.StringData.fromString("雪-" + id));
    return row;
  }
}

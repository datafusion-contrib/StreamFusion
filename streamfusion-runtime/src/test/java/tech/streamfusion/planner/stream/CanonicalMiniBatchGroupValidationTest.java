package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.GroupAggregateStockTestFixture.field;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;

class CanonicalMiniBatchGroupValidationTest {
  static Stream<Arguments> cases() {
    return Stream.of("STRING", "DOUBLE")
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        global ->
                            Stream.of(false, true)
                                .flatMap(
                                    rocks ->
                                        Stream.of(
                                                "null-count",
                                                "null-avg-count",
                                                "late-null-avg-count",
                                                "wrong-row-kind",
                                                "foreign-state")
                                            .map(
                                                reason ->
                                                    Arguments.of(type, global, rocks, reason)))));
  }

  @ParameterizedTest(name = "{0},global={1},rocks={2},{3}")
  @MethodSource("cases")
  @SuppressWarnings("unchecked")
  void wholeSourceValidationLeavesSourceIntactAndDestinationEmptyThenAllowsRepair(
      String type, boolean global, boolean rocks, String reason) throws Exception {
    try (var fixture = new MiniBatchGroupStockFixture(type, global, rocks);
        var destination = fixture.nativeHarness()) {
      fixture.openStock(null);
      destination.open();
      int size = reason.startsWith("late-") ? 1025 : 1;
      var seeds = new ArrayList<RowData>();
      for (int i = 0; i < size; i++) seeds.add(fixture.seed((long) i, 7, "member", true));
      fixture.stockBundle(fixture.partials(seeds));
      var backend =
          (CheckpointableKeyedStateBackend<RowData>)
              (CheckpointableKeyedStateBackend<?>)
                  fixture.harness.getOperator().getKeyedStateBackend();
      var accumulator = (ValueState<RowData>) field(fixture.function, "accState");
      List<RowData> keys;
      try (var stream = backend.getKeys("accState", VoidNamespace.INSTANCE)) {
        keys =
            stream
                .map(backend.getKeySerializer()::copy)
                .collect(java.util.stream.Collectors.toList());
      }
      assertEquals(size, keys.size());
      backend.setCurrentKey(keys.get(keys.size() - 1));
      var serializer = new RowDataSerializer(fixture.accumulatorType);
      RowData original = serializer.copy(accumulator.value());
      if (reason.equals("foreign-state")) {
        backend
            .getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new ValueStateDescriptor<>("unexpected", LongSerializer.INSTANCE))
            .update(9L);
      } else {
        var invalid = new GenericRowData(original.getArity());
        for (int i = 0; i < original.getArity(); i++)
          invalid.setField(
              i,
              RowData.createFieldGetter(fixture.accumulatorType.getTypeAt(i), i)
                  .getFieldOrNull(original));
        if (reason.equals("wrong-row-kind")) invalid.setRowKind(RowKind.DELETE);
        else {
          int field =
              reason.equals("null-count")
                  ? ((int[]) field(fixture.contract, "scalarFields"))[0]
                  : ((int[]) field(fixture.contract, "countFields"))[1];
          invalid.setField(field, null);
        }
        accumulator.update(invalid);
      }
      var before = fixture.rawStateBytes();
      var owner = (MiniBatchGroupStockFixture.Direct) destination.getOperator();
      assertThrows(
          Exception.class,
          () -> fixture.contract.importState(backend, owner.address(), owner.persistent()));
      assertEquals(before, fixture.rawStateBytes());
      Native.exportGroupTypedCanonical(
          owner.address(),
          owner.persistent(),
          -1,
          (a, s) -> {
            throw new AssertionError("failed validation populated native state");
          });
      if (!reason.equals("foreign-state")) {
        backend.setCurrentKey(keys.get(keys.size() - 1));
        accumulator.update(original);
        var repaired = fixture.rawStateBytes();
        fixture.contract.importState(backend, owner.address(), owner.persistent());
        assertTrue(fixture.rawStateBytes().isEmpty());
        fixture.contract.exportState(backend, owner.address(), owner.persistent());
        assertEquals(repaired, fixture.rawStateBytes());
        var next = fixture.partials(List.of(fixture.seed(0L, 4, "other", false)));
        assertEquals(
            fixture.stockBundle(next),
            CanonicalMiniBatchGroupStateTest.nativeBundle(destination, fixture, next));
      }
    }
  }
}

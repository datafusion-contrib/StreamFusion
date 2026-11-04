package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.CanonicalGroupTypedLifecycleTest.*;
import static tech.streamfusion.planner.GroupAggregateStockTestFixture.field;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.data.*;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import tech.streamfusion.Native;

class CanonicalGroupTypedValidationTest {
  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            append ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of(
                                    "late-null-count",
                                    "wrong-row-kind",
                                    "orphan-view",
                                    "malformed-counters",
                                    "late-corrupt-view",
                                    "orphan-null-marker",
                                    "malformed-null-marker",
                                    "foreign-state",
                                    "missing-membership")
                                .filter(reason -> append || !reason.equals("missing-membership"))
                                .map(reason -> Arguments.of(append, rocks, reason))));
  }

  @ParameterizedTest(name = "append={0},rocks={1},{2}")
  @MethodSource("cases")
  @SuppressWarnings("unchecked")
  void invalidWholeSourceRetainsAllStockStateAndAnEmptyNativeTarget(
      boolean append, boolean rocks, String reason) throws Exception {
    try (var fixture = new TypedGroupStockFixture("INT", append, rocks);
        var destination = fixture.nativeHarness()) {
      fixture.harness.open();
      destination.open();
      int size = reason.startsWith("late-") ? 1025 : 1;
      for (int i = 0; i < size; i++)
        stockStep(fixture, row("INT", "key-" + i, 0, 7, "member", true, RowKind.INSERT));
      var backend = fixture.backend();
      var accumulator = (ValueState<RowData>) field(fixture.function, "accState");
      var views = (List<?>) field(fixture.contract, "views");
      Object view = views.get(0);
      String name = (String) field(view, "name");
      var map =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new MapStateDescriptor<Object, Object>(
                  name,
                  (TypeSerializer<Object>) field(view, "valueKeys"),
                  (TypeSerializer<Object>) field(view, "counters")));
      List<RowData> keys;
      try (var stream = backend.getKeys("accState", VoidNamespace.INSTANCE)) {
        keys =
            stream
                .map(backend.getKeySerializer()::copy)
                .collect(java.util.stream.Collectors.toList());
      }
      assertEquals(size, keys.size());
      backend.setCurrentKey(keys.get(keys.size() - 1));
      RowData original = new RowDataSerializer(fixture.accumulatorType).copy(accumulator.value());
      if (reason.equals("late-null-count") || reason.equals("wrong-row-kind")) {
        var invalid = new GenericRowData(original.getArity());
        for (int i = 0; i < original.getArity(); i++)
          invalid.setField(
              i,
              RowData.createFieldGetter(fixture.accumulatorType.getTypeAt(i), i)
                  .getFieldOrNull(original));
        if (reason.equals("late-null-count"))
          invalid.setField(((int[]) field(fixture.contract, "scalarFields"))[0], null);
        else invalid.setRowKind(RowKind.DELETE);
        accumulator.update(invalid);
      }
      if (reason.equals("orphan-view")) accumulator.clear();
      if (reason.equals("malformed-counters") || reason.equals("late-corrupt-view"))
        map.put(StringData.fromString("member"), append ? Long.MIN_VALUE : new long[] {1});
      if (reason.equals("orphan-null-marker") || reason.equals("malformed-null-marker")) {
        var marker =
            backend.getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new ValueStateDescriptor<Object>(
                    name + "_null_state", (TypeSerializer<Object>) field(view, "counters")));
        marker.update(append ? Long.MIN_VALUE : new long[] {1});
        if (reason.equals("orphan-null-marker")) {
          map.clear();
          accumulator.clear();
          marker.update(append ? 3L : new long[] {1, 1});
        }
      }
      if (reason.equals("missing-membership")) map.clear();
      if (reason.equals("foreign-state"))
        backend
            .getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new ValueStateDescriptor<>("unexpected", LongSerializer.INSTANCE))
            .update(9L);
      var before = fixture.rawStateBytes();
      var owner = (TypedGroupStockFixture.Direct) destination.getOperator();
      assertThrows(
          Exception.class,
          () -> fixture.contract.importState(backend, owner.address(), owner.persistent()),
          reason);
      assertEquals(before, fixture.rawStateBytes(), "failed validation retains every source entry");
      Native.exportGroupTypedCanonical(
          owner.address(),
          owner.persistent(),
          -1,
          (a, s) -> {
            throw new AssertionError("validation populated native state");
          });
      if (reason.equals("late-null-count") || reason.equals("wrong-row-kind")) {
        backend.setCurrentKey(keys.get(keys.size() - 1));
        accumulator.update(original);
        var repaired = fixture.rawStateBytes();
        fixture.contract.importState(backend, owner.address(), owner.persistent());
        assertTrue(fixture.rawStateBytes().isEmpty());
        fixture.contract.exportState(backend, owner.address(), owner.persistent());
        assertEquals(
            repaired,
            fixture.rawStateBytes(),
            "repaired source remains retryable with all shared-view bytes");
      }
    }
  }
}

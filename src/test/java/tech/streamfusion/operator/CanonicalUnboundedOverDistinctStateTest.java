package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.CanonicalUnboundedOverScalarTest.raw;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;

class CanonicalUnboundedOverDistinctStateTest {
  private final List<OperatorSubtaskState> snapshots = new ArrayList<>();

  @org.junit.jupiter.api.AfterEach
  void releaseSnapshots() throws Exception {
    for (var state : snapshots) state.discardState();
  }

  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(
        Stream.of("INT NOT NULL", "BIGINT NOT NULL")
            .flatMap(
                type ->
                    Stream.of(false, true)
                        .flatMap(
                            rows ->
                                Stream.of(false, true)
                                    .flatMap(
                                        global ->
                                            Stream.of(false, true)
                                                .flatMap(
                                                    rocks ->
                                                        Stream.of(false, true)
                                                            .map(
                                                                first ->
                                                                    Arguments.of(
                                                                        type, rows, global, rocks,
                                                                        first)))))));
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
  void scalarAndMembershipStateSurviveEmptyReturnsAndNativeRecovery(
      String type, boolean rows, boolean global, boolean rocks, boolean first) throws Exception {
    var plan =
        new CanonicalUnboundedOverScalarTest.Plan(
            type, rows, global, 0, "BIGINT", false, false, true, true);
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var reference = plan.stock(rocks);
        var owner = plan.nativeHarness(rocks)) {
      reference.open();
      var prefix =
          List.of(plan.row(1L, 7L), plan.row(1L, 7L), plan.row(2L, 9L), plan.row(null, 3L));
      var expected = plan.stockInput(reference, prefix);
      if (first) {
        owner.open();
        assertEquals(expected, plan.nativeInput(owner, prefix));
      } else {
        owner.initializeState(snapshot(reference));
        owner.open();
      }
      assertEquals(rocks, ((NativeOverAggregateOperator) owner.getOperator()).directRocksDBState());
      try (var returned = plan.stock(!rocks)) {
        returned.initializeState(snapshot(owner));
        returned.open();
        assertTrue(returned.extractOutputValues().isEmpty());
        assertEquals(raw(reference), raw(returned));
        var suffix =
            List.of(
                plan.row(1L, 7L),
                plan.row(1L, 8L),
                plan.row(2L, 9L),
                plan.row(2L, 10L),
                plan.row(null, 3L),
                plan.row(null, 4L));
        expected = plan.stockInput(reference, suffix);
        assertEquals(expected, plan.stockInput(returned, suffix));
        assertEquals(expected, plan.nativeInput(owner, suffix));
      }
      owner.prepareSnapshotPreBarrier(2);
      var checkpoint = owner.snapshot(2, 0);
      snapshots.add(checkpoint);
      try (var recovered = plan.nativeHarness(rocks);
          var returned = plan.stock(!rocks)) {
        recovered.initializeState(checkpoint);
        recovered.open();
        returned.initializeState(snapshot(recovered));
        returned.open();
        assertEquals(raw(reference), raw(returned));
        var future =
            List.of(
                plan.row(1L, 8L),
                plan.row(1L, 11L),
                plan.row(2L, 10L),
                plan.row(null, 4L),
                plan.row(null, 5L));
        expected = plan.stockInput(reference, future);
        assertEquals(expected, plan.stockInput(returned, future));
        assertEquals(expected, plan.nativeInput(recovered, future));
      }
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  static Stream<Arguments> malformedCases() {
    return Stream.of("bits", "count", "null", "orphan")
        .flatMap(mode -> Stream.of(false, true).map(rocks -> Arguments.of(mode, rocks)));
  }

  @ParameterizedTest
  @MethodSource("malformedCases")
  @SuppressWarnings("unchecked")
  void inconsistentMembershipsRejectBeforeEitherNativeStateChanges(String mode, boolean rocks)
      throws Exception {
    var plan =
        new CanonicalUnboundedOverScalarTest.Plan(
            "BIGINT NOT NULL", true, false, 0, "BIGINT", false, false, true, true);
    try (var source = plan.stock(rocks);
        var target = plan.nativeHarness(rocks)) {
      source.open();
      target.open();
      plan.stockInput(source, List.of(plan.row(1L, 7L), plan.row(1L, 8L)));
      var valid = snapshot(source);
      var backend =
          (CheckpointableKeyedStateBackend<RowData>) source.getOperator().getKeyedStateBackend();
      String view =
          plan.contract.stateDescriptors().entrySet().stream()
              .filter(
                  entry ->
                      entry.getValue()
                          == org.apache.flink.api.common.state.StateDescriptor.Type.MAP)
              .map(java.util.Map.Entry::getKey)
              .findFirst()
              .orElseThrow();
      org.apache.flink.api.common.typeutils.base.MapSerializer<Object, Object> serializers = null;
      var resources = backend.savepoint().getSnapshotResources();
      try (AutoCloseable release = resources::release) {
        for (var meta : resources.getMetaInfoSnapshots())
          if (meta.getName().equals(view))
            serializers =
                (org.apache.flink.api.common.typeutils.base.MapSerializer<Object, Object>)
                    new org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo<
                            Object, Object>(meta)
                        .getStateSerializer();
      }
      assertNotNull(serializers);
      var map =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new MapStateDescriptor<>(
                  view, serializers.getKeySerializer(), serializers.getValueSerializer()));
      var marker =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>(view + "_null_state", serializers.getValueSerializer()));
      backend.setCurrentKey(plan.selector.getKey(plan.row(1L, 7L)));
      if (mode.equals("bits")) map.put(7L, 2L);
      else if (mode.equals("count")) map.remove(7L);
      else if (mode.equals("null")) marker.update(1L);
      else {
        backend.setCurrentKey(plan.selector.getKey(plan.row(99L, 7L)));
        map.put(7L, 1L);
      }
      var invalid = raw(source);
      long handle = ((NativeOverAggregateOperator) target.getOperator()).handle;
      assertThrows(Exception.class, () -> plan.contract.importState(backend, handle, rocks));
      assertEquals(invalid, raw(source));
      var folds = new java.util.concurrent.atomic.AtomicInteger();
      Native.exportOverFoldCanonical(
          handle,
          (arrayAddress, schemaAddress) -> {
            try (var array = org.apache.arrow.c.ArrowArray.wrap(arrayAddress);
                var schema = org.apache.arrow.c.ArrowSchema.wrap(schemaAddress);
                var root =
                    org.apache.arrow.c.Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              folds.addAndGet(root.getRowCount());
            }
          });
      assertEquals(0, folds.get());
      try (var repaired = plan.stock(rocks)) {
        repaired.initializeState(valid);
        repaired.open();
        var repairedBackend =
            (CheckpointableKeyedStateBackend<RowData>)
                repaired.getOperator().getKeyedStateBackend();
        plan.contract.importState(repairedBackend, handle, rocks);
        try (var reference = plan.stock(!rocks)) {
          reference.initializeState(valid);
          reference.open();
          var future = List.of(plan.row(1L, 7L), plan.row(1L, 11L));
          assertEquals(plan.stockInput(reference, future), plan.nativeInput(target, future));
        }
      }
    }
  }
}

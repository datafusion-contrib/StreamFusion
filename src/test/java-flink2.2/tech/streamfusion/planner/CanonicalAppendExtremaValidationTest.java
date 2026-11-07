package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.CanonicalGroupArithmeticInitializationTest.*;

import java.nio.file.Path;
import java.util.List;
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
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.NativeMemoryLimitException;
import tech.streamfusion.state.StockGroupArithmeticStateContract;

class CanonicalAppendExtremaValidationTest {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  static Stream<Arguments> modes() {
    return Stream.of("extrema", "mixed-extrema")
        .flatMap(mode -> Stream.of(false, true).map(rocks -> Arguments.of(mode, rocks)));
  }

  static Stream<Arguments> invalidSources() {
    return modes()
        .flatMap(
            args ->
                Stream.of("late-null-count", "foreign-state", "wrong-row-kind")
                    .filter(
                        reason ->
                            !reason.equals("late-null-count")
                                || args.get()[0].equals("mixed-extrema"))
                    .map(reason -> Arguments.of(args.get()[0], args.get()[1], reason)));
  }

  static GenericRowData accumulator(String mode) {
    return mode.equals("extrema")
        ? GenericRowData.of(7L, 7L)
        : GenericRowData.of(1L, 1L, 7L, 7L, 7L);
  }

  @ParameterizedTest
  @MethodSource("invalidSources")
  @SuppressWarnings("unchecked")
  void invalidLateEntryRetainsSourceAndLeavesDestinationEmpty(
      String mode, boolean rocks, String reason) throws Exception {
    try (var source = new GroupAggregateStockTestFixture(mode, rocks, 0, true, true);
        var destination = nativeHarness(source, rocks)) {
      source.harness.open();
      destination.open();
      var backend =
          (CheckpointableKeyedStateBackend<RowData>)
              source.harness.getOperator().<RowData>getKeyedStateBackend();
      var state = (ValueState<RowData>) source.rawState();
      int count = reason.equals("foreign-state") ? 1 : 1025;
      for (int i = 0; i < count; i++) {
        backend.setCurrentKey(
            backend.getKeySerializer().copy(source.selector.getKey(row((long) i, 7L, 0))));
        state.update(accumulator(mode));
      }
      List<RowData> keys;
      try (var stream = backend.getKeys("accState", VoidNamespace.INSTANCE)) {
        keys =
            stream
                .map(backend.getKeySerializer()::copy)
                .collect(java.util.stream.Collectors.toList());
      }
      var badKey = keys.get(keys.size() - 1);
      backend.setCurrentKey(badKey);
      var invalid = accumulator(mode);
      if (reason.equals("late-null-count")) invalid.setField(0, null);
      if (reason.equals("wrong-row-kind")) invalid.setRowKind(RowKind.DELETE);
      if (reason.equals("foreign-state")) {
        backend
            .getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new ValueStateDescriptor<>("foreign", LongSerializer.INSTANCE))
            .update(99L);
      } else state.update(invalid);
      var before = source.rawStateBytes();
      var owner = (DirectGroup) destination.getOperator();
      assertThrows(
          Exception.class,
          () -> source.contract().importState(backend, owner.address(), owner.persistent()));
      assertEquals(before, source.rawStateBytes());
      Native.exportGroupArithmeticCanonical(
          owner.address(),
          owner.persistent(),
          (array, schema) -> {
            throw new AssertionError("invalid source populated native state");
          });
      if (!reason.equals("foreign-state")) {
        backend.setCurrentKey(badKey);
        state.update(accumulator(mode));
        var repaired = source.rawStateBytes();
        source.contract().importState(backend, owner.address(), owner.persistent());
        assertTrue(source.rawStateBytes().isEmpty());
        try (var projected = new GroupAggregateStockTestFixture(mode, rocks, 0, true, true)) {
          projected.harness.initializeState(snapshot(destination));
          projected.harness.open();
          assertEquals(repaired, projected.rawStateBytes());
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("modes")
  @SuppressWarnings("unchecked")
  void deniedImportPreservesCompleteSourceForFreshRetry(String mode, boolean rocks)
      throws Exception {
    String handles = Native.liveNativeHandles();
    var factory = new CanonicalGroupArithmeticValidationTest();
    factory.directory = directory;
    try (var source = new GroupAggregateStockTestFixture(mode, rocks, 0, true, true)) {
      source.harness.open();
      var backend =
          (CheckpointableKeyedStateBackend<RowData>)
              source.harness.getOperator().<RowData>getKeyedStateBackend();
      var state = (ValueState<RowData>) source.rawState();
      for (int i = 0; i < 1025; i++) {
        backend.setCurrentKey(
            backend.getKeySerializer().copy(source.selector.getKey(row((long) i, 7L, 0))));
        state.update(accumulator(mode));
      }
      var expected = source.rawStateBytes();
      assertEquals(1025, expected.size());
      long denied = factory.create(source, rocks, 64, "denied");
      try {
        assertThrows(
            NativeMemoryLimitException.class,
            () -> source.contract().importState(backend, denied, rocks));
        assertEquals(expected, source.rawStateBytes());
      } finally {
        CanonicalGroupArithmeticValidationTest.close(denied, rocks);
      }
      assertEquals(handles, Native.liveNativeHandles());
      long retry = factory.create(source, rocks, 16_000_000, "retry");
      try {
        source.contract().importState(backend, retry, rocks);
        assertTrue(source.rawStateBytes().isEmpty());
        source.contract().exportState(backend, retry, rocks);
        assertEquals(expected, source.rawStateBytes());
      } finally {
        CanonicalGroupArithmeticValidationTest.close(retry, rocks);
      }
    }
    assertEquals(handles, Native.liveNativeHandles());
  }

  @ParameterizedTest
  @MethodSource("modes")
  void receiverExceptionLeavesNullableScalarsAndWriteAgesIntact(String mode, boolean rocks)
      throws Exception {
    try (var reference = new GroupAggregateStockTestFixture(mode, rocks, 1000, true, true);
        var nativeHarness = nativeHarness(reference, rocks)) {
      reference.harness.open();
      nativeHarness.open();
      assertEquals(
          stockStep(reference, row(1L, null, 0), 1000),
          nativeStep(nativeHarness, row(1L, null, 0), 1000, reference.outputType));
      assertEquals(
          stockStep(reference, row(2L, 7L, 0), 1700),
          nativeStep(nativeHarness, row(2L, 7L, 0), 1700, reference.outputType));
      var owner = (DirectGroup) nativeHarness.getOperator();
      var sentinel = new IllegalArgumentException("MIN/MAX receiver sentinel");
      assertSame(
          sentinel,
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  Native.exportGroupArithmeticCanonical(
                      owner.address(),
                      owner.persistent(),
                      (array, schema) -> {
                        throw sentinel;
                      })));
      try (var restored = new GroupAggregateStockTestFixture(mode, rocks, 1000, true, true)) {
        restored.harness.initializeState(snapshot(nativeHarness));
        restored.harness.open();
        assertEquals(reference.rawStateBytes(), restored.rawStateBytes());
        var continued = stockStep(reference, row(1L, -11L, 0), 2300);
        assertEquals(continued, stockStep(restored, row(1L, -11L, 0), 2300));
        assertEquals(
            continued, nativeStep(nativeHarness, row(1L, -11L, 0), 2300, reference.outputType));
      }
      assertEquals(
          stockStep(reference, row(2L, 4L, 0), 2400),
          nativeStep(nativeHarness, row(2L, 4L, 0), 2400, reference.outputType));
    }
  }

  static Stream<Arguments> invalidContracts() {
    return Stream.of(10, 11)
        .flatMap(kind -> Stream.of(false, true).map(retract -> Arguments.of(kind, retract)));
  }

  @ParameterizedTest
  @MethodSource("invalidContracts")
  void scalarExtremaCannotMasqueradeAsRetractingAccumulators(int kind, boolean retract) {
    var keys = RowType.of(new BigIntType());
    var fields = RowType.of(new BigIntType(), new BigIntType());
    var failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new StockGroupArithmeticStateContract(
                    new RowDataSerializer(keys),
                    keys,
                    fields,
                    new int[] {kind},
                    new int[] {0},
                    new int[] {retract ? -1 : 1},
                    retract ? 1 : -1,
                    false));
    assertTrue(failure.getMessage().contains(retract ? "append-only" : "separate physical count"));
  }
}

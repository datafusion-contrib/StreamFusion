package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.CanonicalGroupArithmeticInitializationTest.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.Native;
import tech.streamfusion.operator.NativeAllocator;

class CanonicalGroupArithmeticValidationTest {
  @org.junit.jupiter.api.io.TempDir(cleanup = org.junit.jupiter.api.io.CleanupMode.ON_SUCCESS)
  java.nio.file.Path directory;

  static Stream<Arguments> invalidSources() {
    return Stream.of(false, true)
        .flatMap(
            rocks ->
                Stream.of("late-null-count", "foreign-state", "wrong-row-kind")
                    .map(reason -> Arguments.of(rocks, reason)));
  }

  @ParameterizedTest
  @MethodSource("invalidSources")
  @SuppressWarnings("unchecked")
  void validationRetainsTheEntireSourceAndAnEmptyTarget(boolean rocks, String reason)
      throws Exception {
    try (var source = new GroupAggregateStockTestFixture("star", rocks, 0, true);
        var destination = nativeHarness(source, rocks)) {
      source.harness.open();
      destination.open();
      var backend =
          (CheckpointableKeyedStateBackend<RowData>)
              source.harness.getOperator().<RowData>getKeyedStateBackend();
      var state = (ValueState<RowData>) source.rawState();
      int count = reason.equals("late-null-count") ? 1025 : 1;
      for (int i = 0; i < count; i++) {
        backend.setCurrentKey(
            backend.getKeySerializer().copy(source.selector.getKey(row((long) i, 7L, 0))));
        state.update(GenericRowData.of(1L));
      }
      List<RowData> keys;
      try (var stream = backend.getKeys("accState", VoidNamespace.INSTANCE)) {
        keys =
            stream
                .map(backend.getKeySerializer()::copy)
                .collect(java.util.stream.Collectors.toList());
      }
      RowData badKey = keys.get(keys.size() - 1);
      backend.setCurrentKey(badKey);
      if (reason.equals("late-null-count")) state.update(GenericRowData.of((Object) null));
      if (reason.equals("wrong-row-kind")) {
        var invalid = GenericRowData.of(1L);
        invalid.setRowKind(RowKind.DELETE);
        state.update(invalid);
      }
      if (reason.equals("foreign-state"))
        backend
            .getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new ValueStateDescriptor<>("foreign", LongSerializer.INSTANCE))
            .update(99L);
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
            throw new AssertionError("failed validation populated native state");
          });
      if (!reason.equals("foreign-state")) {
        backend.setCurrentKey(badKey);
        state.update(GenericRowData.of(1L));
        var repaired = source.rawStateBytes();
        source.contract().importState(backend, owner.address(), owner.persistent());
        assertTrue(source.rawStateBytes().isEmpty(), "source consumed only after success");
        try (var projected = new GroupAggregateStockTestFixture("star", rocks, 0, true)) {
          projected.harness.initializeState(snapshot(destination));
          projected.harness.open();
          assertEquals(repaired, projected.rawStateBytes());
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("unchecked")
  void deniedImportKeepsTheSourceForAFreshDestination(boolean persistent) throws Exception {
    String handles = Native.liveNativeHandles();
    try (var source = new GroupAggregateStockTestFixture("star", persistent, 0, true)) {
      source.harness.open();
      var backend =
          (CheckpointableKeyedStateBackend<RowData>)
              source.harness.getOperator().<RowData>getKeyedStateBackend();
      var state = (ValueState<RowData>) source.rawState();
      for (int i = 0; i < 1025; i++) {
        backend.setCurrentKey(
            backend.getKeySerializer().copy(source.selector.getKey(row((long) i, 7L, 0))));
        state.update(GenericRowData.of(1L));
      }
      var expected = source.rawStateBytes();
      assertEquals(1025, expected.size(), "nonempty complete stock source");
      long denied = create(source, persistent, 64, "denied");
      try {
        assertThrows(
            tech.streamfusion.NativeMemoryLimitException.class,
            () -> source.contract().importState(backend, denied, persistent));
        assertEquals(expected, source.rawStateBytes());
      } finally {
        close(denied, persistent);
      }
      assertEquals(handles, Native.liveNativeHandles());
      long retry = create(source, persistent, 1_000_000, "retry");
      try {
        source.contract().importState(backend, retry, persistent);
        assertTrue(source.rawStateBytes().isEmpty());
        source.contract().exportState(backend, retry, persistent);
        assertEquals(expected, source.rawStateBytes());
      } finally {
        close(retry, persistent);
      }
    }
    assertEquals(handles, Native.liveNativeHandles());
  }

  private long create(
      GroupAggregateStockTestFixture fixture, boolean persistent, long budget, String name) {
    int[] empty = new int[0],
        key = new int[] {0},
        descriptor = new int[] {Native.STOCK_KEY_PROJECTION, -1};
    if (!persistent)
      return Native.createGroupAggregator(
          fixture.kinds,
          new int[fixture.kinds.length],
          fixture.valueColumns,
          key,
          descriptor,
          empty,
          empty,
          empty,
          -1,
          true,
          false,
          0,
          budget);
    return Native.createRocksDBGroupAggregator(
        fixture.kinds,
        new int[fixture.kinds.length],
        fixture.valueColumns,
        key,
        descriptor,
        empty,
        empty,
        empty,
        -1,
        true,
        false,
        0,
        0,
        budget,
        directory.resolve(name).toString(),
        128,
        tech.streamfusion.state.FlinkRocksDBOptions.from(
                new org.apache.flink.configuration.Configuration())
            .json(),
        0,
        new String[0],
        new String[0],
        0,
        127,
        true,
        new byte[0][]);
  }

  private static void close(long handle, boolean persistent) {
    if (persistent) Native.closeRocksDBGroupAggregator(handle);
    else Native.closeGroupAggregator(handle);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void receiverFailurePreservesItsExceptionAndLiveNativeState(boolean rocks) throws Exception {
    try (var fixture = new GroupAggregateStockTestFixture("hidden", rocks, 1000, true);
        var nativeHarness = nativeHarness(fixture, rocks)) {
      nativeHarness.open();
      nativeStep(nativeHarness, row(1L, 7L, 0), 1000, fixture.outputType);
      var owner = (DirectGroup) nativeHarness.getOperator();
      var failure = new IllegalArgumentException("receiver sentinel");
      var observed =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  Native.exportGroupArithmeticCanonical(
                      owner.address(),
                      owner.persistent(),
                      (array, schema) -> {
                        throw failure;
                      }));
      assertSame(failure, observed);
      var rows = new AtomicInteger();
      Native.exportGroupArithmeticCanonical(
          owner.address(),
          owner.persistent(),
          (arrayAddress, schemaAddress) -> {
            try (var array = ArrowArray.wrap(arrayAddress);
                var schema = ArrowSchema.wrap(schemaAddress);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              rows.addAndGet(root.getRowCount());
            }
          });
      assertEquals(1, rows.get());
      try (var restored = new GroupAggregateStockTestFixture("hidden", rocks, 1000, true)) {
        restored.harness.initializeState(snapshot(nativeHarness));
        restored.harness.open();
        assertFalse(restored.rawStateBytes().isEmpty());
      }
    }
  }
}

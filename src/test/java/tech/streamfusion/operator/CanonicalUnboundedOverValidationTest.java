package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.CanonicalUnboundedOverScalarTest.*;

import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.ttl.TtlStateFactory.TtlSerializer;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.NativeMemoryLimitException;

class CanonicalUnboundedOverValidationTest {
  static Stream<Arguments> modes() {
    return Stream.of(false, true)
        .flatMap(
            rows ->
                Stream.of(false, true)
                    .flatMap(
                        rocks -> Stream.of(0L, 1000L).map(ttl -> Arguments.of(rows, rocks, ttl))));
  }

  static Stream<Arguments> invalid() {
    return modes()
        .flatMap(
            args ->
                Stream.of("late-count", "late-value", "header-kind", "key-kind", "foreign")
                    .map(
                        reason ->
                            Arguments.of(args.get()[0], args.get()[1], args.get()[2], reason)));
  }

  @ParameterizedTest
  @MethodSource("invalid")
  void malformedWholeSourceLeavesTargetEmptyAndSourceUnchanged(
      boolean rows, boolean rocks, long ttl, String reason) throws Exception {
    var plan = new Plan("BIGINT", rows, false, ttl);
    String handles = Native.liveNativeHandles();
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var source = new Projection(plan, rocks)) {
      for (int key = 0; key < 1025; key++) source.put(key, header(plan), 1000);
      List<RowData> keys;
      try (var stream = source.backend.getKeys("accState", VoidNamespace.INSTANCE)) {
        keys =
            stream
                .map(source.backend.getKeySerializer()::copy)
                .collect(java.util.stream.Collectors.toList());
      }
      var last = keys.get(keys.size() - 1);
      source.backend.setCurrentKey(last);
      if (reason.equals("foreign")) {
        source
            .backend
            .getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new ValueStateDescriptor<>("foreign", LongSerializer.INSTANCE))
            .update(99L);
      } else if (reason.equals("key-kind")) {
        source.state.clear();
        last.setRowKind(RowKind.DELETE);
        source.backend.setCurrentKey(last);
        source.state.update(source.wrap(header(plan), 1000));
      } else {
        var bad = header(plan);
        if (reason.equals("header-kind")) bad.setRowKind(RowKind.DELETE);
        else {
          int slot = 0;
          for (int kind : plan.kinds) {
            if (reason.equals("late-count") ? kind == 3 : kind == 4 || kind == 22) {
              bad.setField(slot, null);
              break;
            }
            slot += kind == 4 ? 2 : 1;
          }
        }
        source.state.update(source.wrap(bad, 1000));
      }
      var expected = raw(source.harness);
      long target = create(plan, 16_000_000);
      try {
        assertThrows(
            Exception.class, () -> plan.contract.importState(source.backend, target, false));
        assertEquals(expected, raw(source.harness));
        Native.exportOverFoldCanonical(
            target,
            (array, schema) -> {
              throw new AssertionError("invalid source installed state");
            });
      } finally {
        Native.closeOverAggregator(target);
      }
    }
    assertEquals(handles, Native.liveNativeHandles());
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @ParameterizedTest
  @MethodSource("modes")
  void memoryDenialPreservesAllChunksForFreshRetry(boolean rows, boolean rocks, long ttl)
      throws Exception {
    var plan = new Plan("BIGINT", rows, false, ttl);
    String handles = Native.liveNativeHandles();
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var source = new Projection(plan, rocks)) {
      for (int key = 0; key < 1025; key++) source.put(key, header(plan), 1000 + key);
      var expected = raw(source.harness);
      assertEquals(1025, expected.size());
      long denied = create(plan, 64);
      try {
        assertThrows(
            NativeMemoryLimitException.class,
            () -> plan.contract.importState(source.backend, denied, false));
        assertEquals(expected, raw(source.harness));
      } finally {
        Native.closeOverAggregator(denied);
      }
      long retry = create(plan, 16_000_000);
      try {
        plan.contract.importState(source.backend, retry, false);
        assertTrue(raw(source.harness).isEmpty());
        plan.contract.exportState(source.backend, retry, false);
        assertEquals(expected, raw(source.harness));
      } finally {
        Native.closeOverAggregator(retry);
      }
    }
    assertEquals(handles, Native.liveNativeHandles());
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @ParameterizedTest
  @MethodSource("modes")
  void populatedTargetIsRejectedWithoutChangingEitherSide(boolean rows, boolean rocks, long ttl)
      throws Exception {
    var plan = new Plan("BIGINT", rows, false, ttl);
    try (var source = new Projection(plan, rocks);
        var target = plan.nativeHarness(rocks);
        var stock = plan.stock(rocks)) {
      source.put(2, header(plan), 1000);
      var original = raw(source.harness);
      target.open();
      stock.open();
      time(target, 100);
      time(stock, 100);
      var prefix = List.of(plan.row(1L, 7L));
      assertEquals(plan.stockInput(stock, prefix), plan.nativeInput(target, prefix));
      var owner = (NativeOverAggregateOperator) target.getOperator();
      assertThrows(
          Exception.class, () -> plan.contract.importState(source.backend, owner.handle, rocks));
      assertEquals(original, raw(source.harness));
      var suffix = List.of(plan.row(1L, 3L));
      time(target, 500);
      time(stock, 500);
      assertEquals(plan.stockInput(stock, suffix), plan.nativeInput(target, suffix));
    }
  }

  static Stream<Arguments> receivers() {
    return modes()
        .flatMap(
            args ->
                Stream.of(false, true)
                    .map(
                        consumed ->
                            Arguments.of(args.get()[0], args.get()[1], args.get()[2], consumed)));
  }

  @ParameterizedTest
  @MethodSource("receivers")
  void receiverFailureKeepsOriginalExceptionStateAndAges(
      boolean rows, boolean rocks, long ttl, boolean consumed) throws Exception {
    var plan = new Plan("BIGINT", rows, false, ttl);
    String handles = Native.liveNativeHandles();
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var target = plan.nativeHarness(rocks);
        var stock = plan.stock(rocks)) {
      target.open();
      stock.open();
      time(target, 100);
      time(stock, 100);
      var prefix = List.of(plan.row(1L, 7L), plan.row(null, null));
      assertEquals(plan.stockInput(stock, prefix), plan.nativeInput(target, prefix));
      var owner = (NativeOverAggregateOperator) target.getOperator();
      var sentinel = new IllegalArgumentException("unbounded OVER receiver sentinel");
      assertSame(
          sentinel,
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  Native.exportOverFoldCanonical(
                      owner.handle,
                      (array, schema) -> {
                        if (consumed)
                          try (var a = ArrowArray.wrap(array);
                              var s = ArrowSchema.wrap(schema);
                              var root =
                                  Data.importVectorSchemaRoot(
                                      NativeAllocator.SHARED, a, s, NativeAllocator.DICTIONARIES)) {
                            assertEquals(2, root.getRowCount());
                          }
                        throw sentinel;
                      })));
      try (var returned = plan.stock(rocks)) {
        returned.initializeState(snapshot(target));
        returned.open();
        assertEquals(raw(stock), raw(returned));
      }
      time(target, 500);
      time(stock, 500);
      var suffix = List.of(plan.row(1L, 3L), plan.row(null, 2L));
      assertEquals(plan.stockInput(stock, suffix), plan.nativeInput(target, suffix));
    }
    assertEquals(handles, Native.liveNativeHandles());
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private static long create(Plan plan, long budget) {
    return Native.createOverAggregator(
        plan.codes,
        plan.kinds,
        plan.timeColumn,
        plan.columns,
        plan.partitions,
        plan.rows ? 3 : 0,
        0,
        true,
        plan.ttl,
        budget);
  }

  private static GenericRowData header(Plan plan) {
    var row = new GenericRowData(plan.accumulator.getFieldCount());
    for (int field = 0; field < row.getArity(); field++) {
      Object value = 1L;
      if (plan.accumulator.getTypeAt(field).is(LogicalTypeRoot.FLOAT)) value = 1F;
      else if (plan.accumulator.getTypeAt(field).is(LogicalTypeRoot.DOUBLE)) value = 1D;
      row.setField(field, value);
    }
    return row;
  }

  private static final class Projection implements AutoCloseable {
    final Plan plan;
    final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness;
    final CheckpointableKeyedStateBackend<RowData> backend;
    final ValueState<Object> state;

    @SuppressWarnings({"unchecked", "rawtypes"})
    Projection(Plan plan, boolean rocks) throws Exception {
      this.plan = plan;
      var operator =
          new KeyedProcessOperator<RowData, RowData, RowData>(
              new KeyedProcessFunction<RowData, RowData, RowData>() {
                @Override
                public void processElement(
                    RowData row, Context context, Collector<RowData> output) {}
              });
      harness =
          new KeyedOneInputStreamOperatorTestHarness<>(
              operator, plan.selector, plan.selector.getProducedType(), 128, 1, 0);
      harness.setStateBackend(
          rocks
              ? tech.streamfusion.compat.CanonicalTestCompat.rocksBackend()
              : new HashMapStateBackend());
      harness.setup(new RowDataSerializer(plan.output));
      harness.open();
      backend = (CheckpointableKeyedStateBackend<RowData>) operator.<RowData>getKeyedStateBackend();
      TypeSerializer<?> serializer = new RowDataSerializer(plan.accumulator);
      if (plan.ttl > 0) serializer = new TtlSerializer(LongSerializer.INSTANCE, serializer);
      state =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>("accState", (TypeSerializer<Object>) serializer));
    }

    Object wrap(RowData row, long age) {
      return plan.ttl > 0 ? new TtlValue<>(row, age) : row;
    }

    void put(long key, RowData row, long age) throws Exception {
      backend.setCurrentKey(plan.selector.getKey(plan.row(key, 1L)));
      state.update(wrap(row, age));
    }

    @Override
    public void close() throws Exception {
      harness.close();
    }
  }
}

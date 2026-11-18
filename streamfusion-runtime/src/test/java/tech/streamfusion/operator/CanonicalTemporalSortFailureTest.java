package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.CanonicalTemporalSortInteropTest.*;

import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.NativeMemoryLimitException;

class CanonicalTemporalSortFailureTest {
  static Stream<Arguments> modes() {
    return Stream.of(false, true)
        .flatMap(store -> Stream.of(1, 128).map(maximum -> Arguments.of(store, maximum)));
  }

  static Stream<Arguments> populated() {
    return modes()
        .flatMap(
            args ->
                Stream.of(false, true)
                    .flatMap(
                        target ->
                            Stream.of(false, true)
                                .map(
                                    cutoff ->
                                        Arguments.of(
                                            args.get()[0], args.get()[1], target, cutoff))));
  }

  @ParameterizedTest
  @MethodSource("populated")
  void pendingOrCutoffOnlyTargetRejectsWithoutChangingEitherSide(
      boolean sourceStore, int maximum, boolean targetStore, boolean cutoff) throws Exception {
    var plan = new Plan("long", maximum);
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    String handles = Native.liveNativeHandles();
    try (var source = plan.stock(sourceStore);
        var target = plan.nativeHarness(targetStore)) {
      source.open();
      target.open();
      plan.pushStock(source, plan.row(3, 300));
      plan.push(target, plan.row(1, 100));
      if (cutoff) {
        target.processWatermark(new Watermark(100));
        assertEquals(List.of(plan.encode(plan.row(1, 100))), plan.nativeOutput(target));
      }
      try (var projection = projection(plan, sourceStore, snapshot(source))) {
        var beforeSource = plan.digest(snapshot(projection));
        var beforeTarget = plan.digest(snapshot(target));
        var backend =
            (CheckpointableKeyedStateBackend<RowData>)
                projection.getOperator().<RowData>getKeyedStateBackend();
        long handle = ((NativeColumnarTemporalSortOperator) target.getOperator()).handle;
        assertThrows(
            IllegalStateException.class,
            () -> plan.contract().importState(backend, handle, targetStore));
        assertEquals(beforeSource, plan.digest(snapshot(projection)));
        assertEquals(beforeTarget, plan.digest(snapshot(target)));
        plan.push(target, plan.row(2, 200));
        target.processWatermark(new Watermark(300));
        assertEquals(
            cutoff
                ? List.of(plan.encode(plan.row(2, 200)))
                : List.of(plan.encode(plan.row(1, 100)), plan.encode(plan.row(2, 200))),
            plan.nativeOutput(target));
      }
    }
    assertEquals(handles, Native.liveNativeHandles());
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @ParameterizedTest
  @MethodSource("modes")
  void deniedMultiChunkImportKeepsSourceForFreshRetry(boolean sourceStore, int maximum)
      throws Exception {
    var plan = new Plan("long", maximum);
    String handles = Native.liveNativeHandles();
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var source = plan.stock(sourceStore)) {
      source.open();
      for (int id = 0; id < 2050; id++) plan.pushStock(source, plan.row(id, 100 + id % 7));
      try (var projection = projection(plan, sourceStore, snapshot(source))) {
        var original = CanonicalUnboundedOverScalarTest.raw(projection);
        var backend =
            (CheckpointableKeyedStateBackend<RowData>)
                projection.getOperator().<RowData>getKeyedStateBackend();
        var contract = plan.contract();
        long denied = Native.createTemporalSorter(2, 64);
        try {
          assertThrows(
              NativeMemoryLimitException.class, () -> contract.importState(backend, denied, false));
          assertEquals(original, CanonicalUnboundedOverScalarTest.raw(projection));
        } finally {
          Native.closeTemporalSorter(denied);
        }
        long retry = Native.createTemporalSorter(2, 16_000_000);
        try {
          contract.importState(backend, retry, false);
          assertTrue(CanonicalUnboundedOverScalarTest.raw(projection).isEmpty());
          contract.exportState(backend, retry, false);
          assertEquals(original, CanonicalUnboundedOverScalarTest.raw(projection));
        } finally {
          Native.closeTemporalSorter(retry);
        }
        try (var returned = plan.stock(sourceStore)) {
          returned.initializeState(snapshot(projection));
          returned.open();
          source.processWatermark(new Watermark(400));
          returned.processWatermark(new Watermark(400));
          var expected = plan.stockOutput(source);
          assertEquals(2050, expected.size());
          assertEquals(expected, plan.stockOutput(returned));
        }
      }
    }
    assertEquals(handles, Native.liveNativeHandles());
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  static Stream<Arguments> receivers() {
    return modes()
        .flatMap(
            args ->
                Stream.of(false, true)
                    .map(consumed -> Arguments.of(args.get()[0], args.get()[1], consumed)));
  }

  @ParameterizedTest
  @MethodSource("receivers")
  void receiverFailureBeforeOrAfterImportRetainsOriginalExceptionAndState(
      boolean nativeStore, int maximum, boolean consumed) throws Exception {
    var plan = new Plan("long", maximum);
    String handles = Native.liveNativeHandles();
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var target = plan.nativeHarness(nativeStore)) {
      target.open();
      plan.push(target, plan.row(1, 100));
      var expected = plan.digest(snapshot(target));
      var sentinel = new IllegalArgumentException("temporal sort receiver sentinel");
      long handle = ((NativeColumnarTemporalSortOperator) target.getOperator()).handle;
      assertSame(
          sentinel,
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  Native.exportTemporalSortCanonicalRows(
                      handle,
                      (array, schema) -> {
                        if (consumed)
                          try (var a = ArrowArray.wrap(array);
                              var s = ArrowSchema.wrap(schema);
                              var root =
                                  Data.importVectorSchemaRoot(
                                      NativeAllocator.SHARED, a, s, NativeAllocator.DICTIONARIES)) {
                            assertEquals(1, root.getRowCount());
                          }
                        throw sentinel;
                      })));
      assertEquals(expected, plan.digest(snapshot(target)));
      target.processWatermark(new Watermark(100));
      assertEquals(List.of(plan.encode(plan.row(1, 100))), plan.nativeOutput(target));
    }
    assertEquals(handles, Native.liveNativeHandles());
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private static org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness<
          RowData, RowData, RowData>
      projection(
          Plan plan, boolean rocks, org.apache.flink.runtime.checkpoint.OperatorSubtaskState state)
          throws Exception {
    var harness =
        new org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness<
            RowData, RowData, RowData>(
            new StateView(), plan.selector, plan.selector.getProducedType(), plan.maximum, 1, 0);
    plan.stockBackend(harness, rocks);
    harness.setup(new org.apache.flink.table.runtime.typeutils.RowDataSerializer(plan.input));
    harness.initializeState(state);
    harness.open();
    return harness;
  }

  private static final class StateView
      extends org.apache.flink.streaming.api.operators.AbstractStreamOperator<RowData>
      implements org.apache.flink.streaming.api.operators.OneInputStreamOperator<RowData, RowData> {
    @Override
    public void processElement(
        org.apache.flink.streaming.runtime.streamrecord.StreamRecord<RowData> row) {}
  }
}

package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.CanonicalUnboundedOverScalarTest.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.compat.FlinkPlanCompat;
import tech.streamfusion.state.CanonicalStateBackend;
import tech.streamfusion.state.CanonicalStateBackendFactory;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;
import tech.streamfusion.state.StockOverEventFoldStateContract;

class CanonicalEventUnboundedOverInteropTest {
  static Stream<Arguments> modes() {
    return tech.streamfusion.CanonicalTestMatrix.select(allModes());
  }

  private static Stream<Arguments> allModes() {
    return Stream.of("BIGINT", "FLOAT", "DOUBLE")
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        rows ->
                            Stream.of(false, true)
                                .flatMap(
                                    global ->
                                        Stream.of(0L, 1L, 1000L)
                                            .flatMap(
                                                ttl ->
                                                    Stream.of(false, true)
                                                        .flatMap(
                                                            stock ->
                                                                Stream.of(false, true)
                                                                    .map(
                                                                        nativeStore ->
                                                                            Arguments.of(
                                                                                type,
                                                                                rows,
                                                                                global,
                                                                                ttl,
                                                                                stock,
                                                                                nativeStore)))))))
        .filter(
            args ->
                !Boolean.getBoolean("sf.canonical.eventOver.smoke")
                    || args.get()[0].equals("BIGINT")
                        && (boolean) args.get()[1]
                        && !(boolean) args.get()[2]
                        && args.get()[3].equals(1000L)
                        && !(boolean) args.get()[4]
                        && !(boolean) args.get()[5]);
  }

  @ParameterizedTest
  @MethodSource("modes")
  void actualReleasedStateAndTimersContinueBothWaysWithoutRefolding(
      String type, boolean rows, boolean global, long ttl, boolean stockStore, boolean nativeStore)
      throws Exception {
    verifyInterchange(new Plan(type, rows, global, ttl), stockStore, nativeStore);
  }

  @ParameterizedTest
  @MethodSource("constantModes")
  void physicalProctimePlaceholderSurvivesPendingCanonicalHandoffs(
      boolean rows, boolean global, long ttl, boolean stockStore, boolean nativeStore)
      throws Exception {
    var plan = new Plan("BIGINT", rows, global, ttl, false, true);
    int placeholder = -1;
    for (int p = 0; p < plan.input.getFieldCount(); p++)
      if (org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isProctimeAttribute(
          plan.input.getTypeAt(p))) placeholder = p;
    assertTrue(placeholder >= 0, "Actual SQL input must retain its PROCTIME indicator");
    assertFalse(plan.input.getTypeAt(placeholder).isNullable());
    assertTrue(ArrowConversion.toArrowSchema(plan.input).getFields().get(placeholder).isNullable());
    assertTrue(
        ArrowConversion.toArrowInputSchema(plan.input, true)
            .getFields()
            .get(placeholder)
            .isNullable());
    assertTrue(plan.row(1L, 7L, 10).isNullAt(placeholder));
    verifyInterchange(plan, stockStore, nativeStore);
  }

  static Stream<Arguments> constantModes() {
    return tech.streamfusion.CanonicalTestMatrix.select(allConstantModes());
  }

  private static Stream<Arguments> allConstantModes() {
    return Stream.of(false, true)
        .flatMap(
            rows ->
                Stream.of(false, true)
                    .flatMap(
                        global ->
                            Stream.of(0L, 1000L)
                                .flatMap(
                                    ttl ->
                                        Stream.of(false, true)
                                            .flatMap(
                                                stock ->
                                                    Stream.of(false, true)
                                                        .map(
                                                            nativeStore ->
                                                                Arguments.of(
                                                                    rows,
                                                                    global,
                                                                    ttl,
                                                                    stock,
                                                                    nativeStore))))))
        .filter(
            args ->
                !Boolean.getBoolean("sf.canonical.eventOver.smoke")
                    || (boolean) args.get()[0]
                        && !(boolean) args.get()[1]
                        && args.get()[2].equals(1000L)
                        && !(boolean) args.get()[3]
                        && !(boolean) args.get()[4]);
  }

  @ParameterizedTest
  @MethodSource("constantModes")
  void countStarAndLiteralHelpersStayOutOfStockPendingRows(
      boolean rows, boolean global, long ttl, boolean stockStore, boolean nativeStore)
      throws Exception {
    verifyInterchange(new Plan("BIGINT", rows, global, ttl, true), stockStore, nativeStore);
  }

  @ParameterizedTest
  @MethodSource("constantModes")
  void ordinaryNativeCheckpointRetainsPendingAndCleanupBeforeCanonicalReturn(
      boolean rows, boolean global, long ttl, boolean stockStore, boolean nativeStore)
      throws Exception {
    verifyCheckpoint(new Plan("BIGINT", rows, global, ttl), stockStore, nativeStore);
  }

  @ParameterizedTest
  @MethodSource("constantModes")
  void physicalProctimePlaceholderSurvivesOrdinaryNativeCheckpoint(
      boolean rows, boolean global, long ttl, boolean stockStore, boolean nativeStore)
      throws Exception {
    verifyCheckpoint(new Plan("BIGINT", rows, global, ttl, false, true), stockStore, nativeStore);
  }

  static Stream<Arguments> pollingCases() {
    return Stream.of(false, true)
        .flatMap(
            rows ->
                Stream.of(false, true)
                    .flatMap(
                        stock ->
                            Stream.of(false, true)
                                .flatMap(
                                    nativeStore ->
                                        Stream.of(0L, 1000L)
                                            .map(
                                                ttl ->
                                                    Arguments.of(rows, stock, nativeStore, ttl)))));
  }

  @ParameterizedTest
  @MethodSource("pollingCases")
  void pollingCallbacksRetainTheirPhysicalTimersAcrossTheFirstRestoredWatermark(
      boolean rows, boolean stockStore, boolean nativeStore, long ttl) throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(FlinkPlanCompat.unboundedOverVersion() == 1);
    var plan = new Plan("BIGINT", rows, false, ttl);
    OperatorSubtaskState state;
    try (var stock = plan.stock(stockStore);
        var nativeOp = plan.nativeHarness(nativeStore)) {
      stock.open();
      nativeOp.open();
      time(stock, 100);
      time(nativeOp, 100);
      var negative = List.of(plan.row(1L, 2L, -10));
      plan.stockInput(stock, negative);
      plan.nativeInput(nativeOp, negative);
      stock.processWatermark(new Watermark(-1));
      nativeOp.processWatermark(new Watermark(-1));
      assertTrue(plan.stockOutput(stock).isEmpty());
      assertTrue(plan.nativeOutput(nativeOp).isEmpty());
      stock.processWatermark(new Watermark(0));
      nativeOp.processWatermark(new Watermark(0));
      assertEquals(plan.stockOutput(stock), plan.nativeOutput(nativeOp));
      var future = List.of(plan.row(1L, 3L, 100), plan.row(2L, 4L, 200));
      plan.stockInput(stock, future);
      plan.nativeInput(nativeOp, future);
      time(stock, 800);
      time(nativeOp, 800);
      stock.processWatermark(new Watermark(10));
      nativeOp.processWatermark(new Watermark(10));
      assertTrue(plan.stockOutput(stock).isEmpty());
      assertTrue(plan.nativeOutput(nativeOp).isEmpty());
      state = snapshot(stock);
      try (var returned = plan.stock(stockStore)) {
        returned.initializeState(snapshot(nativeOp));
        returned.open();
        assertEquals(raw(stock), raw(returned), "future-only callbacks create neutral folds");
      }
    }
    try (var stock = plan.stock(stockStore);
        var nativeOp = plan.nativeHarness(nativeStore)) {
      stock.initializeState(state);
      nativeOp.initializeState(state);
      stock.open();
      nativeOp.open();
      time(stock, 900);
      time(nativeOp, 900);
      var early = List.of(plan.row(1L, 7L, 5));
      plan.stockInput(stock, early);
      plan.nativeInput(nativeOp, early);
      for (long watermark : new long[] {-1, 5, 10, 11, 200}) {
        stock.processWatermark(new Watermark(watermark));
        nativeOp.processWatermark(new Watermark(watermark));
        assertEquals(plan.stockOutput(stock), plan.nativeOutput(nativeOp));
        try (var returned = plan.stock(stockStore)) {
          returned.initializeState(snapshot(nativeOp));
          returned.open();
          assertEquals(raw(stock), raw(returned), "physical polling timers at " + watermark);
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("pollingCases")
  void orphanPollingCallbacksPreserveCleanupWithoutInventingInputOrFolds(
      boolean rows, boolean stockStore, boolean nativeStore, long ttl) throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(FlinkPlanCompat.unboundedOverVersion() == 1);
    var plan = new Plan("BIGINT", rows, false, ttl);
    OperatorSubtaskState state;
    try (var stock = plan.stock(stockStore)) {
      stock.open();
      time(stock, 100);
      plan.stockInput(stock, List.of(plan.row(1L, 3L, 10)));
      state = snapshot(stock);
    }
    try (var view = plan.view(stockStore)) {
      view.initializeState(state);
      view.open();
      var backend =
          (CheckpointableKeyedStateBackend<RowData>)
              view.getOperator().<RowData>getKeyedStateBackend();
      var timers =
          new TimerSerializer<>(
              plan.selector.getProducedType().toSerializer(), VoidNamespaceSerializer.INSTANCE);
      backend
          .create("_timer_state/event_user-timers", timers)
          .add(
              new TimerHeapInternalTimer<>(
                  42, plan.selector.getKey(plan.row(99L, 0L, 0)), VoidNamespace.INSTANCE));
      state = snapshot(view);
    }
    try (var stock = plan.stock(stockStore);
        var nativeOp = plan.nativeHarness(nativeStore)) {
      stock.initializeState(state);
      nativeOp.initializeState(state);
      stock.open();
      nativeOp.open();
      time(stock, 900);
      time(nativeOp, 900);
      for (long watermark : new long[] {-1, 0, 42}) {
        stock.processWatermark(new Watermark(watermark));
        nativeOp.processWatermark(new Watermark(watermark));
        assertEquals(plan.stockOutput(stock), plan.nativeOutput(nativeOp));
        try (var returned = plan.stock(stockStore)) {
          returned.initializeState(snapshot(nativeOp));
          returned.open();
          assertEquals(raw(stock), raw(returned), "orphan callback at " + watermark);
        }
      }
    }
  }

  private void verifyCheckpoint(Plan plan, boolean stockStore, boolean nativeStore)
      throws Exception {
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    OperatorSubtaskState original, checkpoint;
    try (var stock = plan.stock(stockStore)) {
      stock.open();
      time(stock, 100);
      plan.stockInput(stock, List.of(plan.row(1L, 3L, 1)));
      stock.processWatermark(new Watermark(1));
      plan.stockOutput(stock);
      time(stock, 200);
      plan.stockInput(stock, List.of(plan.row(1L, 7L, 10), plan.row(null, 5L, 11)));
      original = snapshot(stock);
    }
    try (var nativeOp = plan.nativeHarness(nativeStore)) {
      nativeOp.initializeState(original);
      nativeOp.open();
      time(nativeOp, 200);
      checkpoint = nativeOp.snapshot(2, 300);
      assertTrue(
          java.util.stream.Stream.concat(
                      checkpoint.getManagedKeyedState().stream(),
                      checkpoint.getRawKeyedState().stream())
                  .count()
              > 0);
      assertTrue(
          java.util.stream.Stream.concat(
                  checkpoint.getManagedKeyedState().stream(),
                  checkpoint.getRawKeyedState().stream())
              .noneMatch(
                  state ->
                      state instanceof org.apache.flink.runtime.state.SavepointKeyedStateHandle));
    }
    try (var reference = plan.stock(stockStore);
        var recovered = plan.nativeHarness(nativeStore)) {
      reference.initializeState(original);
      reference.open();
      recovered.initializeState(checkpoint);
      recovered.open();
      time(reference, 500);
      time(recovered, 500);
      var suffix = List.of(plan.row(1L, 2L, 10), plan.row(null, 1L, 1));
      plan.stockInput(reference, suffix);
      plan.nativeInput(recovered, suffix);
      reference.processWatermark(new Watermark(20));
      recovered.processWatermark(new Watermark(20));
      assertEquals(plan.stockOutput(reference), plan.nativeOutput(recovered));
      try (var returned = plan.stock(stockStore)) {
        returned.initializeState(snapshot(recovered));
        returned.open();
        assertEquals(raw(reference), raw(returned));
      }
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private void verifyInterchange(Plan plan, boolean stockStore, boolean nativeStore)
      throws Exception {
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    String handles = Native.liveNativeHandles();
    OperatorSubtaskState stockState, nativeState;
    Map<String, String> expected;
    try (var stock = plan.stock(stockStore);
        var nativeOp = plan.nativeHarness(nativeStore)) {
      stock.open();
      nativeOp.open();
      time(stock, 100);
      time(nativeOp, 100);
      var prefix = List.of(plan.row(1L, 3L, 1), plan.row(null, null, 1));
      assertTrue(plan.stockInput(stock, prefix).isEmpty());
      assertTrue(plan.nativeInput(nativeOp, prefix).isEmpty());
      stock.processWatermark(new Watermark(1));
      nativeOp.processWatermark(new Watermark(1));
      assertEquals(plan.stockOutput(stock), plan.nativeOutput(nativeOp));
      time(stock, 200);
      time(nativeOp, 200);
      var pending =
          List.of(
              plan.row(1L, 7L, 10),
              plan.row(1L, null, 10),
              plan.row(null, null, 12),
              plan.row(2L, 11L, 15),
              plan.row(9L, 99L, 1));
      assertTrue(plan.stockInput(stock, pending).isEmpty());
      assertTrue(plan.nativeInput(nativeOp, pending).isEmpty());
      expected = raw(stock);
      stockState = snapshot(stock);
      nativeState = snapshot(nativeOp);
    }
    try (var reference = plan.stock(stockStore);
        var returned = plan.stock(stockStore);
        var restored = plan.nativeHarness(nativeStore)) {
      reference.initializeState(stockState);
      reference.open();
      returned.initializeState(nativeState);
      returned.open();
      assertEquals(expected, raw(returned));
      restored.initializeState(stockState);
      restored.open();
      try (var unchanged = plan.stock(stockStore)) {
        unchanged.initializeState(snapshot(restored));
        unchanged.open();
        assertEquals(
            expected,
            raw(unchanged),
            "pending buckets, raw folds and both timer domains survive immediate re-export");
      }
      time(reference, 500);
      time(returned, 500);
      time(restored, 500);
      var suffix = List.of(plan.row(1L, 2L, 10), plan.row(null, 5L, 1));
      assertTrue(plan.stockInput(reference, suffix).isEmpty());
      assertTrue(plan.stockInput(returned, suffix).isEmpty());
      assertTrue(plan.nativeInput(restored, suffix).isEmpty());
      reference.processWatermark(new Watermark(20));
      returned.processWatermark(new Watermark(20));
      restored.processWatermark(new Watermark(20));
      var output = plan.stockOutput(reference);
      assertEquals(output, plan.stockOutput(returned));
      assertEquals(output, plan.nativeOutput(restored));
      time(reference, 2000);
      time(returned, 2000);
      time(restored, 2000);
      var afterIdle = List.of(plan.row(1L, 5L, 30));
      plan.stockInput(reference, afterIdle);
      plan.stockInput(returned, afterIdle);
      plan.nativeInput(restored, afterIdle);
      reference.processWatermark(new Watermark(30));
      returned.processWatermark(new Watermark(30));
      restored.processWatermark(new Watermark(30));
      var idleOutput = plan.stockOutput(reference);
      assertEquals(idleOutput, plan.stockOutput(returned));
      assertEquals(idleOutput, plan.nativeOutput(restored));
      try (var finalStock = plan.stock(stockStore)) {
        finalStock.initializeState(snapshot(restored));
        finalStock.open();
        assertEquals(raw(reference), raw(finalStock));
      }
    }
    assertEquals(handles, Native.liveNativeHandles());
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  static Stream<Arguments> invalid() {
    return Stream.of(false, true)
        .flatMap(
            rows ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of(
                                    "missing-event",
                                    "orphan-event",
                                    "missing-cleanup",
                                    "orphan-processing",
                                    "wrong-processing",
                                    "empty-peers",
                                    "row-kind",
                                    "row-key",
                                    "row-time",
                                    "null-required",
                                    "foreign")
                                .map(reason -> Arguments.of(rows, rocks, reason))))
        .filter(
            args ->
                FlinkPlanCompat.unboundedOverVersion() != 1
                    || !args.get()[2].equals("orphan-event"))
        .filter(
            args ->
                !Boolean.getBoolean("sf.canonical.eventOver.smoke")
                    || (boolean) args.get()[0]
                        && !(boolean) args.get()[1]
                        && args.get()[2].equals("missing-event"));
  }

  @ParameterizedTest
  @MethodSource("invalid")
  void malformedSourceFailsBeforeAnyNativeMutation(boolean rows, boolean rocks, String reason)
      throws Exception {
    var plan = new Plan("BIGINT", rows, false, 1000, false, false, reason.equals("null-required"));
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var stock = plan.stock(rocks)) {
      stock.open();
      time(stock, 100);
      plan.stockInput(stock, List.of(plan.row(1L, 3L, 1)));
      stock.processWatermark(new Watermark(1));
      plan.stockOutput(stock);
      plan.stockInput(stock, List.of(plan.row(1L, 7L, 10)));
      try (var corrupted = plan.view(rocks)) {
        corrupted.initializeState(snapshot(stock));
        corrupted.open();
        var backend =
            (CheckpointableKeyedStateBackend<RowData>)
                corrupted.getOperator().<RowData>getKeyedStateBackend();
        var key = plan.selector.getKey(plan.row(1L, 0L, 0));
        backend.setCurrentKey(key);
        var ns = VoidNamespace.INSTANCE;
        var namespaces = VoidNamespaceSerializer.INSTANCE;
        MapState<Long, List<RowData>> pending =
            backend.getPartitionedState(
                ns,
                namespaces,
                new MapStateDescriptor<>(
                    "inputState",
                    LongSerializer.INSTANCE,
                    new ListSerializer<>(new RowDataSerializer(plan.input))));
        var timers =
            new TimerSerializer<>(plan.selector.getProducedType().toSerializer(), namespaces);
        var eventTimers = backend.create("_timer_state/event_user-timers", timers);
        var processingTimers = backend.create("_timer_state/processing_user-timers", timers);
        switch (reason) {
          case "missing-event":
            while (eventTimers.poll() != null) {}
            break;
          case "orphan-event":
            eventTimers.add(new TimerHeapInternalTimer<>(11, key, ns));
            break;
          case "missing-cleanup":
            backend
                .getPartitionedState(
                    ns,
                    namespaces,
                    new ValueStateDescriptor<>(
                        "RowTimeUnboundedOverCleanupTime", LongSerializer.INSTANCE))
                .clear();
            break;
          case "orphan-processing":
            processingTimers.add(
                new TimerHeapInternalTimer<>(1600, plan.selector.getKey(plan.row(99L, 0L, 0)), ns));
            break;
          case "wrong-processing":
            processingTimers.add(new TimerHeapInternalTimer<>(1601, key, ns));
            break;
          case "empty-peers":
            pending.put(10L, List.of());
            break;
          case "row-kind":
          case "row-key":
          case "row-time":
            RowData bad =
                plan.row(
                    reason.equals("row-key") ? 99L : 1L, 7L, reason.equals("row-time") ? 11 : 10);
            if (reason.equals("row-kind")) bad.setRowKind(org.apache.flink.types.RowKind.DELETE);
            pending.put(10L, List.of(bad));
            break;
          case "null-required":
            assertFalse(plan.input.getTypeAt(1).isNullable());
            pending.put(10L, List.of(plan.row(1L, null, 10)));
            break;
          case "foreign":
            backend
                .getPartitionedState(
                    ns, namespaces, new ValueStateDescriptor<>("foreign", LongSerializer.INSTANCE))
                .update(9L);
            break;
          default:
            throw new AssertionError(reason);
        }
        try (var source = plan.view(rocks)) {
          source.initializeState(snapshot(corrupted));
          source.open();
          var importBackend =
              (CheckpointableKeyedStateBackend<RowData>)
                  source.getOperator().<RowData>getKeyedStateBackend();
          var before = raw(source);
          long target =
              Native.createOverAggregator(
                  plan.codes,
                  plan.kinds,
                  plan.timeColumn,
                  plan.columns,
                  plan.partitions,
                  rows ? 3 : 0,
                  0,
                  false,
                  1000,
                  16_000_000);
          try {
            plan.contract.configureNativeState(target, false);
            assertThrows(
                Exception.class, () -> plan.contract.importState(importBackend, target, false));
            assertEquals(before, raw(source));
            Native.exportOverFoldCanonical(
                target,
                (a, s) -> {
                  throw new AssertionError("invalid source installed folds");
                });
            Native.exportOverPendingCanonical(
                target,
                (a, s) -> {
                  throw new AssertionError("invalid source installed pending rows");
                });
            Native.exportOverCleanupCanonical(
                target,
                (a, s) -> {
                  throw new AssertionError("invalid source installed cleanup");
                });
          } finally {
            Native.closeOverAggregator(target);
          }
        }
      }
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  static final class Plan {
    final PlannerBase planner;
    final String json;
    final RowType input, output, accumulator;
    final RowDataKeySelector selector;
    final StockOverEventFoldStateContract contract;
    final int[] kinds, columns, codes, partitions;
    final int timeColumn;
    final boolean rows;
    final long ttl;
    final tech.streamfusion.planner.OverConstantArguments constants;

    Plan(String type, boolean rows, boolean global, long ttl) throws Exception {
      this(type, rows, global, ttl, false);
    }

    Plan(String type, boolean rows, boolean global, long ttl, boolean literals) throws Exception {
      this(type, rows, global, ttl, literals, false);
    }

    Plan(String type, boolean rows, boolean global, long ttl, boolean literals, boolean proctime)
        throws Exception {
      this(type, rows, global, ttl, literals, proctime, false);
    }

    Plan(
        String type,
        boolean rows,
        boolean global,
        long ttl,
        boolean literals,
        boolean proctime,
        boolean requiredValue)
        throws Exception {
      this.rows = rows;
      this.ttl = ttl;
      var config = new Configuration();
      config.setString("streamfusion.native.enabled", "false");
      config.setString("table.exec.state.ttl", ttl + " ms");
      var table = NativePlannerTestEnvironment.create(config);
      table.executeSql(
          "CREATE TABLE scalar_input(k BIGINT,v "
              + type
              + (requiredValue ? " NOT NULL" : "")
              + ",rt TIMESTAMP_LTZ(3),"
              + (proctime ? "pt AS PROCTIME()," : "")
              + "WATERMARK FOR rt AS rt) WITH ('connector'='datagen')");
      table.executeSql(
          "CREATE TABLE scalar_output(k BIGINT,v "
              + type
              + ",s "
              + type
              + ",c BIGINT,a "
              + type
              + (proctime ? ",pt TIMESTAMP_LTZ(3)" : "")
              + ") WITH ('connector'='blackhole')");
      json =
          table
              .compilePlanSql(
                  "INSERT INTO scalar_output SELECT k,v,"
                      + (literals
                          ? "COUNT(*) OVER w,SUM(CAST(7 AS BIGINT)) OVER w,AVG(v) OVER w"
                          : "SUM(v) OVER w,COUNT(v) OVER w,AVG(v) OVER w")
                      + (proctime ? ",pt" : "")
                      + " FROM scalar_input WINDOW w AS ("
                      + (global ? "" : "PARTITION BY k ")
                      + "ORDER BY rt "
                      + (rows ? "ROWS" : "RANGE")
                      + " BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)")
              .asJsonString();
      planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
      var node = node();
      input = (RowType) node.getInputEdges().get(0).getOutputType();
      output = (RowType) node.getOutputType();
      var transform = (OneInputTransformation<?, ?>) node.translateToPlan(planner);
      var operator =
          (KeyedProcessOperator<?, ?, ?>)
              ((SimpleOperatorFactory<?>) transform.getOperatorFactory()).getOperator();
      var function = operator.getUserFunction();
      assertEquals(FlinkPlanCompat.eventUnboundedOver(rows), function.getClass());
      Class<?> owner = function.getClass();
      while (Arrays.stream(owner.getDeclaredFields())
          .noneMatch(candidate -> candidate.getName().equals("accTypes")))
        owner = owner.getSuperclass();
      Field field = owner.getDeclaredField("accTypes");
      field.setAccessible(true);
      accumulator = RowType.of((LogicalType[]) field.get(function));
      selector = (RowDataKeySelector) transform.getStateKeySelector();
      var reader = FlinkPlanCompat.createJsonObjectReader(planner.createSerdeContext());
      org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode definition = null;
      for (var candidate : reader.readTree(json).path("nodes"))
        if (candidate.path("id").asInt() == node.getId()) definition = candidate;
      var spec = reader.forType(OverSpec.class).<OverSpec>readValue(definition.path("overSpec"));
      partitions = spec.getPartition().getFieldIndices();
      var group = spec.getGroups().get(0);
      timeColumn = group.getSort().getFieldIndices()[0];
      var calls = group.getAggCalls();
      var referenced =
          calls.stream()
              .flatMap(call -> call.getArgList().stream())
              .filter(index -> index >= input.getFieldCount())
              .map(index -> index - input.getFieldCount())
              .collect(java.util.stream.Collectors.toSet());
      constants =
          tech.streamfusion.planner.OverConstantArguments.create(
                  spec.getConstants(),
                  calls.stream().anyMatch(call -> call.getArgList().isEmpty()),
                  referenced)
              .orElseThrow();
      var argumentType = constants.appendTo(input);
      var infos =
          AggregateUtil.transformToStreamAggregateInfoList(
              planner.getTypeFactory(),
              argumentType,
              JavaScalaConversionUtil.toScala(calls),
              new boolean[calls.size()],
              false,
              true,
              true);
      assertEquals(0, infos.distinctInfos().length);
      assertFalse(infos.countStarInserted());
      int[] widths =
          Arrays.stream(infos.getActualAggregateInfos())
              .mapToInt(info -> info.externalAccTypes().length)
              .toArray();
      kinds =
          calls.stream()
              .mapToInt(
                  call -> {
                    switch (call.getAggregation().getKind()) {
                      case SUM:
                        return 0;
                      case COUNT:
                        return 3;
                      case AVG:
                        return 4;
                      case SUM0:
                        return 22;
                      default:
                        throw new AssertionError(call);
                    }
                  })
              .toArray();
      columns =
          calls.stream()
              .mapToInt(
                  call ->
                      call.getArgList().isEmpty()
                          ? input.getFieldCount() + spec.getConstants().size()
                          : call.getArgList().get(0))
              .toArray();
      codes =
          Arrays.stream(columns)
              .map(
                  column -> {
                    var logical = argumentType.getTypeAt(column);
                    return logical.is(LogicalTypeRoot.FLOAT)
                        ? 6
                        : logical.is(LogicalTypeRoot.DOUBLE) ? 1 : 0;
                  })
              .toArray();
      assertArrayEquals(
          Arrays.stream(infos.getAccTypes())
              .map(org.apache.flink.table.types.DataType::getLogicalType)
              .toArray(LogicalType[]::new),
          accumulator.getChildren().toArray(LogicalType[]::new));
      contract =
          new StockOverEventFoldStateContract(
              selector.getProducedType().toSerializer(),
              selector.getProducedType().toRowType(),
              input,
              accumulator,
              partitions,
              timeColumn,
              kinds,
              widths,
              constants.type(),
              constants.encodedValues(),
              ttl > 1);
    }

    ExecNode<?> node() throws Exception {
      var graph =
          FlinkPlanCompat.createJsonObjectReader(planner.createSerdeContext())
              .forType(ExecNodeGraph.class)
              .<ExecNodeGraph>readValue(json);
      return CanonicalBoundedRangeInteropTest.Plan.find(graph.getRootNodes().get(0));
    }

    @SuppressWarnings("unchecked")
    KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(boolean rocks)
        throws Exception {
      var transform = (OneInputTransformation<?, ?>) node().translateToPlan(planner);
      var operator =
          (KeyedProcessOperator<RowData, RowData, RowData>)
              ((SimpleOperatorFactory<?>) transform.getOperatorFactory()).getOperator();
      var harness =
          new KeyedOneInputStreamOperatorTestHarness<>(
              operator, selector, selector.getProducedType(), 128, 1, 0);
      var config = new Configuration();
      config.set(StateBackendOptions.STATE_BACKEND, rocks ? "rocksdb" : "hashmap");
      harness.setStateBackend(
          StateBackendLoader.loadStateBackendFromConfig(config, getClass().getClassLoader(), null));
      harness.setup(new RowDataSerializer(output));
      return harness;
    }

    KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> view(boolean rocks)
        throws Exception {
      var harness =
          new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
              new StateView(), selector, selector.getProducedType(), 128, 1, 0);
      var config = new Configuration();
      config.set(StateBackendOptions.STATE_BACKEND, rocks ? "rocksdb" : "hashmap");
      harness.setStateBackend(
          StateBackendLoader.loadStateBackendFromConfig(config, getClass().getClassLoader(), null));
      harness.setup(new RowDataSerializer(output));
      return harness;
    }

    private static final class StateView
        extends org.apache.flink.streaming.api.operators.AbstractStreamOperator<RowData>
        implements org.apache.flink.streaming.api.operators.OneInputStreamOperator<
            RowData, RowData> {
      @Override
      public void processElement(StreamRecord<RowData> row) {}
    }

    KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness(
        boolean rocks) throws Exception {
      var operator =
          new NativeOverAggregateOperator(
              timeColumn,
              columns,
              partitions,
              codes,
              kinds,
              rows ? 3 : 0,
              0,
              false,
              partitions.length == 0
                  ? new int[] {Native.STOCK_KEY_PROJECTION}
                  : new int[] {Native.STOCK_KEY_PROJECTION, -1},
              ttl,
              input,
              128);
      if (constants.type().getFieldCount() > 0)
        operator.withConstantArguments(constants.type(), constants.encodedValues());
      if (ttl > 1) operator.withEventTimeCleanupTimers();
      operator.setStockStateContract(contract);
      var harness =
          new KeyedOneInputStreamOperatorTestHarness<>(
              operator, ignored -> 0, Types.INT, 128, 1, 0);
      var config = new Configuration();
      if (rocks)
        config.setString("state.backend.type", RocksDBNativeStateBackendFactory.class.getName());
      CanonicalStateBackend.install(
          StreamExecutionEnvironment.getExecutionEnvironment(),
          config,
          harness.getStreamConfig().getOperatorID().toHexString(),
          operator.getClass().getSimpleName(),
          contract,
          getClass().getClassLoader());
      harness.setStateBackend(
          new CanonicalStateBackendFactory().createFromConfig(config, getClass().getClassLoader()));
      harness.setup(new ArrowBatchSerializer());
      return harness;
    }

    RowData row(Long key, Long value, long timestamp) {
      var row = new GenericRowData(input.getFieldCount());
      row.setField(0, key);
      Object number = value;
      if (value != null && input.getTypeAt(1).is(LogicalTypeRoot.FLOAT))
        number = value.floatValue();
      else if (value != null && input.getTypeAt(1).is(LogicalTypeRoot.DOUBLE))
        number = value.doubleValue();
      row.setField(1, number);
      row.setField(timeColumn, TimestampData.fromEpochMillis(timestamp));
      return row;
    }

    List<String> stockInput(
        KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness,
        List<RowData> rows)
        throws Exception {
      harness.getOutput().clear();
      for (var row : rows)
        harness.processElement(
            new StreamRecord<>(new RowDataSerializer(input).toBinaryRow(row).copy()));
      return stockOutput(harness);
    }

    List<String> nativeInput(
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
        List<RowData> rows)
        throws Exception {
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(rows, input, NativeAllocator.SHARED, true))));
      return nativeOutput(harness);
    }

    List<String> stockOutput(
        KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
      var result =
          harness.extractOutputValues().stream()
              .map(this::value)
              .sorted()
              .collect(java.util.stream.Collectors.toList());
      harness.getOutput().clear();
      return result;
    }

    List<String> nativeOutput(
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
      var out = new ArrayList<String>();
      for (var batch : harness.extractOutputValues())
        try (var root = batch.root()) {
          var reader = ArrowConversion.createArrowReader(root, output);
          for (int i = 0; i < root.getRowCount(); i++) out.add(value(reader.read(i)));
        }
      harness.getOutput().clear();
      out.sort(String::compareTo);
      return out;
    }

    private String value(RowData row) {
      var values = new ArrayList<String>();
      for (int i = 0; i < output.getFieldCount(); i++) {
        Object value =
            row.isNullAt(i)
                ? null
                : RowData.createFieldGetter(output.getTypeAt(i).copy(true), i).getFieldOrNull(row);
        values.add(String.valueOf(value));
      }
      return String.join("|", values);
    }
  }
}

package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
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
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowAggregate;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.compat.FlinkPlanCompat;
import tech.streamfusion.state.FlinkRocksDBOptions;
import tech.streamfusion.state.StockSlicingWindowStateContract;

/** Actual released SQL-generated operators are the reference, including their hidden COUNT(*). */
class CanonicalSharedWindowInteropTest {
  static long processingWatermark() {
    return tech.streamfusion.compat.CanonicalTestCompat.flink22() ? Long.MAX_VALUE - 1 : BASE + 500;
  }

  static final long BASE = Instant.parse("2024-03-31T00:59:57Z").toEpochMilli();
  @TempDir Path directory;

  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(allCases());
  }

  private static Stream<Arguments> allCases() {
    var result = new ArrayList<Arguments>();
    for (String shape : List.of("HOP", "CUMULATE"))
      for (String zone : List.of("UTC", "+05:45", "Europe/Berlin", "America/New_York"))
        for (boolean persistent : new boolean[] {false, true})
          for (boolean keyless : new boolean[] {false, true})
            for (boolean countStar : new boolean[] {false, true})
              result.add(Arguments.of(shape, zone, persistent, keyless, countStar));
    return result.stream();
  }

  @ParameterizedTest(name = "{0}, zone={1}, native rocks={2}, keyless={3}, COUNT(*)={4}")
  @MethodSource("cases")
  void releasedPanesContinueAfterFiringLateInputAndRepeatedEngineSwitches(
      String shape, String zoneId, boolean persistent, boolean keyless, boolean countStar)
      throws Exception {
    var plan = new StockPlan(shape, zoneId, keyless, countStar);
    assertContinuation(plan, persistent);
  }

  static Stream<Arguments> distinctCases() {
    var cases = new ArrayList<Arguments>();
    for (String shape : List.of("HOP", "CUMULATE"))
      for (String zone : List.of("UTC", "Europe/Berlin"))
        for (boolean rocks : new boolean[] {false, true})
          for (boolean keyless : new boolean[] {false, true})
            cases.add(Arguments.of(shape, zone, rocks, keyless));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("distinctCases")
  void releasedDistinctViewsContinueAcrossSharedPaneHandoffs(
      String shape, String zone, boolean rocks, boolean keyless) throws Exception {
    assertContinuation(new StockPlan(shape, zone, keyless, false, true), rocks);
  }

  static Stream<Arguments> offsetCases() {
    var cases = new ArrayList<Arguments>();
    for (String shape : List.of("HOP", "CUMULATE"))
      for (String zone : List.of("UTC", "+05:45", "America/New_York", "Europe/Berlin"))
        for (boolean rocks : new boolean[] {false, true})
          for (boolean distinct : new boolean[] {false, true})
            for (long offset : new long[] {-1250, 1250})
              cases.add(Arguments.of(shape, zone, rocks, distinct, offset));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("offsetCases")
  void releasedSqlOffsetsPreservePanesTimersAndDistinctCounters(
      String shape, String zone, boolean rocks, boolean distinct, long offset) throws Exception {
    assertContinuation(new StockPlan(shape, zone, false, false, distinct, offset), rocks);
  }

  private void assertContinuation(StockPlan plan, boolean persistent) throws Exception {
    long handle = plan.create(persistent, directory.resolve("native"));
    var continued = new ArrayList<>(middle());
    var suffix = new ArrayList<>(suffix());
    if (plan.distinct) {
      continued.add(new Input(-1L, 5L, 1500));
      continued.add(new Input(-1L, 5L, 1600));
      suffix.add(new Input(-1L, 5L, 2500));
    }
    try (var source = plan.stock(!persistent);
        var reference = plan.stock(persistent);
        var view = plan.harness(new Projection(), persistent)) {
      source.open();
      for (var row : prefix()) plan.send(source, row);
      if (plan.distinct) {
        plan.send(source, new Input(-1L, 5L, 1200));
        plan.send(source, new Input(-1L, 5L, 200));
      }
      source.processWatermark(new Watermark(BASE + 999));
      source.prepareSnapshotPreBarrier(1);
      var snapshot =
          source
              .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      reference.initializeState(snapshot);
      reference.open();
      view.initializeState(snapshot);
      view.open();
      var contract = plan.contract();
      contract.initializeOperatorState(((Projection) view.getOperator()).store, true);
      assertEquals(BASE + 999, contract.restoredWatermark());
      contract.importState(backend(view), handle, persistent);
      for (var row : continued) plan.send(reference, row);
      plan.update(handle, continued);
      reference.processWatermark(new Watermark(BASE + 1999));
      assertEquals(plan.output(reference), plan.flush(handle, BASE + 1999));
      contract.snapshotOperatorState(BASE + 1999);
      contract.exportState(backend(view), handle, persistent);
      if (plan.distinct)
        assertEquals(plan.memberships(backend(reference)), plan.memberships(backend(view)));
      var exported =
          view.snapshotWithLocalState(2, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      try (var restored = plan.stock(!persistent)) {
        restored.initializeState(exported);
        restored.open();
        for (var row : suffix) {
          plan.send(reference, row);
          plan.send(restored, row);
        }
        for (long watermark : new long[] {BASE + 2999, BASE + 4999, Long.MAX_VALUE}) {
          reference.processWatermark(new Watermark(watermark));
          restored.processWatermark(new Watermark(watermark));
          assertEquals(plan.output(reference), plan.output(restored));
        }
      }
    } finally {
      Native.closeTumblingAggregator(handle);
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "HOP,false,false",
    "HOP,true,false",
    "HOP,false,true",
    "HOP,true,true",
    "CUMULATE,false,false",
    "CUMULATE,true,false",
    "CUMULATE,false,true",
    "CUMULATE,true,true"
  })
  void retainedSharedStateWithoutAnyPendingTimerIsRejected(
      String shape, boolean persistent, boolean keyless) throws Exception {
    var plan = new StockPlan(shape, "UTC", keyless, true);
    long handle = plan.create(persistent, directory.resolve("native"));
    try (var source = plan.stock(!persistent);
        var view = plan.harness(new Projection(), persistent)) {
      source.open();
      for (var row : prefix()) plan.send(source, row);
      source.processWatermark(new Watermark(BASE + 999));
      source.prepareSnapshotPreBarrier(1);
      view.initializeState(
          source
              .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState());
      view.open();
      var timers =
          backend(view)
              .create(
                  "_timer_state/event_window-timers",
                  new org.apache.flink.streaming.api.operators.TimerSerializer<>(
                      plan.selector.getProducedType().toSerializer(),
                      org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE));
      while (timers.poll() != null) {}
      var damaged =
          view.snapshotWithLocalState(2, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      try (var restored = plan.harness(new Projection(), persistent)) {
        restored.initializeState(damaged);
        restored.open();
        var contract = plan.contract();
        contract.initializeOperatorState(((Projection) restored.getOperator()).store, true);
        var rejected =
            assertThrows(
                IllegalStateException.class,
                () -> contract.importState(backend(restored), handle, persistent));
        assertTrue(rejected.getMessage().contains("no pending timer"), rejected.getMessage());
      }
      Native.exportSlicingWindowCanonical(
          handle, false, (array, schema) -> fail("Rejected state must not import any native rows"));
    } finally {
      Native.closeTumblingAggregator(handle);
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "HOP,false,false",
    "HOP,true,false",
    "HOP,false,true",
    "HOP,true,true",
    "CUMULATE,false,false",
    "CUMULATE,true,false",
    "CUMULATE,false,true",
    "CUMULATE,true,true"
  })
  void operatorOnlyWatermarkRestoresBeforeInputWithoutAnyKeyedImport(
      String shape, boolean persistent, boolean keyless) throws Exception {
    var plan = new StockPlan(shape, "UTC", keyless, true);
    long handle = plan.create(persistent, directory.resolve("native"));
    try (var source = plan.stock(!persistent);
        var view = plan.harness(new Projection(), persistent)) {
      source.open();
      plan.send(source, new Input(9L, 5L, 100));
      source.processWatermark(new Watermark(BASE + 5999));
      assertFalse(plan.output(source).isEmpty());
      source.prepareSnapshotPreBarrier(1);
      var snapshot =
          source
              .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      try (var entries = backend(source).getKeysAndNamespaces("window-aggs")) {
        assertEquals(0, entries.count());
      }
      assertFalse(snapshot.getManagedOperatorState().isEmpty());
      // A task without a keyed assignment must still consume redistributed UNION watermark state.
      var operatorOnly =
          org.apache.flink.runtime.checkpoint.OperatorSubtaskState.builder()
              .setManagedOperatorState(snapshot.getManagedOperatorState())
              .build();
      view.initializeState(operatorOnly);
      view.open();
      var contract = plan.contract();
      contract.initializeOperatorState(((Projection) view.getOperator()).store, true);
      // The runtime always configures the native handle, but imports only restored keyed handles.
      contract.configureNativeState(handle, persistent);
      var rows = List.of(new Input(9L, 1000L, 200), new Input(9L, 7L, 6500));
      for (var row : rows) plan.send(source, row);
      plan.update(handle, rows);
      source.processWatermark(new Watermark(Long.MAX_VALUE));
      assertEquals(plan.output(source), plan.flush(handle, Long.MAX_VALUE));
    } finally {
      Native.closeTumblingAggregator(handle);
    }
  }

  static Stream<Arguments> timerCases() {
    var result = new ArrayList<Arguments>();
    for (String zone : List.of("UTC", "+05:45", "Europe/Berlin", "America/New_York"))
      for (boolean persistent : new boolean[] {false, true})
        for (boolean keyless : new boolean[] {false, true})
          result.add(Arguments.of(zone, persistent, keyless));
    return result.stream();
  }

  @ParameterizedTest
  @MethodSource("timerCases")
  void timerOnlySavepointsCanBeImportedAndReexportedWithoutInput(
      String zoneId, boolean persistent, boolean keyless) throws Exception {
    var plan = new StockPlan("HOP", zoneId, keyless, false);
    long handle = plan.create(persistent, directory.resolve("native"));
    try (var source = plan.stock(!persistent);
        var view = plan.harness(new Projection(), persistent)) {
      source.open();
      plan.send(source, new Input(9L, null, 100));
      source.processWatermark(new Watermark(BASE + 2999));
      assertEquals(3, plan.output(source).size());
      try (var entries = backend(source).getKeysAndNamespaces("window-aggs")) {
        assertEquals(0, entries.count());
      }
      source.prepareSnapshotPreBarrier(1);
      var snapshot =
          source
              .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      view.initializeState(snapshot);
      view.open();
      var contract = plan.contract();
      contract.initializeOperatorState(((Projection) view.getOperator()).store, true);
      contract.importState(backend(view), handle, persistent);
      var timers = new java.util.concurrent.atomic.AtomicInteger();
      Native.exportSlicingWindowCanonical(
          handle,
          true,
          (arrayAddress, schemaAddress) -> {
            try (var array = ArrowArray.wrap(arrayAddress);
                var schema = ArrowSchema.wrap(schemaAddress);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              timers.addAndGet(root.getRowCount());
            }
          });
      assertEquals(1, timers.get());
      contract.snapshotOperatorState(BASE + 2999);
      contract.exportState(backend(view), handle, persistent);
      var exported =
          view.snapshotWithLocalState(2, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      try (var restored = plan.stock(!persistent)) {
        restored.initializeState(exported);
        restored.open();
        assertTrue(plan.flush(handle, BASE + 3999).isEmpty());
        restored.processWatermark(new Watermark(BASE + 3999));
        assertTrue(plan.output(restored).isEmpty());
        plan.send(source, new Input(9L, 7L, 4500));
        plan.send(restored, new Input(9L, 7L, 4500));
        source.processWatermark(new Watermark(Long.MAX_VALUE));
        restored.processWatermark(new Watermark(Long.MAX_VALUE));
        assertEquals(plan.output(source), plan.output(restored));
      }
    } finally {
      Native.closeTumblingAggregator(handle);
    }
  }

  static final class Input {
    final Long key, value;
    final long timestamp;

    Input(Long key, Long value, long timestamp) {
      this.key = key;
      this.value = value;
      this.timestamp = timestamp;
    }
  }

  private static List<Input> prefix() {
    return List.of(
        new Input(-1L, 5L, 100),
        new Input(-1L, null, 300),
        new Input(2L, null, 900),
        new Input(null, 7L, 800),
        new Input(-1L, 11L, 1100),
        new Input(2L, null, 2100),
        new Input(9L, null, 900));
  }

  private static List<Input> middle() {
    return List.of(
        new Input(-1L, 99L, 200),
        new Input(-1L, 3L, 700),
        new Input(2L, null, 800),
        new Input(null, -2L, 900),
        new Input(-1L, 9L, 1300),
        new Input(2L, 20L, 2300));
  }

  private static List<Input> suffix() {
    return List.of(
        new Input(-1L, 1000L, 900),
        new Input(-1L, -4L, 1500),
        new Input(2L, 2L, 2500),
        new Input(null, 5L, 1800),
        new Input(-1L, 3L, 3500));
  }

  static final class StockPlan {
    final PlannerBase planner;
    final String json;
    final ZoneId zone;
    final boolean cumulative, keyless, countStar, distinct, integerKey;
    final boolean session, processingTime, tumbling;
    final int[] kinds, averageOffsets;
    final boolean[] starValues;
    final int visibleAggregates;
    final RowType input, output, accumulators;
    final int countStarIndex;
    final RowDataKeySelector selector;
    final List<tech.streamfusion.state.StockWindowDistinctView> views;
    final List<org.apache.flink.api.common.state.MapStateDescriptor<Object, Object>>
        viewDescriptors;
    final int scalarFields;
    final long windowOffset;

    StockPlan(String shape, String zoneId, boolean keyless, boolean countStar) throws Exception {
      this(shape, zoneId, keyless, countStar, false);
    }

    StockPlan(String shape, String zoneId, boolean keyless, boolean countStar, boolean distinct)
        throws Exception {
      this(shape, zoneId, keyless, countStar, distinct, 0);
    }

    StockPlan(
        String shape,
        String zoneId,
        boolean keyless,
        boolean countStar,
        boolean distinct,
        long windowOffset)
        throws Exception {
      this(shape, zoneId, keyless, countStar, distinct, windowOffset, false);
    }

    StockPlan(
        String shape,
        String zoneId,
        boolean keyless,
        boolean countStar,
        boolean distinct,
        long windowOffset,
        boolean processingTime)
        throws Exception {
      this(shape, zoneId, keyless, countStar, distinct, windowOffset, processingTime, false);
    }

    StockPlan(
        String shape,
        String zoneId,
        boolean keyless,
        boolean countStar,
        boolean distinct,
        long windowOffset,
        boolean processingTime,
        boolean integerKey)
        throws Exception {
      org.junit.jupiter.api.Assumptions.assumeTrue(
          tech.streamfusion.compat.FlinkPlanCompat.sharedWindowCanonical()
              || shape.equals("TUMBLE"),
          "Flink 1.18 shared-pane timer continuation remains unqualified");
      this.integerKey = integerKey;
      this.processingTime = processingTime;
      this.tumbling = shape.equals("TUMBLE");
      this.windowOffset = windowOffset;
      this.session = shape.equals("SESSION");
      this.distinct = distinct;
      this.zone = ZoneId.of(zoneId);
      this.cumulative = shape.equals("CUMULATE");
      this.keyless = keyless;
      this.countStar = countStar;
      var config = new Configuration();
      config.setString("streamfusion.native.enabled", "false");
      config.setString("table.optimizer.agg-phase-strategy", "ONE_PHASE");
      var table = NativePlannerTestEnvironment.create(config);
      table.getConfig().setLocalTimeZone(zone);
      table.executeSql(
          "CREATE TABLE input_table (k "
              + (integerKey ? "INT" : "BIGINT")
              + ", v BIGINT, "
              + (processingTime
                  ? "rt AS PROCTIME()"
                  : "rt TIMESTAMP_LTZ(3), WATERMARK FOR rt AS rt")
              + ") WITH ('connector'='datagen')");
      table.executeSql(
          "CREATE TABLE output_table ("
              + (keyless ? "" : "k " + (integerKey ? "INT" : "BIGINT") + ", ")
              + "n BIGINT, total BIGINT, mean BIGINT, ws TIMESTAMP(3), we TIMESTAMP(3)) "
              + "WITH ('connector'='blackhole')");
      json =
          table
              .compilePlanSql(
                  "INSERT INTO output_table SELECT "
                      + (keyless ? "" : "k, ")
                      + "COUNT("
                      + (distinct ? "DISTINCT v" : countStar ? "*" : "v")
                      + "), SUM(v), AVG(v), window_start, window_end "
                      + "FROM TABLE("
                      + shape
                      + "(TABLE input_table"
                      + (session && !keyless ? " PARTITION BY k" : "")
                      + ", DESCRIPTOR(rt), INTERVAL '"
                      + (tumbling ? "3" : "1")
                      + "' SECOND"
                      + (session || tumbling ? "" : ", INTERVAL '3' SECOND")
                      + (windowOffset == 0
                          ? ""
                          : ", INTERVAL '"
                              + java.math.BigDecimal.valueOf(windowOffset, 3).toPlainString()
                              + "' SECOND")
                      + ")) GROUP BY "
                      + (keyless ? "" : "k, ")
                      + "window_start, window_end")
              .asJsonString();
      planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
      var node = node();
      input = (RowType) node.getInputEdges().get(0).getSource().getOutputType();
      output = (RowType) node.getOutputType();
      var reader = FlinkPlanCompat.createJsonObjectReader(planner.createSerdeContext());
      org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode definition = null;
      for (var entry : reader.readTree(json).path("nodes"))
        if (entry.path("id").asInt() == node.getId()) definition = entry;
      assertFalse(
          definition.path("needRetraction").asBoolean(),
          "This fixture must consume append-only input");
      var calls =
          reader
              .forType(org.apache.calcite.rel.core.AggregateCall[].class)
              .<org.apache.calcite.rel.core.AggregateCall[]>readValue(definition.path("aggCalls"));
      var windowing =
          reader
              .forType(org.apache.flink.table.planner.plan.logical.WindowingStrategy.class)
              .<org.apache.flink.table.planner.plan.logical.WindowingStrategy>readValue(
                  definition.path("windowing"));
      var information =
          FlinkPlanCompat.windowAggregates(
              planner.getTypeFactory(),
              input,
              org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala(
                  Arrays.asList(calls)),
              windowing.getWindow());
      accumulators =
          RowType.of(
              Arrays.stream(information.getAccTypes())
                  .map(org.apache.flink.table.types.DataType::getLogicalType)
                  .toArray(LogicalType[]::new));
      kinds = new int[information.aggInfos().length];
      starValues = new boolean[kinds.length];
      visibleAggregates = calls.length;
      var averages = new ArrayList<Integer>();
      int width = 0, countIndex = -1;
      for (int i = 0; i < kinds.length; i++) {
        if (width == information.getIndexOfCountStar()) countIndex = i;
        if (i == calls.length) {
          kinds[i] = 12;
          starValues[i] = true;
        } else {
          switch (calls[i].getAggregation().getKind()) {
            case COUNT:
              kinds[i] = calls[i].isDistinct() ? 7 : 3;
              break;
            case SUM:
              kinds[i] = 0;
              break;
            case SUM0:
              kinds[i] = 22;
              break;
            case AVG:
              kinds[i] = 4;
              averages.add(width);
              break;
            default:
              throw new AssertionError(calls[i]);
          }
          starValues[i] = calls[i].getArgList().isEmpty();
        }
        width += information.aggInfos()[i].externalAccTypes().length;
      }
      countStarIndex = countIndex;
      scalarFields = width;
      views = new ArrayList<>();
      viewDescriptors = new ArrayList<>();
      for (var view : information.distinctInfos()) {
        var spec = view.dataViewSpec().get();
        int[] slots = new int[view.aggIndexes().size()];
        for (int i = 0; i < slots.length; i++) slots[i] = (Integer) view.aggIndexes().apply(i);
        if (view.consumeRetraction())
          for (int slot : slots)
            kinds[slot] = NativeWindowOperatorCore.KIND_COUNT_DISTINCT_WITH_COUNTS;
        viewDescriptors.add(
            new org.apache.flink.api.common.state.MapStateDescriptor<>(
                spec.getStateId(),
                org.apache.flink.table.runtime.typeutils.ExternalSerializer.of(
                    spec.getKeyDataType()),
                org.apache.flink.table.runtime.typeutils.ExternalSerializer.of(
                    spec.getValueDataType())));
        views.add(
            new tech.streamfusion.state.StockWindowDistinctView(
                spec.getStateId(),
                view.keyType().getLogicalType(),
                org.apache.flink.table.runtime.typeutils.ExternalSerializer.of(
                    spec.getKeyDataType()),
                org.apache.flink.table.runtime.typeutils.ExternalSerializer.of(
                    spec.getValueDataType()),
                slots,
                view.consumeRetraction()));
      }
      averageOffsets = averages.stream().mapToInt(Integer::intValue).toArray();
      selector =
          KeySelectorUtil.getRowDataSelector(
              getClass().getClassLoader(),
              keyless ? new int[0] : new int[] {input.getFieldIndex("k")},
              InternalTypeInfo.of(input));
    }

    private StreamExecWindowAggregate node() throws Exception {
      ExecNodeGraph graph =
          FlinkPlanCompat.createJsonObjectReader(planner.createSerdeContext())
              .forType(ExecNodeGraph.class)
              .readValue(json);
      return find(graph.getRootNodes().get(0));
    }

    private static StreamExecWindowAggregate find(ExecNode<?> node) {
      if (node instanceof StreamExecWindowAggregate) return (StreamExecWindowAggregate) node;
      for (var edge : node.getInputEdges()) {
        var result = find(edge.getSource());
        if (result != null) return result;
      }
      return null;
    }

    @SuppressWarnings("unchecked")
    KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(boolean rocks)
        throws Exception {
      var transformation =
          (OneInputTransformation<RowData, RowData>) node().translateToPlan(planner);
      var operator =
          (OneInputStreamOperator<RowData, RowData>)
              ((SimpleOperatorFactory<?>) transformation.getOperatorFactory()).getOperator();
      return harness(operator, rocks);
    }

    KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
        OneInputStreamOperator<RowData, RowData> operator, boolean rocks) throws Exception {
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

    tech.streamfusion.state.StockStateContract contract() {
      if (session)
        return new tech.streamfusion.state.StockSessionWindowStateContract(
            selector.getProducedType().toSerializer(),
            selector.getProducedType().toRowType(),
            accumulators,
            1000,
            zone.getId(),
            averageOffsets,
            views,
            scalarFields,
            processingTime);
      return new StockSlicingWindowStateContract(
          selector.getProducedType().toSerializer(),
          selector.getProducedType().toRowType(),
          accumulators,
          3000,
          tumbling ? 3000 : 1000,
          !tumbling,
          countStarIndex,
          zone.getId(),
          averageOffsets,
          views,
          scalarFields,
          null,
          processingTime);
    }

    List<String> memberships(CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
      var rows = new ArrayList<String>();
      for (var descriptor : viewDescriptors) {
        try (var stream = backend.<Long>getKeysAndNamespaces(descriptor.getName())) {
          var iterator = stream.iterator();
          while (iterator.hasNext()) {
            var entry = iterator.next();
            backend.setCurrentKey(entry.f0);
            String key =
                keyless
                    ? "singleton"
                    : entry.f0.isNullAt(0) ? "NULL" : Long.toString(entry.f0.getLong(0));
            for (var member :
                backend
                    .getPartitionedState(
                        entry.f1,
                        org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE,
                        descriptor)
                    .entries()) {
              Object counter = member.getValue();
              rows.add(
                  descriptor.getName()
                      + "/"
                      + key
                      + "/"
                      + entry.f1
                      + "/"
                      + member.getKey()
                      + "/"
                      + (counter instanceof long[]
                          ? Arrays.toString((long[]) counter)
                          : counter.toString()));
            }
          }
        }
      }
      rows.sort(String::compareTo);
      return rows;
    }

    long create(boolean persistent, Path directory) {
      int[] types = new int[kinds.length];
      if (session) {
        long handle = Native.createSessionAggregator(1000, types, kinds, 1 << 26);
        if (!zone.getRules().isFixedOffset())
          Native.setSessionWindowCalendar(handle, new WindowCalendar(zone.getId(), processingTime));
        return handle;
      }
      long handle =
          persistent && !distinct
              ? Native.createRocksDBWindowAggregator(
                  3000,
                  tumbling ? 3000 : 1000,
                  cumulative,
                  types,
                  kinds,
                  keyless ? new int[0] : new int[] {0},
                  keyless
                      ? new int[] {Native.STOCK_KEY_PROJECTION}
                      : new int[] {Native.STOCK_KEY_PROJECTION, -1},
                  1 << 26,
                  directory.toString(),
                  128,
                  FlinkRocksDBOptions.from(new Configuration()).json(),
                  0,
                  new String[0],
                  new String[0],
                  0,
                  127,
                  false,
                  new byte[0][],
                  Long.MIN_VALUE)
              : cumulative
                  ? Native.createCumulativeAggregator(3000, 1000, types, kinds, 1 << 26)
                  : Native.createTumblingAggregator(
                      3000, tumbling ? 3000 : 1000, types, kinds, 1 << 26);
      Native.setFixedWindowOffset(handle, windowOffset);
      if (zone.getRules().isFixedOffset())
        Native.setFixedWindowAssignmentOffset(
            handle, zone.getRules().getOffset(Instant.EPOCH).getTotalSeconds() * 1000L);
      else Native.setFixedWindowCalendar(handle, new WindowCalendar(zone.getId(), processingTime));
      return handle;
    }

    void send(KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness, Input row)
        throws Exception {
      var result = new GenericRowData(input.getFieldCount());
      for (int i = 0; i < result.getArity(); i++) {
        String field = input.getFieldNames().get(i);
        result.setField(
            i,
            field.equals("k")
                ? keyValue(row.key)
                : field.equals("v")
                    ? row.value
                    : TimestampData.fromEpochMillis(BASE + row.timestamp));
      }
      harness.processElement(
          new StreamRecord<>(new RowDataSerializer(input).toBinaryRow(result).copy()));
    }

    void update(long handle, List<Input> rows) {
      var fields = new ArrayList<RowType.RowField>();
      if (!keyless) fields.add(new RowType.RowField("key0", new BigIntType()));
      for (int i = 0; i < kinds.length; i++)
        fields.add(new RowType.RowField("value" + i, new BigIntType()));
      fields.add(new RowType.RowField("ts", new BigIntType(false)));
      var values = new ArrayList<RowData>();
      for (var row : rows) {
        var value = new GenericRowData(fields.size());
        int offset = 0;
        if (!keyless) value.setField(offset++, row.key);
        for (int i = 0; i < kinds.length; i++) {
          Object field = row.value;
          if (starValues[i]) field = 1L;
          else if (kinds[i] == 3) field = row.value == null ? null : Long.valueOf(1);
          value.setField(offset++, field);
        }
        long timestamp = BASE + row.timestamp;
        value.setField(
            offset,
            zone.getRules().isFixedOffset()
                ? timestamp
                : TimeWindowUtil.toUtcTimestampMills(timestamp, zone));
        values.add(value);
      }
      try (var root =
              RowDataArrowConverter.write(values, new RowType(fields), NativeAllocator.SHARED);
          var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
          var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
        Data.exportVectorSchemaRoot(
            NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
        if (session)
          Native.updateSessionAggregator(handle, array.memoryAddress(), schema.memoryAddress());
        else if (processingTime)
          Native.updateLocalTumblingAggregator(
              handle, array.memoryAddress(), schema.memoryAddress());
        else Native.updateTumblingAggregator(handle, array.memoryAddress(), schema.memoryAddress());
      }
    }

    List<String> flush(long handle, long watermark) {
      var values = new ArrayList<String>();
      try (var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
          var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
        if (session)
          Native.flushSessionAggregator(
              handle,
              WindowTimeDomain.closeThreshold(watermark, 0),
              array.memoryAddress(),
              schema.memoryAddress());
        else
          Native.flushTumblingAggregator(
              handle,
              WindowTimeDomain.closeThreshold(watermark, 0),
              array.memoryAddress(),
              schema.memoryAddress());
        try (var root =
            Data.importVectorSchemaRoot(
                NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
          var fields = new ArrayList<LogicalType>();
          if (!keyless) fields.add(new BigIntType());
          for (int i = 0; i < 2 + kinds.length; i++) fields.add(new BigIntType());
          var reader =
              ArrowConversion.createArrowReader(
                  root, RowType.of(fields.toArray(LogicalType[]::new)));
          int shift = keyless ? 0 : 1;
          for (int i = 0; i < root.getRowCount(); i++) {
            var row = reader.read(i);
            long start = row.getLong(shift), end = row.getLong(shift + 1);
            if (zone.getRules().isFixedOffset()) {
              start = TimeWindowUtil.toUtcTimestampMills(start, zone);
              end = TimeWindowUtil.toUtcTimestampMills(end, zone);
            }
            var text = new StringBuilder(keyless ? "global" : keyText(row));
            for (int aggregate = 0; aggregate < visibleAggregates; aggregate++)
              text.append(':').append(number(row, shift + 2 + aggregate));
            values.add(text.append(':').append(start).append(':').append(end).toString());
          }
        }
      }
      values.sort(String::compareTo);
      return values;
    }

    List<String> output(KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
      var values = new ArrayList<String>();
      int shift = keyless ? 0 : 1;
      while (!harness.getOutput().isEmpty()) {
        var event = harness.getOutput().poll();
        if (event instanceof StreamRecord<?>) {
          var row = (RowData) ((StreamRecord<?>) event).getValue();
          var text = new StringBuilder(keyless ? "global" : keyText(row));
          for (int aggregate = 0; aggregate < visibleAggregates; aggregate++)
            text.append(':').append(number(row, shift + aggregate));
          values.add(
              text.append(':')
                  .append(row.getTimestamp(shift + visibleAggregates, 3).getMillisecond())
                  .append(':')
                  .append(row.getTimestamp(shift + visibleAggregates + 1, 3).getMillisecond())
                  .toString());
        }
      }
      values.sort(String::compareTo);
      return values;
    }

    Object keyValue(Long key) {
      if (key == null) return null;
      if (integerKey) return Integer.valueOf(Math.toIntExact(key));
      return key;
    }

    String keyText(RowData row) {
      if (row.isNullAt(0)) return "null";
      return integerKey ? Integer.toString(row.getInt(0)) : Long.toString(row.getLong(0));
    }

    private static String number(RowData row, int column) {
      return row.isNullAt(column) ? "null" : Long.toString(row.getLong(column));
    }
  }

  @SuppressWarnings("unchecked")
  private static CheckpointableKeyedStateBackend<RowData> backend(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> view) {
    return (CheckpointableKeyedStateBackend<RowData>)
        view.getOperator().<RowData>getKeyedStateBackend();
  }

  private static final class Projection extends AbstractStreamOperator<RowData>
      implements OneInputStreamOperator<RowData, RowData> {
    transient org.apache.flink.api.common.state.OperatorStateStore store;

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
      super.initializeState(context);
      store = context.getOperatorStateStore();
    }

    @Override
    public void processElement(StreamRecord<RowData> row) {}
  }
}

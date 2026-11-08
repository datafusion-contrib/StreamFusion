package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowRank;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeAll;
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
import tech.streamfusion.state.StockWindowRankStateContract;

class CanonicalWindowRankInteropTest {
  static final long BASE = Instant.parse("2024-03-31T00:59:57Z").toEpochMilli();
  static final SavepointType CANONICAL = SavepointType.savepoint(SavepointFormatType.CANONICAL);

  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String shape : List.of("TUMBLE", "HOP", "CUMULATE"))
      for (String zone : List.of("UTC", "+05:45", "Europe/Berlin", "America/New_York"))
        for (boolean rocks : new boolean[] {false, true})
          for (boolean keyless : new boolean[] {false, true})
            for (boolean descending : new boolean[] {false, true})
              for (boolean outputRank : new boolean[] {false, true})
                cases.add(Arguments.of(shape, zone, rocks, keyless, descending, outputRank));
    return cases.stream();
  }

  static Stream<Arguments> corruptions() {
    return Stream.of(
            "missingTimer",
            "wrongTimer",
            "processingTimer",
            "missingState",
            "orphanTimer",
            "wrongEnd",
            "wrongStart",
            "wrongPartition",
            "wrongSort",
            "retraction",
            "emptyList",
            "unknownState")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        stockRocks ->
                            Stream.of(
                                Arguments.of(mode, stockRocks, false),
                                Arguments.of(mode, stockRocks, true))));
  }

  @ParameterizedTest
  @MethodSource("corruptions")
  void invalidOwnershipFailsBeforeImportingAnyRankRows(
      String mode, boolean stockRocks, boolean rocks) throws Exception {
    var plan = new StockPlan("TUMBLE", "UTC", false, false, true);
    try (var source = plan.stock(!stockRocks);
        var view = plan.view(stockRocks);
        var target = plan.nativeHarness(rocks)) {
      source.open();
      plan.send(source, plan.row(1L, 10L, 1, BASE + 3000));
      var row = plan.row(2L, 20L, 2, BASE + 6000);
      plan.send(source, row);
      source.processWatermark(new Watermark(BASE + 999));
      source.prepareSnapshotPreBarrier(1);
      view.initializeState(
          source.snapshotWithLocalState(1, 0, CANONICAL).getJobManagerOwnedState());
      view.open();
      var backend =
          (org.apache.flink.runtime.state.CheckpointableKeyedStateBackend<RowData>)
              view.getOperator().<RowData>getKeyedStateBackend();
      var key = plan.selector.getKey(row);
      backend.setCurrentKey(key);
      var longs = org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE;
      var state =
          backend.getPartitionedState(
              BASE + 6000,
              longs,
              new org.apache.flink.api.common.state.MapStateDescriptor<RowData, List<RowData>>(
                  "window_rank",
                  plan.sortSelector.getProducedType().toSerializer(),
                  new org.apache.flink.api.common.typeutils.base.ListSerializer<>(
                      new RowDataSerializer(plan.input))));
      var timers =
          backend.create(
              "_timer_state/event_window-timers",
              new org.apache.flink.streaming.api.operators.TimerSerializer<>(
                  plan.selector.getProducedType().toSerializer(), longs));
      if (mode.equals("missingState")) state.clear();
      else if (mode.equals("emptyList")) state.put(plan.sortSelector.getKey(row), List.of());
      else if (mode.equals("unknownState"))
        backend
            .getPartitionedState(
                BASE + 6000,
                longs,
                new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                    "unowned-rank-state", longs))
            .update(1L);
      else if (mode.equals("missingTimer") || mode.equals("wrongTimer")) {
        var timer = timers.poll();
        assertNotNull(timer);
        if (mode.equals("wrongTimer"))
          timers.add(
              new org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<>(
                  timer.getTimestamp() + 1, timer.getKey(), timer.getNamespace()));
      } else if (mode.equals("processingTimer")) {
        backend
            .create(
                "_timer_state/processing_window-timers",
                new org.apache.flink.streaming.api.operators.TimerSerializer<>(
                    plan.selector.getProducedType().toSerializer(), longs))
            .add(
                new org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<>(
                    BASE + 5999, key, BASE + 6000));
      } else if (mode.equals("orphanTimer"))
        timers.add(
            new org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<>(
                BASE + 8999, key, BASE + 9000));
      else {
        var changed = new GenericRowData(plan.input.getFieldCount());
        for (int i = 0; i < changed.getArity(); i++)
          changed.setField(
              i, RowData.createFieldGetter(plan.input.getTypeAt(i), i).getFieldOrNull(row));
        if (mode.equals("wrongEnd"))
          changed.setField(plan.end, TimestampData.fromEpochMillis(BASE + 6001));
        else if (mode.equals("wrongStart"))
          changed.setField(plan.start, TimestampData.fromEpochMillis(BASE + 3001));
        else if (mode.equals("wrongPartition")) changed.setField(plan.partitions[0], 999L);
        else if (mode.equals("wrongSort")) changed.setField(plan.sortColumns[0], 999L);
        else if (mode.equals("retraction"))
          changed.setRowKind(org.apache.flink.types.RowKind.DELETE);
        else fail(mode);
        state.put(plan.sortSelector.getKey(row), List.of(changed));
      }
      var saved = view.snapshotWithLocalState(2, 0, CANONICAL).getJobManagerOwnedState();
      var failure = assertThrows(Exception.class, () -> target.initializeState(saved));
      Throwable cause = failure;
      while (cause.getCause() != null) cause = cause.getCause();
      assertInstanceOf(IllegalStateException.class, cause);
      var operator = (NativeColumnarWindowRankOperator) target.getOperator();
      if (mode.equals("processingTimer") || mode.equals("unknownState")) {
        assertTrue(
            cause
                .getMessage()
                .contains(
                    mode.equals("processingTimer")
                        ? "native adapter does not consume these pending timers"
                        : "descriptor is not owned by the selected native adapter"),
            cause.toString());
        assertEquals(
            0,
            operator.handle,
            "Inventory must reject unowned state before allocating native state");
        return;
      }
      assertTrue(cause.getMessage().contains("window-rank"), cause.toString());
      assertNotEquals(0, operator.handle, "Native handle must exist before contract preflight");
      assertEquals(rocks, operator.directRocksDBState());
      var imported = new java.util.concurrent.atomic.AtomicInteger();
      Native.exportWindowRankerCanonical(
          operator.handle,
          (arrayAddress, schemaAddress) -> {
            try (var array = org.apache.arrow.c.ArrowArray.wrap(arrayAddress);
                var schema = org.apache.arrow.c.ArrowSchema.wrap(schemaAddress);
                var root =
                    org.apache.arrow.c.Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              imported.addAndGet(root.getRowCount());
            }
          });
      assertEquals(0, imported.get(), "Preflight must cover all stock rows before JNI import");
    }
  }

  static Stream<Arguments> emptyCases() {
    return Stream.of("UTC", "+05:45", "Europe/Berlin", "America/New_York")
        .flatMap(zone -> Stream.of(Arguments.of(zone, false), Arguments.of(zone, true)));
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "false,false",
    "false,true",
    "true,false",
    "true,true"
  })
  void oneStockSortListSpanningTransferChunksKeepsEarliestTies(boolean rocks, boolean outputRank)
      throws Exception {
    var plan = new StockPlan("TUMBLE", "UTC", false, false, outputRank);
    try (var source = plan.stock(!rocks);
        var reference = plan.stock(rocks);
        var nativeRun = plan.nativeHarness(rocks)) {
      source.open();
      for (long tag = 0; tag < 2500; tag++) plan.send(source, plan.row(1L, 10L, tag, BASE + 3000));
      source.prepareSnapshotPreBarrier(1);
      var saved = source.snapshotWithLocalState(1, 0, CANONICAL).getJobManagerOwnedState();
      reference.initializeState(saved);
      reference.open();
      nativeRun.initializeState(saved);
      nativeRun.open();
      nativeRun.prepareSnapshotPreBarrier(2);
      var exported = nativeRun.snapshotWithLocalState(2, 0, CANONICAL).getJobManagerOwnedState();
      try (var stockAgain = plan.stock(!rocks)) {
        stockAgain.initializeState(exported);
        stockAgain.open();
        for (long score : new long[] {5, 10, 100}) {
          var row = plan.row(1L, score, 2500 + score, BASE + 3000);
          plan.send(reference, row);
          plan.send(stockAgain, row);
          plan.sendNative(nativeRun, row);
        }
        reference.processWatermark(Watermark.MAX_WATERMARK);
        stockAgain.processWatermark(Watermark.MAX_WATERMARK);
        nativeRun.processWatermark(Watermark.MAX_WATERMARK);
        var expected = plan.output(reference);
        assertEquals(3, expected.size());
        assertEquals(expected, plan.output(stockAgain));
        assertEquals(expected, plan.nativeOutput(nativeRun));
      }
    }
  }

  @ParameterizedTest
  @MethodSource("emptyCases")
  void emptySavepointsRetainWatermarkBeforeLateInputAndImmediateReexport(String zone, boolean rocks)
      throws Exception {
    var plan = new StockPlan("TUMBLE", zone, false, false, true);
    try (var source = plan.stock(!rocks);
        var reference = plan.stock(rocks);
        var nativeRun = plan.nativeHarness(rocks)) {
      source.open();
      source.processWatermark(new Watermark(BASE + 2999));
      source.prepareSnapshotPreBarrier(1);
      var saved = source.snapshotWithLocalState(1, 0, CANONICAL).getJobManagerOwnedState();
      reference.initializeState(saved);
      reference.open();
      nativeRun.initializeState(saved);
      nativeRun.open();
      assertEquals(
          rocks, ((NativeColumnarWindowRankOperator) nativeRun.getOperator()).directRocksDBState());
      nativeRun.prepareSnapshotPreBarrier(2);
      var exported = nativeRun.snapshotWithLocalState(2, 0, CANONICAL).getJobManagerOwnedState();
      try (var stockAgain = plan.stock(!rocks)) {
        stockAgain.initializeState(exported);
        stockAgain.open();
        nativeRun.processWatermark(new Watermark(BASE - 1));
        long local =
            org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills(
                BASE, ZoneId.of(zone));
        for (long end : new long[] {local + 3000, local + 6000}) {
          var row = plan.row(1L, 10L, end, end);
          plan.send(reference, row);
          plan.send(stockAgain, row);
          plan.sendNative(nativeRun, row);
        }
        reference.processWatermark(Watermark.MAX_WATERMARK);
        stockAgain.processWatermark(Watermark.MAX_WATERMARK);
        nativeRun.processWatermark(Watermark.MAX_WATERMARK);
        var expected = plan.output(reference);
        assertEquals(1, expected.size(), "Only the future window may emit");
        assertEquals(expected, plan.output(stockAgain));
        assertEquals(expected, plan.nativeOutput(nativeRun));
      }
    }
  }

  @ParameterizedTest(name = "{0} {1} rocks={2} keyless={3} descending={4}")
  @MethodSource("cases")
  void releasedSortListsAndTieArrivalOrderContinueAcrossEngineSwitches(
      String shape,
      String zone,
      boolean rocks,
      boolean keyless,
      boolean descending,
      boolean outputRank)
      throws Exception {
    var plan = new StockPlan(shape, zone, keyless, descending, outputRank);
    try (var source = plan.stock(!rocks);
        var reference = plan.stock(rocks);
        var nativeRun = plan.nativeHarness(rocks)) {
      source.open();
      long base =
          org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills(
              BASE, ZoneId.of(zone));
      long[] ends =
          shape.equals("CUMULATE")
              ? new long[] {base + 1000, base + 2000, base + 3000, base + 6000}
              : new long[] {base + 3000, base + 6000};
      long tag = 0;
      for (long end : ends)
        for (Long key : new Long[] {-1L, null, 2L})
          for (Long score : new Long[] {10L, null, 10L, 30L})
            plan.send(source, plan.row(key, score, tag++, end));
      source.processWatermark(new Watermark(BASE + 999));
      source.prepareSnapshotPreBarrier(1);
      var saved = source.snapshotWithLocalState(1, 0, CANONICAL).getJobManagerOwnedState();
      reference.initializeState(saved);
      reference.open();
      nativeRun.initializeState(saved);
      nativeRun.open();
      assertEquals(
          rocks, ((NativeColumnarWindowRankOperator) nativeRun.getOperator()).directRocksDBState());
      nativeRun.prepareSnapshotPreBarrier(2);
      var immediate = nativeRun.snapshotWithLocalState(2, 0, CANONICAL).getJobManagerOwnedState();
      try (var stockAgain = plan.stock(!rocks)) {
        stockAgain.initializeState(immediate);
        stockAgain.open();
        for (long end : ends) {
          for (Long score : new Long[] {10L, -5L, 100L}) {
            var row = plan.row(-1L, score, tag++, end);
            plan.send(reference, row);
            plan.send(stockAgain, row);
            plan.sendNative(nativeRun, row);
          }
        }
        nativeRun.prepareSnapshotPreBarrier(3);
        var checkpoint = nativeRun.snapshot(3, 0);
        assertTrue(
            checkpoint.getManagedKeyedState().stream()
                .noneMatch(
                    state ->
                        state instanceof org.apache.flink.runtime.state.SavepointKeyedStateHandle));
        assertFalse(
            checkpoint.getManagedKeyedState().isEmpty() && checkpoint.getRawKeyedState().isEmpty());
        try (var resumed = plan.nativeHarness(rocks)) {
          resumed.initializeState(checkpoint);
          resumed.open();
          resumed.prepareSnapshotPreBarrier(4);
          var exported = resumed.snapshotWithLocalState(4, 0, CANONICAL).getJobManagerOwnedState();
          try (var stockAfterCheckpoint = plan.stock(!rocks)) {
            stockAfterCheckpoint.initializeState(exported);
            stockAfterCheckpoint.open();
            int emitted = 0;
            for (long wm : new long[] {BASE + 1999, BASE + 2999, BASE + 5999, Long.MAX_VALUE}) {
              reference.processWatermark(new Watermark(wm));
              stockAgain.processWatermark(new Watermark(wm));
              stockAfterCheckpoint.processWatermark(new Watermark(wm));
              resumed.processWatermark(new Watermark(wm));
              var expected = plan.output(reference);
              emitted += expected.size();
              assertEquals(expected, plan.output(stockAgain));
              assertEquals(expected, plan.output(stockAfterCheckpoint));
              assertEquals(expected, plan.nativeOutput(resumed));
            }
            assertTrue(emitted > 0);
          }
        }
      }
    }
  }

  static final class StockPlan {
    final PlannerBase planner;
    final String json;
    final RowType input, output;
    final int[] partitions, sortColumns, ascending, nullsFirst;
    final int start, end;
    final RowDataKeySelector selector, sortSelector;
    final String zone, shape;
    final boolean outputRank, deduplicate, keepLast;
    final StockWindowRankStateContract.WindowStart windowStart;

    StockPlan(String shape, String zone, boolean keyless, boolean descending, boolean outputRank)
        throws Exception {
      this(shape, zone, keyless, descending, outputRank, false);
    }

    StockPlan(
        String shape,
        String zone,
        boolean keyless,
        boolean descending,
        boolean outputRank,
        boolean deduplicate)
        throws Exception {
      this.outputRank = outputRank;
      this.deduplicate = deduplicate;
      this.keepLast = deduplicate && descending;
      this.zone = zone;
      this.shape = shape;
      var config = new Configuration();
      config.setString("streamfusion.native.enabled", "false");
      var table = NativePlannerTestEnvironment.create(config);
      table.getConfig().setLocalTimeZone(ZoneId.of(zone));
      table.executeSql(
          "CREATE TABLE rank_input (k BIGINT, score BIGINT, tag BIGINT, rt TIMESTAMP_LTZ(3),"
              + " WATERMARK FOR rt AS rt) WITH ('connector'='datagen')");
      table.executeSql(
          "CREATE TABLE rank_output (k BIGINT, score BIGINT, tag BIGINT, ws TIMESTAMP(3), we"
              + " TIMESTAMP(3)"
              + (outputRank ? ", rn BIGINT" : "")
              + ") WITH ('connector'='blackhole')");
      String sizes =
          shape.equals("TUMBLE")
              ? "INTERVAL '3' SECOND"
              : "INTERVAL '1' SECOND, INTERVAL '3' SECOND";
      json =
          table
              .compilePlanSql(
                  "INSERT INTO rank_output SELECT k, score, tag, window_start, window_end"
                      + (outputRank ? ", rn" : "")
                      + " FROM (SELECT k, score, tag, window_start, window_end, ROW_NUMBER() OVER"
                      + " (PARTITION BY "
                      + (keyless ? "" : "k, ")
                      + "window_start, window_end ORDER BY "
                      + (deduplicate ? "rt " : "score ")
                      + (descending ? "DESC NULLS LAST" : "ASC NULLS FIRST")
                      + ") rn FROM TABLE("
                      + shape
                      + "(TABLE rank_input, DESCRIPTOR(rt), "
                      + sizes
                      + (deduplicate ? "))) WHERE rn = 1" : "))) WHERE rn <= 3"))
              .asJsonString();
      planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
      var node = node();
      input = (RowType) node.getInputEdges().get(0).getOutputType();
      output = (RowType) node.getOutputType();
      var reader = FlinkPlanCompat.createJsonObjectReader(planner.createSerdeContext());
      org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode definition = null;
      for (var value : reader.readTree(json).path("nodes"))
        if (value.path("id").asInt() == node.getId()) definition = value;
      var window =
          reader
              .forType(
                  org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy.class)
              .<org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy>
                  readValue(definition.path("windowing"));
      if (deduplicate) {
        partitions = reader.forType(int[].class).readValue(definition.path("partitionKeys"));
        sortColumns = new int[] {definition.path("orderKey").asInt()};
        ascending = new int[] {descending ? 0 : 1};
        nullsFirst = new int[] {0};
      } else {
        var partition =
            reader
                .forType(org.apache.flink.table.planner.plan.nodes.exec.spec.PartitionSpec.class)
                .<org.apache.flink.table.planner.plan.nodes.exec.spec.PartitionSpec>readValue(
                    definition.path("partitionSpec"));
        var sort =
            reader
                .forType(org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec.class)
                .<org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec>readValue(
                    definition.path("sortSpec"));
        partitions = partition.getFieldIndices();
        sortColumns = sort.getFieldIndices();
        ascending = new int[sortColumns.length];
        nullsFirst = new int[sortColumns.length];
        for (int i = 0; i < sortColumns.length; i++) {
          ascending[i] = sort.getFieldSpec(i).getIsAscendingOrder() ? 1 : 0;
          nullsFirst[i] = sort.getFieldSpec(i).getNullIsLast() ? 0 : 1;
        }
      }
      start = window.getWindowStart();
      end = window.getWindowEnd();
      selector =
          KeySelectorUtil.getRowDataSelector(
              getClass().getClassLoader(), partitions, InternalTypeInfo.of(input));
      sortSelector =
          KeySelectorUtil.getRowDataSelector(
              getClass().getClassLoader(), sortColumns, InternalTypeInfo.of(input));
      var assigner =
          FlinkPlanCompat.windowStart(window.getWindow(), ZoneId.of(zone), 3000, 1000, null);
      windowStart = assigner::start;
    }

    ExecNode<?> node() throws Exception {
      var graph =
          FlinkPlanCompat.createJsonObjectReader(planner.createSerdeContext())
              .forType(ExecNodeGraph.class)
              .<ExecNodeGraph>readValue(json);
      return find(graph.getRootNodes().get(0));
    }

    private static ExecNode<?> find(ExecNode<?> node) {
      if (node instanceof StreamExecWindowRank
          || node
              instanceof
              org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowDeduplicate)
        return node;
      for (var edge : node.getInputEdges()) {
        var found = find(edge.getSource());
        if (found != null) return found;
      }
      return null;
    }

    StockWindowRankStateContract contract() {
      if (deduplicate)
        return StockWindowRankStateContract.deduplicate(
            selector.getProducedType().toSerializer(),
            selector.getProducedType().toRowType(),
            input,
            partitions,
            sortColumns[0],
            start,
            end,
            zone,
            windowStart);
      return new StockWindowRankStateContract(
          selector.getProducedType().toSerializer(),
          selector.getProducedType().toRowType(),
          sortSelector.getProducedType().toSerializer(),
          sortSelector.getProducedType().toRowType(),
          input,
          partitions,
          sortColumns,
          start,
          end,
          zone,
          windowStart);
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

    KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> view(boolean rocks)
        throws Exception {
      return harness(new Projection(), rocks);
    }

    private KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
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

    private int[] keyLayout() {
      int[] layout = new int[partitions.length + 1];
      layout[0] = Native.STOCK_KEY_PROJECTION;
      for (int i = 0; i < partitions.length; i++) {
        var type = input.getTypeAt(partitions[i]);
        layout[i + 1] =
            type.is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                    || type.is(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                ? org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision(type)
                : -1;
      }
      return layout;
    }

    KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness(
        boolean rocks) throws Exception {
      return nativeHarness(rocks, deduplicate ? 1 : 3);
    }

    KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness(
        boolean rocks, int limit) throws Exception {
      var operator =
          new NativeColumnarWindowRankOperator(
                  start,
                  end,
                  partitions,
                  keyLayout(),
                  sortColumns,
                  ascending,
                  nullsFirst,
                  limit,
                  outputRank,
                  ZoneId.of(zone).getRules().isFixedOffset()
                      ? ZoneId.of(zone).getRules().getOffset(Instant.EPOCH).getTotalSeconds()
                          * 1000L
                      : 0,
                  false,
                  3000,
                  shape.equals("TUMBLE") ? 3000 : 1000,
                  shape.equals("CUMULATE"),
                  input,
                  128,
                  keepLast)
              .withCalendarZone(zone);
      var contract = contract();
      operator.setStockStateContract(contract);
      var harness =
          new KeyedOneInputStreamOperatorTestHarness<>(
              operator, ignored -> 0, Types.INT, 128, 1, 0);
      var config = new Configuration();
      if (rocks)
        config.setString(
            org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND.key(),
            RocksDBNativeStateBackendFactory.class.getName());
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

    RowData row(Long key, Long score, long tag, long windowEnd) {
      var row = new GenericRowData(input.getFieldCount());
      for (int i = 0; i < row.getArity(); i++) {
        String name = input.getFieldNames().get(i);
        Object value;
        if (i == start) value = windowStart.start(windowEnd);
        else if (i == end) value = windowEnd;
        else if (name.equals("k")) value = key;
        else if (name.equals("score")) value = score;
        else if (name.equals("tag")) value = tag;
        else value = windowEnd - 1;
        if (input.getTypeAt(i).is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
            || input.getTypeAt(i).is(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE))
          value = TimestampData.fromEpochMillis((Long) value);
        row.setField(i, value);
      }
      return new RowDataSerializer(input).toBinaryRow(row).copy();
    }

    void send(
        KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness, RowData row)
        throws Exception {
      harness.processElement(new StreamRecord<>(row));
    }

    void sendNative(
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
        RowData row)
        throws Exception {
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(List.of(row), input, NativeAllocator.SHARED))));
    }

    List<String> output(KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
      var rows = new ArrayList<String>();
      while (!harness.getOutput().isEmpty()) {
        var event = harness.getOutput().poll();
        if (event instanceof StreamRecord<?>)
          rows.add(format((RowData) ((StreamRecord<?>) event).getValue()));
      }
      rows.sort(String::compareTo);
      return rows;
    }

    List<String> nativeOutput(
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
      var rows = new ArrayList<String>();
      while (!harness.getOutput().isEmpty()) {
        var event = harness.getOutput().poll();
        if (!(event instanceof StreamRecord<?>)) continue;
        try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          var reader = ArrowConversion.createArrowReader(root, output);
          for (int i = 0; i < root.getRowCount(); i++) rows.add(format(reader.read(i)));
        }
      }
      rows.sort(String::compareTo);
      return rows;
    }

    private String format(RowData row) {
      var text = new StringBuilder(row.getRowKind().toString());
      for (int i = 0; i < output.getFieldCount(); i++)
        text.append(':')
            .append(RowData.createFieldGetter(output.getTypeAt(i), i).getFieldOrNull(row));
      return text.toString();
    }
  }

  private static final class Projection
      extends org.apache.flink.streaming.api.operators.AbstractStreamOperator<RowData>
      implements OneInputStreamOperator<RowData, RowData> {
    @Override
    public void processElement(StreamRecord<RowData> row) {}
  }
}

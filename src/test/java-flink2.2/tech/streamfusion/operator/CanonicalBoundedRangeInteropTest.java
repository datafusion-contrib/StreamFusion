package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.*;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.*;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.*;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.*;
import org.apache.flink.table.planner.plan.nodes.exec.serde.CompiledPlanSerdeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecOverAggregate;
import org.apache.flink.table.planner.plan.utils.*;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.over.RowTimeRangeBoundedPrecedingFunction;
import org.apache.flink.table.runtime.typeutils.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import tech.streamfusion.Native;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.*;

class CanonicalBoundedRangeInteropTest {
  @BeforeAll
  static void memory() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> modes() {
    var out = new ArrayList<Arguments>();
    for (String type : List.of("BIGINT", "FLOAT", "DOUBLE", "DECIMAL(38,2)"))
      for (boolean keyless : new boolean[] {false, true})
        for (boolean stock : new boolean[] {false, true})
          for (boolean nativeStore : new boolean[] {false, true})
            for (boolean pendingOnly : new boolean[] {false, true})
              out.add(Arguments.of(type, keyless, stock, nativeStore, pendingOnly));
    return out.stream();
  }

  @ParameterizedTest
  @MethodSource("modes")
  void realStockCanonicalStateContinuesThroughNativeAndBack(
      String type, boolean keyless, boolean stockStore, boolean nativeStore, boolean pendingOnly)
      throws Exception {
    var plan = new Plan(type, keyless);
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    OperatorSubtaskState original;
    List<String> expected;
    try (var stock = plan.stock(stockStore)) {
      stock.open();
      plan.prefix(stock, pendingOnly);
      original = snapshot(stock);
      plan.suffix(stock);
      expected = plan.stockOutput(stock);
      assertFalse(expected.isEmpty());
    }
    OperatorSubtaskState projected;
    try (var nativeOp = plan.nativeHarness(nativeStore)) {
      nativeOp.initializeState(original);
      nativeOp.open();
      projected = snapshot(nativeOp);
      plan.suffixNative(nativeOp);
      assertEquals(expected, plan.nativeOutput(nativeOp));
    }
    try (var stock = plan.stock(stockStore)) {
      stock.initializeState(projected);
      stock.open();
      plan.suffix(stock);
      assertEquals(expected, plan.stockOutput(stock));
    }
    OperatorSubtaskState fromNative;
    try (var nativeOp = plan.nativeHarness(nativeStore)) {
      nativeOp.open();
      plan.prefixNative(nativeOp, pendingOnly);
      fromNative = snapshot(nativeOp);
      plan.suffixNative(nativeOp);
      assertEquals(expected, plan.nativeOutput(nativeOp));
    }
    try (var stock = plan.stock(stockStore)) {
      stock.initializeState(fromNative);
      stock.open();
      plan.suffix(stock);
      assertEquals(expected, plan.stockOutput(stock));
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private static OperatorSubtaskState snapshot(
      org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness<?> harness)
      throws Exception {
    return harness
        .snapshotWithLocalState(1, 100, SavepointType.savepoint(SavepointFormatType.CANONICAL))
        .getJobManagerOwnedState();
  }

  static Stream<Arguments> wrappedModes() {
    var out = new ArrayList<Arguments>();
    for (boolean keyless : new boolean[] {false, true})
      for (boolean stock : new boolean[] {false, true})
        for (boolean nativeStore : new boolean[] {false, true})
          for (int phase = 0; phase < 3; phase++)
            out.add(Arguments.of(keyless, stock, nativeStore, phase));
    return out.stream();
  }

  static Stream<Arguments> corruptions() {
    var out = new ArrayList<Arguments>();
    for (String mode :
        List.of(
            "hiddenCount",
            "missingCleanup",
            "missingAccumulator",
            "wrongTime",
            "wrongKey",
            "retraction",
            "emptyPeers",
            "missingPendingTimer",
            "missingCleanupTimer",
            "processingTimer",
            "unknownState"))
      for (boolean stock : new boolean[] {false, true})
        for (boolean nativeStore : new boolean[] {false, true})
          out.add(Arguments.of(mode, stock, nativeStore));
    return out.stream();
  }

  @ParameterizedTest
  @MethodSource("corruptions")
  @SuppressWarnings("unchecked")
  void malformedStockRangeStateFailsBeforeInstallation(
      String mode, boolean stockStore, boolean nativeStore) throws Exception {
    var plan = new Plan("BIGINT", false);
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var stock = plan.stock(stockStore);
        var view = plan.view(stockStore);
        var target = plan.nativeHarness(nativeStore)) {
      stock.open();
      plan.prefix(stock, false);
      view.initializeState(snapshot(stock));
      view.open();
      var backend =
          (org.apache.flink.runtime.state.CheckpointableKeyedStateBackend<RowData>)
              view.getOperator().<RowData>getKeyedStateBackend();
      var key =
          plan.selector.getKey(
              new RowDataSerializer(plan.input).toBinaryRow(plan.row(1L, 7, 1064)).copy());
      backend.setCurrentKey(key);
      var ns = VoidNamespace.INSTANCE;
      var namespaces = VoidNamespaceSerializer.INSTANCE;
      var longs = org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE;
      var pending =
          backend.getPartitionedState(
              ns,
              namespaces,
              new org.apache.flink.api.common.state.MapStateDescriptor<Long, List<RowData>>(
                  "inputState",
                  longs,
                  new org.apache.flink.api.common.typeutils.base.ListSerializer<>(
                      new RowDataSerializer(plan.input))));
      var cleanup =
          backend.getPartitionedState(
              ns,
              namespaces,
              new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                  "cleanupTsState", longs));
      var accumulator =
          backend.getPartitionedState(
              ns,
              namespaces,
              new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                  "accState", new RowDataSerializer(plan.accumulator)));
      var timers =
          new TimerSerializer<>(plan.selector.getProducedType().toSerializer(), namespaces);
      var events = backend.create("_timer_state/event_user-timers", timers);
      String expected;
      switch (mode) {
        case "hiddenCount":
          assertTrue(plan.hiddenCount >= 0);
          var saved = accumulator.value();
          var changed = new GenericRowData(plan.accumulator.getFieldCount());
          for (int i = 0; i < changed.getArity(); i++)
            changed.setField(
                i,
                RowData.createFieldGetter(plan.accumulator.getTypeAt(i), i).getFieldOrNull(saved));
          changed.setField(plan.hiddenCount, 99L);
          accumulator.update(changed);
          expected = "hidden input count";
          break;
        case "missingCleanup":
          cleanup.clear();
          expected = "no cleanup owner";
          break;
        case "missingAccumulator":
          accumulator.clear();
          expected = "accumulator/frame ownership";
          break;
        case "wrongTime":
        case "wrongKey":
        case "retraction":
          var bad =
              plan.row(
                  mode.equals("wrongKey") ? 2L : 1L, 7, mode.equals("wrongTime") ? 1065 : 1064);
          if (mode.equals("retraction")) bad.setRowKind(org.apache.flink.types.RowKind.DELETE);
          pending.put(1064L, List.of(bad));
          expected = "key/time/kind";
          break;
        case "emptyPeers":
          pending.put(1064L, List.of());
          expected = "Empty RANGE timestamp bucket";
          break;
        case "missingPendingTimer":
        case "missingCleanupTimer":
          long timestamp = mode.equals("missingPendingTimer") ? 1064 : cleanup.value();
          var retained = new ArrayList<TimerHeapInternalTimer<RowData, VoidNamespace>>();
          boolean removed = false;
          while (!events.isEmpty()) {
            var timer = events.poll();
            if (timer.getTimestamp() == timestamp && timer.getKey().equals(key)) removed = true;
            else retained.add(timer);
          }
          for (var timer : retained) events.add(timer);
          assertTrue(removed, "The actual stock timer must exist before corruption");
          expected = mode.equals("missingPendingTimer") ? "no event timer" : "no matching timer";
          break;
        case "processingTimer":
          backend
              .create("_timer_state/processing_user-timers", timers)
              .add(new TimerHeapInternalTimer<>(2000, key, ns));
          expected = "timer";
          break;
        case "unknownState":
          backend
              .getPartitionedState(
                  ns,
                  namespaces,
                  new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                      "unowned-range-state", longs))
              .update(1L);
          expected = "not owned";
          break;
        default:
          throw new AssertionError(mode);
      }
      var invalid = snapshot(view);
      var failure = assertThrows(Exception.class, () -> target.initializeState(invalid));
      var messages = new StringBuilder();
      for (Throwable cause = failure; cause != null; cause = cause.getCause())
        messages.append(cause.getMessage()).append('\n');
      assertTrue(messages.toString().contains(expected), messages.toString());
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @ParameterizedTest
  @MethodSource("wrappedModes")
  void canonicalStatePreservesWrappedCleanupAndOrphanCallbacks(
      boolean keyless, boolean stockStore, boolean nativeStore, int phase) throws Exception {
    var plan = new Plan("BIGINT", keyless);
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    OperatorSubtaskState original;
    List<String> expected;
    try (var stock = plan.stock(stockStore)) {
      stock.open();
      plan.pushStock(stock, plan.row(1L, 97, Long.MAX_VALUE - 10));
      if (phase > 0) stock.processWatermark(new Watermark(0));
      if (phase > 1) stock.processWatermark(new Watermark(Long.MAX_VALUE));
      assertTrue(plan.stockOutput(stock).isEmpty());
      original = snapshot(stock);
      plan.wrappedSuffix(stock);
      expected = plan.stockOutput(stock);
      assertEquals(phase == 2 ? 0 : 1, expected.size());
    }
    OperatorSubtaskState projected;
    try (var nativeOp = plan.nativeHarness(nativeStore)) {
      nativeOp.initializeState(original);
      nativeOp.open();
      projected = snapshot(nativeOp);
      plan.wrappedSuffixNative(nativeOp);
      assertEquals(expected, plan.nativeOutput(nativeOp));
    }
    try (var stock = plan.stock(stockStore)) {
      stock.initializeState(projected);
      stock.open();
      plan.wrappedSuffix(stock);
      assertEquals(expected, plan.stockOutput(stock));
    }
    OperatorSubtaskState fromNative;
    try (var nativeOp = plan.nativeHarness(nativeStore)) {
      nativeOp.open();
      plan.push(nativeOp, plan.row(1L, 97, Long.MAX_VALUE - 10));
      if (phase > 0) nativeOp.processWatermark(new Watermark(0));
      if (phase > 1) nativeOp.processWatermark(new Watermark(Long.MAX_VALUE));
      assertTrue(plan.nativeOutput(nativeOp).isEmpty());
      fromNative = snapshot(nativeOp);
      plan.wrappedSuffixNative(nativeOp);
      assertEquals(expected, plan.nativeOutput(nativeOp));
    }
    try (var stock = plan.stock(stockStore)) {
      stock.initializeState(fromNative);
      stock.open();
      plan.wrappedSuffix(stock);
      assertEquals(expected, plan.stockOutput(stock));
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  static final class Plan {
    final PlannerBase planner;
    final String json;
    final RowType input, output, accumulator, keyType;
    final int[] keys, kinds, values, codes, widths;
    final int hiddenCount, timeColumn;
    final boolean processingRows;
    final int offset;
    final long ttl;
    final RowDataKeySelector selector;
    final tech.streamfusion.planner.OverConstantArguments constants;

    Plan(String type, boolean keyless) throws Exception {
      this(type, keyless, false, 32, 0);
    }

    Plan(String type, boolean keyless, boolean processingRows, int offset, long ttl)
        throws Exception {
      this.processingRows = processingRows;
      this.offset = offset;
      this.ttl = ttl;
      var config = new Configuration();
      config.set(
          org.apache.flink.table.api.config.ExecutionConfigOptions.IDLE_STATE_RETENTION,
          java.time.Duration.ofMillis(ttl));
      config.setString("streamfusion.native.enabled", "false");
      var table = NativePlannerTestEnvironment.create(config);
      table.executeSql(
          "CREATE TABLE range_input(k BIGINT,v "
              + type
              + ",rt TIMESTAMP(3)"
              + (processingRows ? ",pt AS PROCTIME()" : ",WATERMARK FOR rt AS rt")
              + ") WITH ('connector'='datagen')");
      table.executeSql(
          "CREATE TABLE range_output(k BIGINT,v "
              + type
              + ",rt TIMESTAMP(3),s "
              + type
              + ",c BIGINT,a "
              + type
              + ") WITH ('connector'='blackhole')");
      json =
          table
              .compilePlanSql(
                  "INSERT INTO range_output SELECT k,v,rt,SUM(v) OVER w,COUNT(v) OVER w,AVG(v) OVER"
                      + " w FROM range_input WINDOW w AS ("
                      + (keyless ? "" : "PARTITION BY k ")
                      + (processingRows
                          ? "ORDER BY pt ROWS BETWEEN " + offset + " PRECEDING AND CURRENT ROW)"
                          : "ORDER BY rt RANGE BETWEEN INTERVAL '0.032' SECOND PRECEDING AND"
                                + " CURRENT ROW)"))
              .asJsonString();
      planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
      var node = node();
      input = (RowType) node.getInputEdges().get(0).getOutputType();
      output = (RowType) node.getOutputType();
      assertEquals(List.of("k", "v", "rt"), input.getFieldNames().subList(0, 3));
      keys = keyless ? new int[0] : new int[] {0};
      keyType =
          RowType.of(Arrays.stream(keys).mapToObj(input::getTypeAt).toArray(LogicalType[]::new));
      selector =
          KeySelectorUtil.getRowDataSelector(
              getClass().getClassLoader(), keys, InternalTypeInfo.of(input));
      var transformation = (OneInputTransformation<?, ?>) node.translateToPlan(planner);
      var operator =
          (KeyedProcessOperator<?, ?, ?>)
              ((SimpleOperatorFactory<?>) transformation.getOperatorFactory()).getOperator();
      var functionType =
          processingRows
              ? org.apache.flink.table.runtime.operators.over.ProcTimeRowsBoundedPrecedingFunction
                  .class
              : RowTimeRangeBoundedPrecedingFunction.class;
      assertInstanceOf(functionType, operator.getUserFunction());
      var field = functionType.getDeclaredField("accTypes");
      field.setAccessible(true);
      accumulator = RowType.of((LogicalType[]) field.get(operator.getUserFunction()));
      var reader = CompiledPlanSerdeUtil.createJsonObjectReader(planner.createSerdeContext());
      org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode definition = null;
      for (var candidate : reader.readTree(json).path("nodes"))
        if (candidate.path("id").asInt() == node.getId()) definition = candidate;
      var spec =
          reader
              .forType(org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec.class)
              .<org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec>readValue(
                  definition.path("overSpec"));
      var calls = spec.getGroups().get(0).getAggCalls();
      timeColumn = spec.getGroups().get(0).getSort().getFieldIndices()[0];
      constants =
          tech.streamfusion.planner.OverConstantArguments.create(
                  spec.getConstants(),
                  false,
                  calls.stream()
                      .flatMap(call -> call.getArgList().stream())
                      .filter(index -> index >= input.getFieldCount())
                      .map(index -> index - input.getFieldCount())
                      .collect(java.util.stream.Collectors.toSet()))
              .orElseThrow();
      var arguments = constants.appendTo(input);
      values = calls.stream().mapToInt(call -> call.getArgList().get(0)).toArray();
      codes =
          Arrays.stream(values)
              .map(
                  column -> {
                    var logical = arguments.getTypeAt(column);
                    if (logical instanceof DecimalType) {
                      var d = (DecimalType) logical;
                      return NativeWindowOperatorCore.decimalCode(d.getPrecision(), d.getScale());
                    }
                    if (logical.is(LogicalTypeRoot.FLOAT)) return 6;
                    return logical.is(LogicalTypeRoot.DOUBLE) ? 1 : 0;
                  })
              .toArray();
      kinds =
          calls.stream()
              .mapToInt(
                  call -> {
                    switch (call.getAggregation().getKind()) {
                      case SUM:
                        return 0;
                      case SUM0:
                        return 22;
                      case COUNT:
                        return 3;
                      case AVG:
                        return 4;
                      default:
                        throw new AssertionError(call);
                    }
                  })
              .toArray();
      boolean[] retract = new boolean[calls.size()];
      Arrays.fill(retract, true);
      var information =
          AggregateUtil.transformToStreamAggregateInfoList(
              planner.getTypeFactory(),
              arguments,
              JavaScalaConversionUtil.toScala(calls),
              retract,
              true,
              true,
              true);
      widths =
          Arrays.stream(information.aggInfos())
              .limit(calls.size())
              .mapToInt(info -> info.externalAccTypes().length)
              .toArray();
      hiddenCount = information.countStarInserted() ? information.getIndexOfCountStar() : -1;
      assertArrayEquals(
          Arrays.stream(information.getAccTypes())
              .map(DataType::getLogicalType)
              .toArray(LogicalType[]::new),
          accumulator.getChildren().toArray(LogicalType[]::new));
    }

    ExecNode<?> node() throws Exception {
      var graph =
          CompiledPlanSerdeUtil.createJsonObjectReader(planner.createSerdeContext())
              .forType(ExecNodeGraph.class)
              .<ExecNodeGraph>readValue(json);
      return find(graph.getRootNodes().get(0));
    }

    static ExecNode<?> find(ExecNode<?> node) {
      if (node instanceof StreamExecOverAggregate) return node;
      for (var edge : node.getInputEdges()) {
        var found = find(edge.getSource());
        if (found != null) return found;
      }
      return null;
    }

    StockStateContract contract() {
      if (processingRows)
        return new StockOverProcessingRowsStateContract(
            selector.getProducedType().toSerializer(),
            keyType,
            input,
            accumulator,
            keys,
            timeColumn,
            kinds,
            widths,
            hiddenCount,
            constants.type(),
            constants.encodedValues(),
            offset + 1L,
            ttl > 1);
      return new StockOverRangeStateContract(
          selector.getProducedType().toSerializer(),
          keyType,
          input,
          accumulator,
          keys,
          timeColumn,
          kinds,
          widths,
          hiddenCount,
          constants.type(),
          constants.encodedValues());
    }

    @SuppressWarnings("unchecked")
    KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(boolean rocks)
        throws Exception {
      var transformation = (OneInputTransformation<?, ?>) node().translateToPlan(planner);
      var operator =
          (KeyedProcessOperator<RowData, RowData, RowData>)
              ((SimpleOperatorFactory<?>) transformation.getOperatorFactory()).getOperator();
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

    private static final class StateView extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {
      @Override
      public void processElement(StreamRecord<RowData> row) {}
    }

    KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness(
        boolean rocks) throws Exception {
      var operator =
          new NativeOverAggregateOperator(
              timeColumn,
              values,
              keys,
              codes,
              kinds,
              processingRows ? 1 : 2,
              offset,
              processingRows,
              keys.length == 0
                  ? new int[] {Native.STOCK_KEY_PROJECTION}
                  : new int[] {Native.STOCK_KEY_PROJECTION, -1},
              ttl,
              input,
              128);
      if (constants.type().getFieldCount() > 0)
        operator.withConstantArguments(constants.type(), constants.encodedValues());
      var contract = contract();
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

    RowData row(Long key, long value, long time) {
      return rowValue(key, value, time);
    }

    RowData nullRow(Long key, long time) {
      return rowValue(key, null, time);
    }

    private RowData rowValue(Long key, Long value, long time) {
      var row = new GenericRowData(input.getFieldCount());
      row.setField(0, key);
      for (int i = 1; i < input.getFieldCount(); i++) {
        var logical = input.getTypeAt(i);
        if (logical instanceof TimestampType || logical instanceof LocalZonedTimestampType) {
          row.setField(i, TimestampData.fromEpochMillis(time));
          continue;
        }
        assertTrue(logical.is(LogicalTypeFamily.NUMERIC), input.asSummaryString());
        if (value == null) continue;
        Object datum = value;
        if (logical.is(LogicalTypeRoot.FLOAT)) datum = value.floatValue();
        else if (logical.is(LogicalTypeRoot.DOUBLE)) datum = value.doubleValue();
        else if (logical instanceof DecimalType) {
          var d = (DecimalType) logical;
          datum =
              DecimalData.fromBigDecimal(
                  java.math.BigDecimal.valueOf(value), d.getPrecision(), d.getScale());
        }
        row.setField(i, datum);
      }
      return row;
    }

    void prefix(
        KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h, boolean pendingOnly)
        throws Exception {
      if (!pendingOnly) {
        pushStock(h, row(1L, 1, 1000));
        pushStock(h, nullRow(1L, 1000));
        h.processWatermark(new Watermark(1000));
        h.getOutput().clear();
        pushStock(h, nullRow(null, 1002));
        h.processWatermark(new Watermark(1002));
        h.getOutput().clear();
      }
      pushStock(h, row(1L, 7, 1064));
      pushStock(h, nullRow(1L, 1064));
      pushStock(h, row(null, 3, 1070));
      if (!pendingOnly) h.processWatermark(new Watermark(1050));
      h.getOutput().clear();
    }

    void prefixNative(
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h,
        boolean pendingOnly)
        throws Exception {
      if (!pendingOnly) {
        push(h, row(1L, 1, 1000));
        push(h, nullRow(1L, 1000));
        h.processWatermark(new Watermark(1000));
        nativeOutput(h);
        push(h, nullRow(null, 1002));
        h.processWatermark(new Watermark(1002));
        nativeOutput(h);
      }
      push(h, row(1L, 7, 1064));
      push(h, nullRow(1L, 1064));
      push(h, row(null, 3, 1070));
      if (!pendingOnly) h.processWatermark(new Watermark(1050));
      nativeOutput(h);
    }

    void suffix(KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h)
        throws Exception {
      pushStock(h, row(1L, 99, 1020));
      pushStock(h, row(1L, 2, 1064));
      h.processWatermark(new Watermark(1070));
      pushStock(h, row(1L, 4, 1072));
      h.processWatermark(new Watermark(1072));
    }

    void suffixNative(KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h)
        throws Exception {
      push(h, row(1L, 99, 1020));
      push(h, row(1L, 2, 1064));
      h.processWatermark(new Watermark(1070));
      push(h, row(1L, 4, 1072));
      h.processWatermark(new Watermark(1072));
    }

    void wrappedSuffix(KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h)
        throws Exception {
      pushStock(h, row(1L, 3, 1000));
      h.processWatermark(new Watermark(1000));
      h.processWatermark(new Watermark(Long.MAX_VALUE));
      pushStock(h, row(1L, 99, 1001));
      h.processWatermark(new Watermark(Long.MAX_VALUE));
    }

    void wrappedSuffixNative(
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h)
        throws Exception {
      push(h, row(1L, 3, 1000));
      h.processWatermark(new Watermark(1000));
      h.processWatermark(new Watermark(Long.MAX_VALUE));
      push(h, row(1L, 99, 1001));
      h.processWatermark(new Watermark(Long.MAX_VALUE));
    }

    void push(
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h, RowData row)
        throws Exception {
      h.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(List.of(row), input, NativeAllocator.SHARED, true))));
    }

    void pushStock(KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h, RowData row)
        throws Exception {
      h.processElement(new StreamRecord<>(new RowDataSerializer(input).toBinaryRow(row).copy()));
    }

    List<String> format(List<RowData> values) {
      var result = new ArrayList<String>();
      for (var row : values) {
        var fields = new ArrayList<String>();
        for (int i = 0; i < output.getFieldCount(); i++) fields.add(String.valueOf(get(row, i)));
        result.add(String.join(":", fields));
      }
      result.sort(String::compareTo);
      return result;
    }

    Object get(RowData row, int i) {
      return RowData.createFieldGetter(output.getTypeAt(i), i).getFieldOrNull(row);
    }

    List<String> stockOutput(KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h) {
      var values = format(h.extractOutputValues());
      h.getOutput().clear();
      return values;
    }

    List<String> nativeOutput(
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h) {
      var result = new ArrayList<String>();
      for (var batch : h.extractOutputValues()) {
        try (var root = batch.root()) {
          var reader = ArrowConversion.createArrowReader(root, output);
          var serializer = new RowDataSerializer(output);
          for (int i = 0; i < root.getRowCount(); i++)
            result.addAll(format(List.of(serializer.copy(reader.read(i)))));
        }
      }
      h.getOutput().clear();
      result.sort(String::compareTo);
      return result;
    }
  }
}

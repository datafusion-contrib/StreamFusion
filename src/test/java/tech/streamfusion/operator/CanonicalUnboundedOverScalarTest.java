package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.over.ProcTimeUnboundedPrecedingFunction;
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
import tech.streamfusion.state.StockOverFoldStateContract;

class CanonicalUnboundedOverScalarTest {
  static Stream<Arguments> cases() {
    return Stream.of("BIGINT", "FLOAT", "DOUBLE")
        .flatMap(
            type ->
                Stream.of(false, true)
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
                                                                                type,
                                                                                rows,
                                                                                global,
                                                                                ttl,
                                                                                stock,
                                                                                nativeStore)))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void rawAccumulatorsAndAgesSurviveBothOrdersAndNoInputReturns(
      String type, boolean rows, boolean global, long ttl, boolean stockStore, boolean nativeStore)
      throws Exception {
    var plan = new Plan(type, rows, global, ttl);
    var prefix =
        List.of(
            plan.row(Long.MIN_VALUE, 10000000000000000L),
            plan.row(Long.MIN_VALUE, 1L),
            plan.row(Long.MIN_VALUE, -10000000000000000L),
            plan.row(null, 3L),
            plan.row(Long.MAX_VALUE, null),
            plan.row(null, null));
    var suffix =
        List.of(plan.row(Long.MIN_VALUE, 2L), plan.row(null, 5L), plan.row(Long.MAX_VALUE, 7L));
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    OperatorSubtaskState source;
    Map<String, String> original;
    List<String> expectedPrefix, expectedLive, expectedExpired;
    try (var stock = plan.stock(stockStore)) {
      stock.open();
      time(stock, 100);
      expectedPrefix = plan.stockInput(stock, prefix);
      original = raw(stock);
      source = snapshot(stock);
      time(stock, 500);
      expectedLive = plan.stockInput(stock, suffix);
      time(stock, 1500);
      expectedExpired = plan.stockInput(stock, suffix);
    }
    OperatorSubtaskState returned;
    try (var target = plan.nativeHarness(nativeStore)) {
      target.initializeState(source);
      target.open();
      time(target, 200);
      returned = snapshot(target);
      time(target, 500);
      assertEquals(expectedLive, plan.nativeInput(target, suffix));
      time(target, 1500);
      assertEquals(expectedExpired, plan.nativeInput(target, suffix));
    }
    try (var stock = plan.stock(stockStore)) {
      stock.initializeState(returned);
      stock.open();
      assertEquals(original, raw(stock), "untouched canonical headers and original TTL ages");
      time(stock, 500);
      assertEquals(expectedLive, plan.stockInput(stock, suffix));
      time(stock, 1500);
      assertEquals(expectedExpired, plan.stockInput(stock, suffix));
    }
    try (var nativeOp = plan.nativeHarness(nativeStore)) {
      nativeOp.open();
      time(nativeOp, 100);
      assertEquals(expectedPrefix, plan.nativeInput(nativeOp, prefix));
      source = snapshot(nativeOp);
    }
    try (var stock = plan.stock(stockStore)) {
      stock.initializeState(source);
      stock.open();
      assertEquals(original, raw(stock), "native-first complete stock state");
      time(stock, 500);
      assertEquals(expectedLive, plan.stockInput(stock, suffix));
      source = snapshot(stock);
      original = raw(stock);
    }
    try (var nativeOp = plan.nativeHarness(nativeStore)) {
      nativeOp.initializeState(source);
      nativeOp.open();
      returned = snapshot(nativeOp);
      time(nativeOp, 1500);
      assertEquals(expectedExpired, plan.nativeInput(nativeOp, suffix));
    }
    try (var stock = plan.stock(stockStore)) {
      stock.initializeState(returned);
      stock.open();
      assertEquals(original, raw(stock));
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  static void time(OneInputStreamOperatorTestHarness<?, ?> harness, long now) throws Exception {
    harness.setProcessingTime(now);
    harness.setStateTtlProcessingTime(now);
  }

  static OperatorSubtaskState snapshot(AbstractStreamOperatorTestHarness<?> harness)
      throws Exception {
    return harness
        .snapshotWithLocalState(1, 100, SavepointType.savepoint(SavepointFormatType.CANONICAL))
        .getJobManagerOwnedState();
  }

  static Map<String, String> raw(
      KeyedOneInputStreamOperatorTestHarness<?, RowData, RowData> harness) throws Exception {
    var backend = (CheckpointableKeyedStateBackend<?>) harness.getOperator().getKeyedStateBackend();
    var resources = backend.savepoint().getSnapshotResources();
    try (AutoCloseable release = resources::release) {
      var result = new TreeMap<String, String>();
      var meta = resources.getMetaInfoSnapshots();
      try (var iterator = resources.createKVStateIterator()) {
        while (iterator.isValid()) {
          result.put(
              meta.get(iterator.kvStateId()).getName()
                  + ":"
                  + Base64.getEncoder().encodeToString(iterator.key()),
              Base64.getEncoder().encodeToString(iterator.value()));
          iterator.next();
        }
      }
      return result;
    }
  }

  static final class Plan {
    final PlannerBase planner;
    final String json;
    final RowType input, output, accumulator;
    final RowDataKeySelector selector;
    final StockOverFoldStateContract contract;
    final int[] kinds, columns, codes, partitions;
    final int timeColumn;
    final boolean rows;
    final long ttl;

    Plan(String type, boolean rows, boolean global, long ttl) throws Exception {
      this.rows = rows;
      this.ttl = ttl;
      var config = new Configuration();
      config.setString("streamfusion.native.enabled", "false");
      config.setString("table.exec.state.ttl", ttl + " ms");
      var table = NativePlannerTestEnvironment.create(config);
      table.executeSql(
          "CREATE TABLE scalar_input(k BIGINT,v "
              + type
              + ",pt AS PROCTIME()) WITH ('connector'='datagen')");
      table.executeSql(
          "CREATE TABLE scalar_output(k BIGINT,v "
              + type
              + ",s "
              + type
              + ",c BIGINT,a "
              + type
              + ") WITH ('connector'='blackhole')");
      json =
          table
              .compilePlanSql(
                  "INSERT INTO scalar_output SELECT k,v,SUM(v) OVER w,COUNT(v) OVER w,AVG(v) OVER w"
                      + " FROM scalar_input WINDOW w AS ("
                      + (global ? "" : "PARTITION BY k ")
                      + "ORDER BY pt "
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
      assertEquals(ProcTimeUnboundedPrecedingFunction.class, function.getClass());
      Field field = function.getClass().getDeclaredField("accTypes");
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
      var infos =
          AggregateUtil.transformToStreamAggregateInfoList(
              planner.getTypeFactory(),
              input,
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
      columns = calls.stream().mapToInt(call -> call.getArgList().get(0)).toArray();
      codes =
          Arrays.stream(columns)
              .map(
                  column -> {
                    var logical = input.getTypeAt(column);
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
          new StockOverFoldStateContract(
              selector.getProducedType().toSerializer(),
              selector.getProducedType().toRowType(),
              accumulator,
              kinds,
              widths,
              ttl > 0);
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
              true,
              partitions.length == 0
                  ? new int[] {Native.STOCK_KEY_PROJECTION}
                  : new int[] {Native.STOCK_KEY_PROJECTION, -1},
              ttl,
              input,
              128);
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

    RowData row(Long key, Long value) {
      var row = new GenericRowData(input.getFieldCount());
      row.setField(0, key);
      Object number = value;
      if (value != null && input.getTypeAt(1).is(LogicalTypeRoot.FLOAT))
        number = value.floatValue();
      else if (value != null && input.getTypeAt(1).is(LogicalTypeRoot.DOUBLE))
        number = value.doubleValue();
      row.setField(1, number);
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
      return harness.extractOutputValues().stream()
          .map(this::value)
          .collect(java.util.stream.Collectors.toList());
    }

    List<String> nativeInput(
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
        List<RowData> rows)
        throws Exception {
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(rows, input, NativeAllocator.SHARED, true))));
      var out = new ArrayList<String>();
      for (var batch : harness.extractOutputValues())
        try (var root = batch.root()) {
          var reader = ArrowConversion.createArrowReader(root, output);
          for (int i = 0; i < root.getRowCount(); i++) out.add(value(reader.read(i)));
        }
      harness.getOutput().clear();
      return out;
    }

    private String value(RowData row) {
      var values = new ArrayList<String>();
      for (int i = 0; i < output.getFieldCount(); i++) {
        if (i == timeColumn)
          continue; // The SQL projection drops the internal PROCTIME placeholder.
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

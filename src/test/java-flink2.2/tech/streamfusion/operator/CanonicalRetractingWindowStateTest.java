package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.delegation.DefaultExecutor;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowAggregate;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.example.canonical.CanonicalFixtureFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.CanonicalTestMatrix;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.compat.FlinkPlanCompat;
import tech.streamfusion.state.CanonicalStateBackend;
import tech.streamfusion.state.CanonicalStateBackendFactory;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;
import tech.streamfusion.state.StockSlicingWindowStateContract;
import tech.streamfusion.state.StockStateContract;

class CanonicalRetractingWindowStateTest {
  @BeforeAll
  static void memory() {
    CanonicalProcessingTimeWindowOperatorTest.memoryAuthority();
  }

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String shape : List.of("TUMBLE", "HOP", "CUMULATE"))
      for (String type : List.of("INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL(38,20)"))
        for (boolean star : new boolean[] {false, true})
          for (boolean filtered : new boolean[] {false, true})
            for (boolean keyless : new boolean[] {false, true})
              for (boolean rocks : new boolean[] {false, true})
                for (boolean first : new boolean[] {false, true})
                  for (boolean compiled : new boolean[] {false, true})
                    cases.add(
                        Arguments.of(shape, type, star, filtered, keyless, rocks, first, compiled));
    return CanonicalTestMatrix.select(cases.stream());
  }

  @ParameterizedTest
  @MethodSource("cases")
  void signedScalarsAndZeroCountOverflowRetainRawStockStateAndFutureTimers(
      String shape,
      String type,
      boolean star,
      boolean filtered,
      boolean keyless,
      boolean rocks,
      boolean first,
      boolean compiled)
      throws Exception {
    var plan = new Plan(shape, type, star, filtered, keyless, compiled);
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    OperatorSubtaskState saved = null, checkpoint = null, returned = null;
    try (var reference = plan.stock(!rocks)) {
      reference.open();
      var input = plan.rows(type);
      for (int stage = 0; stage < 3; stage++) {
        boolean nativeStage = (stage % 2 == 0) == first;
        try (var stock = plan.stock(rocks);
            var nativeOwner = plan.nativeHarness(rocks)) {
          var receiver = nativeStage ? nativeOwner : stock;
          if (saved != null) receiver.initializeState(saved);
          receiver.open();
          if (nativeStage)
            assertEquals(
                rocks,
                ((AbstractNativeStatefulOperator<?>) nativeOwner.getOperator())
                    .directRocksDBState());
          for (int index = stage * 4; index < stage * 4 + 4; index++) {
            plan.send(reference, input.get(index));
            if (nativeStage) plan.sendNative(nativeOwner, input.get(index));
            else plan.send(stock, input.get(index));
          }
          var previous = saved;
          saved = canonical(receiver);
          if (previous != null) previous.discardState();
          try (var view = plan.stock(!rocks)) {
            view.initializeState(saved);
            view.open();
            assertEquals(
                CanonicalUnboundedOverScalarTest.raw(reference),
                CanonicalUnboundedOverScalarTest.raw(view),
                "Signed counts, raw SUM and sticky AVG overflow must remain byte-identical");
          }
        }
      }
      try (var source = plan.nativeHarness(rocks)) {
        source.initializeState(saved);
        source.open();
        source.prepareSnapshotPreBarrier(99);
        checkpoint = source.snapshot(99, 0);
      }
      try (var resumed = plan.nativeHarness(rocks)) {
        resumed.initializeState(checkpoint);
        resumed.open();
        returned = canonical(resumed);
        try (var stock = plan.stock(!rocks)) {
          stock.initializeState(returned);
          stock.open();
          assertEquals(
              CanonicalUnboundedOverScalarTest.raw(reference),
              CanonicalUnboundedOverScalarTest.raw(stock));
          reference.processWatermark(new Watermark(9999));
          resumed.processWatermark(new Watermark(9999));
          stock.processWatermark(new Watermark(9999));
          var expected = plan.output(reference);
          assertFalse(expected.isEmpty());
          assertEquals(expected, plan.output(stock));
          assertEquals(expected, plan.nativeOutput(resumed));
        }
      }
    } finally {
      if (returned != null) returned.discardState();
      if (checkpoint != null) checkpoint.discardState();
      if (saved != null) saved.discardState();
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private static OperatorSubtaskState canonical(AbstractStreamOperatorTestHarness<?> owner)
      throws Exception {
    owner.prepareSnapshotPreBarrier(1);
    return owner
        .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
        .getJobManagerOwnedState();
  }

  private static final class Plan {
    final TableEnvironment table;
    final PlannerBase planner;
    final String json, query;
    final boolean compiled;
    final RowType input, output;
    final int maximum;
    final String uid;

    Plan(
        String shape,
        String type,
        boolean star,
        boolean filtered,
        boolean keyless,
        boolean compiled)
        throws Exception {
      this.compiled = compiled;
      var config = new Configuration();
      config.setString("streamfusion.native.enabled", "false");
      config.setString("parallelism.default", "1");
      config.setString("pipeline.max-parallelism", "128");
      config.setString("table.exec.uid.generation", "ALWAYS");
      config.setString("table.optimizer.agg-phase-strategy", "ONE_PHASE");
      table = NativePlannerTestEnvironment.create(config, CanonicalFixtureFactory.class);
      table.executeSql(
          "CREATE TABLE input_table(k BIGINT,v "
              + type
              + ",f BOOLEAN,rt TIMESTAMP(3),WATERMARK FOR rt AS rt) WITH"
              + " ('connector'='canonical-fixture','input'='unused.csv','insert-only'='false')");
      table.executeSql(
          "CREATE TABLE output_table("
              + (keyless ? "" : "k BIGINT,")
              + "n BIGINT,s "
              + type
              + ",a "
              + type
              + ",ws TIMESTAMP(3),we TIMESTAMP(3))"
              + " WITH ('connector'='blackhole')");
      String filter = filtered ? " FILTER(WHERE f)" : "";
      query =
          "INSERT INTO output_table SELECT "
              + (keyless ? "" : "k,")
              + "COUNT("
              + (star ? "*" : "v")
              + ")"
              + (star ? "" : filter)
              + ",SUM(v)"
              + filter
              + ",AVG(v)"
              + filter
              + ",window_start,window_end FROM TABLE("
              + shape
              + "(TABLE input_table,DESCRIPTOR(rt),INTERVAL '"
              + (shape.equals("TUMBLE") ? "3" : "1")
              + "' SECOND"
              + (shape.equals("TUMBLE") ? "" : ",INTERVAL '3' SECOND")
              + ")) GROUP BY "
              + (keyless ? "" : "k,")
              + "window_start,window_end";
      json = table.compilePlanSql(query).asJsonString();
      planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
      boolean retracting = false;
      for (var definition :
          FlinkPlanCompat.createJsonObjectReader(planner.createSerdeContext())
              .readTree(json)
              .path("nodes"))
        retracting |=
            definition.path("type").asText().startsWith("stream-exec-window-aggregate")
                && definition.path("needRetraction").asBoolean();
      assertTrue(retracting, json);
      var node = node();
      input = (RowType) node.getInputEdges().get(0).getOutputType();
      output = (RowType) node.getOutputType();
      var graph =
          (org.apache.flink.streaming.api.graph.StreamGraph)
              new DefaultExecutor(planner.getExecEnv())
                  .createPipeline(
                      planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(json))),
                      table.getConfig().getConfiguration(),
                      null);
      var original =
          graph.getStreamNodes().stream()
              .filter(
                  n ->
                      n.getOperatorFactory() instanceof SimpleOperatorFactory
                          && FlinkPlanCompat.windowOperator()
                              .isInstance(
                                  ((SimpleOperatorFactory<?>) n.getOperatorFactory())
                                      .getOperator()))
              .findFirst()
              .orElseThrow();
      maximum = FlinkPlanCompat.maximumParallelism(original);
      uid = original.getTransformationUID();
      assertTrue(maximum > 0);
    }

    ExecNode<?> node() throws Exception {
      var graph =
          FlinkPlanCompat.createJsonObjectReader(planner.createSerdeContext())
              .forType(ExecNodeGraph.class)
              .<ExecNodeGraph>readValue(json);
      return find(graph.getRootNodes().get(0));
    }

    static ExecNode<?> find(ExecNode<?> node) {
      if (node instanceof StreamExecWindowAggregate) return node;
      for (var edge : node.getInputEdges()) {
        var found = find(edge.getSource());
        if (found != null) return found;
      }
      return null;
    }

    @SuppressWarnings("unchecked")
    KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(boolean rocks)
        throws Exception {
      table.getConfig().set("streamfusion.native.enabled", "false");
      var transformation =
          (OneInputTransformation<RowData, RowData>) node().translateToPlan(planner);
      var selector = (RowDataKeySelector) transformation.getStateKeySelector();
      var operator =
          (OneInputStreamOperator<RowData, RowData>)
              ((SimpleOperatorFactory<?>) transformation.getOperatorFactory()).getOperator();
      var owner =
          new KeyedOneInputStreamOperatorTestHarness<>(
              operator, selector, selector.getProducedType(), maximum, 1, 0);
      if (rocks) owner.setStateBackend(tech.streamfusion.compat.CanonicalTestCompat.rocksBackend());
      owner.setup(new RowDataSerializer(output));
      return owner;
    }

    @SuppressWarnings("unchecked")
    KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness(
        boolean rocks) throws Exception {
      table.getConfig().set("streamfusion.native.enabled", "true");
      var transformations =
          compiled
              ? planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(json)))
              : planner.translate(
                  planner.getParser().parse(query).stream()
                      .map(op -> (org.apache.flink.table.operations.ModifyOperation) op)
                      .collect(java.util.stream.Collectors.toList()));
      var graph =
          (org.apache.flink.streaming.api.graph.StreamGraph)
              new DefaultExecutor(planner.getExecEnv())
                  .createPipeline(transformations, table.getConfig().getConfiguration(), null);
      var owners =
          graph.getStreamNodes().stream()
              .filter(
                  n ->
                      n.getOperatorFactory() instanceof SimpleOperatorFactory
                          && ((SimpleOperatorFactory<?>) n.getOperatorFactory()).getOperator()
                              instanceof NativeColumnarWindowAggregateOperator)
              .collect(java.util.stream.Collectors.toList());
      assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
      var nativeNode = owners.get(0);
      assertEquals(maximum, FlinkPlanCompat.maximumParallelism(nativeNode));
      if (compiled) assertEquals(uid, nativeNode.getTransformationUID());
      var operator =
          (NativeColumnarWindowAggregateOperator)
              ((SimpleOperatorFactory<?>) nativeNode.getOperatorFactory()).getOperator();
      var field = AbstractNativeStatefulOperator.class.getDeclaredField("stockStateContract");
      field.setAccessible(true);
      var contract = (StockStateContract) field.get(operator);
      assertInstanceOf(StockSlicingWindowStateContract.class, contract);
      var owner =
          new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
              operator, ignored -> 0, Types.INT, maximum, 1, 0);
      var config = new Configuration();
      if (rocks)
        config.setString("state.backend.type", RocksDBNativeStateBackendFactory.class.getName());
      CanonicalStateBackend.install(
          StreamExecutionEnvironment.getExecutionEnvironment(),
          config,
          owner.getStreamConfig().getOperatorID().toHexString(),
          operator.getClass().getSimpleName(),
          contract,
          getClass().getClassLoader());
      owner.setStateBackend(
          new CanonicalStateBackendFactory().createFromConfig(config, getClass().getClassLoader()));
      owner.setup(new ArrowBatchSerializer());
      table.getConfig().set("streamfusion.native.enabled", "false");
      return owner;
    }

    List<RowData> rows(String type) {
      long maximum =
          type.startsWith("DECIMAL")
              ? 999_999_999_999_999_999L
              : type.equals("INT")
                  ? Integer.MAX_VALUE
                  : type.equals("FLOAT")
                      ? 16_777_216L
                      : type.equals("DOUBLE") ? 9_007_199_254_740_992L : Long.MAX_VALUE;
      return List.of(
          row(RowKind.INSERT, 1L, maximum, 100, true), row(RowKind.INSERT, 1L, maximum, 100, true),
          row(RowKind.INSERT, null, null, 100, true), row(RowKind.INSERT, 2L, 5L, 100, true),
          row(RowKind.DELETE, 1L, maximum, 100, true), row(RowKind.DELETE, 1L, maximum, 100, true),
          row(RowKind.INSERT, 1L, null, 100, true), row(RowKind.INSERT, 2L, null, 100, true),
          row(RowKind.INSERT, 1L, 1L, 100, true), row(RowKind.DELETE, 2L, 5L, 100, true),
          row(RowKind.INSERT, 3L, 7L, 1200, false), row(RowKind.INSERT, null, 3L, 3500, true));
    }

    RowData row(RowKind kind, Long key, Long value, long time, boolean filter) {
      var result = new GenericRowData(kind, input.getFieldCount());
      for (int index = 0; index < input.getFieldCount(); index++) {
        switch (input.getFieldNames().get(index)) {
          case "k":
            result.setField(index, key);
            break;
          case "v":
            result.setField(index, CanonicalOverValueFixture.number(input.getTypeAt(index), value));
            break;
          case "rt":
            result.setField(index, TimestampData.fromEpochMillis(time));
            break;
          case "f":
            result.setField(index, filter);
            break;
          default:
            throw new AssertionError(input.asSummaryString());
        }
      }
      return result;
    }

    void send(KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> owner, RowData row)
        throws Exception {
      owner.processElement(
          new StreamRecord<>(new RowDataSerializer(input).toBinaryRow(row).copy()));
    }

    void sendNative(
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> owner, RowData row)
        throws Exception {
      owner.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      CanonicalOverValueFixture.physicalRows(List.of(row), input),
                      input,
                      NativeAllocator.SHARED,
                      true))));
    }

    List<String> output(KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> owner) {
      var result =
          owner.extractOutputValues().stream()
              .map(row -> CanonicalOverValueFixture.serialized(row, output))
              .sorted()
              .collect(java.util.stream.Collectors.toList());
      owner.getOutput().clear();
      return result;
    }

    List<String> nativeOutput(
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> owner) {
      var result = new ArrayList<String>();
      for (var batch : owner.extractOutputValues())
        try (var root = batch.root()) {
          var reader = ArrowConversion.createArrowReader(root, output);
          for (int index = 0; index < root.getRowCount(); index++)
            result.add(CanonicalOverValueFixture.serialized(reader.read(index), output));
        }
      owner.getOutput().clear();
      result.sort(String::compareTo);
      return result;
    }
  }
}

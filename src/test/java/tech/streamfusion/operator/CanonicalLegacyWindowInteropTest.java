package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.planner.delegation.DefaultExecutor;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupWindowAggregate;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.CanonicalTestMatrix;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.compat.FlinkPlanCompat;
import tech.streamfusion.state.CanonicalStateBackend;
import tech.streamfusion.state.CanonicalStateBackendFactory;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;
import tech.streamfusion.state.StockStateContract;

class CanonicalLegacyWindowInteropTest {
  private static final SavepointType CANONICAL =
      SavepointType.savepoint(SavepointFormatType.CANONICAL);
  private static final long BASE = CanonicalSharedWindowInteropTest.BASE;

  @BeforeAll
  static void memory() {
    CanonicalProcessingTimeWindowOperatorTest.memoryAuthority();
  }

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String shape : List.of("SESSION", "TUMBLE", "HOP", "PROC_TUMBLE", "PROC_HOP"))
      for (boolean ltz : new boolean[] {false, true})
        for (String zone : List.of("UTC", "Europe/Berlin"))
          for (boolean keyless : new boolean[] {false, true})
            for (boolean distinct : new boolean[] {false, true})
              for (boolean rocks : new boolean[] {false, true})
                for (boolean compiled : new boolean[] {false, true})
                  cases.add(Arguments.of(shape, ltz, zone, keyless, distinct, rocks, compiled));
    return CanonicalTestMatrix.select(
        cases.stream()
            .filter(
                args -> !((String) args.get()[0]).startsWith("PROC_") || (boolean) args.get()[1]));
  }

  @ParameterizedTest(name = "{0},ltz={1},zone={2},keyless={3},distinct={4},rocks={5},compiled={6}")
  @MethodSource("cases")
  void releasedLegacyWindowsSwitchEnginesAndFinishIndependentAsyncViews(
      String shape,
      boolean ltz,
      String zone,
      boolean keyless,
      boolean distinct,
      boolean rocks,
      boolean compiled)
      throws Exception {
    var plan = new Plan(shape, ltz, zone, keyless, distinct, compiled);
    var captured = new OperatorSnapshotFutures[2];
    var expected = new OperatorSubtaskState[2];
    OperatorSubtaskState recovery = null;
    try (var reference = plan.stock(false)) {
      reference.open();
      if (plan.processing) reference.setProcessingTime(BASE + 1000);
      for (var row :
          List.of(
              plan.row(1L, 10L, 0),
              plan.row(1L, 10L, 2000),
              plan.row(1L, 20L, 10000),
              plan.row(null, null, 12000))) plan.stockInput(reference, row);
      if (!plan.processing) reference.processWatermark(new Watermark(BASE + 500));
      expected[0] = snapshot(reference, 1);
      try (var source = plan.nativeHarness(rocks)) {
        source.initializeState(expected[0]);
        source.open();
        if (plan.processing) source.setProcessingTime(BASE + 1000);
        else source.processWatermark(new Watermark(BASE + 500));
        var owner = (AbstractNativeStatefulOperator<?>) source.getOperator();
        assertEquals(rocks, owner.directRocksDBState());
        try {
          captured[0] = CanonicalDistinctWindowAsyncTest.capture(source, 2);
          if (plan.processing) {
            reference.setProcessingTime(BASE + 1500);
            source.setProcessingTime(BASE + 1500);
          }
          var bridge = plan.row(1L, 10L, 6000);
          plan.stockInput(reference, bridge);
          plan.nativeInput(source, bridge);
          expected[1] = snapshot(reference, 3);
          captured[1] = CanonicalDistinctWindowAsyncTest.capture(source, 3);
          recovery = source.snapshot(4, 0);
          if (plan.processing) source.setProcessingTime(BASE + 1700);
          plan.nativeInput(source, plan.row(1L, 77L, 3000));
          source.prepareSnapshotPreBarrier(5);
          owner.closeHandle();
          owner.handle = 0;
          for (int stage = 1; stage >= 0; stage--) {
            var saved = CanonicalDistinctWindowAsyncTest.finishOnWriter(captured[stage]);
            try (var stock = plan.stock(true);
                var returned = plan.stock(false)) {
              stock.initializeState(expected[stage]);
              returned.initializeState(saved);
              stock.open();
              returned.open();
              plan.suffix(stock);
              plan.suffix(returned);
              var rows = plan.output(stock);
              assertFalse(rows.isEmpty());
              assertEquals(rows, plan.output(returned));
            }
          }
        } finally {
          for (var future : captured) if (future != null) future.cancel();
        }
      }
      try (var stock = plan.stock(false);
          var resumed = plan.nativeHarness(rocks)) {
        stock.initializeState(expected[1]);
        resumed.initializeState(recovery);
        stock.open();
        resumed.open();
        var suffix = plan.suffixRows();
        for (int i = 0; i < suffix.size(); i++) {
          if (plan.processing) {
            stock.setProcessingTime(plan.suffixClock(i));
            resumed.setProcessingTime(plan.suffixClock(i));
          }
          plan.stockInput(stock, suffix.get(i));
          plan.nativeInput(resumed, suffix.get(i));
        }
        if (plan.processing) {
          stock.setProcessingTime(BASE + 30000);
          resumed.setProcessingTime(BASE + 30000);
        } else {
          stock.processWatermark(Watermark.MAX_WATERMARK);
          resumed.processWatermark(Watermark.MAX_WATERMARK);
        }
        assertEquals(plan.output(stock), plan.output(resumed));
      }
    } finally {
      for (var state : expected) if (state != null) state.discardState();
      if (recovery != null) recovery.discardState();
    }
  }

  static OperatorSubtaskState snapshot(AbstractStreamOperatorTestHarness<?> harness, long id)
      throws Exception {
    harness.prepareSnapshotPreBarrier(id);
    return harness.snapshotWithLocalState(id, 0, CANONICAL).getJobManagerOwnedState();
  }

  static final class Plan {
    final org.apache.flink.table.api.TableEnvironment table;
    final PlannerBase planner;
    final String json, query;
    final RowType input, output;
    final boolean compiled, processing;
    final int maximum;

    Plan(
        String shape, boolean ltz, String zone, boolean keyless, boolean distinct, boolean compiled)
        throws Exception {
      this.compiled = compiled;
      processing = shape.startsWith("PROC_");
      String family = processing ? shape.substring(5) : shape;
      String arguments =
          family.equals("HOP") ? "INTERVAL '2' SECOND,INTERVAL '6' SECOND" : "INTERVAL '6' SECOND";
      maximum = keyless ? 1 : 128;
      var config = new Configuration();
      config.setString("streamfusion.native.enabled", "true");
      config.setString("parallelism.default", "1");
      config.setString("pipeline.max-parallelism", "128");
      config.setString("table.local-time-zone", zone);
      table = NativePlannerTestEnvironment.create(config);
      table.getConfig().set("streamfusion.native.enabled", "false");
      table.executeSql(
          "CREATE TABLE legacy_input(k BIGINT,v BIGINT,"
              + (processing
                  ? "rt AS PROCTIME()"
                  : "rt " + (ltz ? "TIMESTAMP_LTZ(3)" : "TIMESTAMP(3)") + ",WATERMARK FOR rt AS rt")
              + ") WITH ('connector'='datagen')");
      table.executeSql(
          "CREATE TABLE legacy_output("
              + (keyless ? "" : "k BIGINT,")
              + "n BIGINT,s BIGINT,a BIGINT,ws TIMESTAMP(3),we TIMESTAMP(3)) WITH"
              + " ('connector'='blackhole')");
      query =
          "INSERT INTO legacy_output SELECT "
              + (keyless ? "" : "k,")
              + "COUNT("
              + (distinct ? "DISTINCT v" : "v")
              + "),SUM(v),AVG(v),"
              + family
              + "_START(rt,"
              + arguments
              + "),"
              + family
              + "_END(rt,"
              + arguments
              + ")"
              + " FROM legacy_input GROUP BY "
              + (keyless ? "" : "k,")
              + family
              + "(rt,"
              + arguments
              + ")";
      json = table.compilePlanSql(query).asJsonString();
      planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
      var node = node();
      input = (RowType) node.getInputEdges().get(0).getOutputType();
      output = (RowType) node.getOutputType();
    }

    ExecNode<?> node() throws Exception {
      var graph =
          FlinkPlanCompat.createJsonObjectReader(planner.createSerdeContext())
              .forType(ExecNodeGraph.class)
              .<ExecNodeGraph>readValue(json);
      return find(graph.getRootNodes().get(0));
    }

    static ExecNode<?> find(ExecNode<?> node) {
      if (node instanceof StreamExecGroupWindowAggregate) return node;
      for (var edge : node.getInputEdges()) {
        var found = find(edge.getSource());
        if (found != null) return found;
      }
      return null;
    }

    @SuppressWarnings("unchecked")
    KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(boolean rocks)
        throws Exception {
      var t = (OneInputTransformation<RowData, RowData>) node().translateToPlan(planner);
      var selector = (RowDataKeySelector) t.getStateKeySelector();
      var op =
          (OneInputStreamOperator<RowData, RowData>)
              ((SimpleOperatorFactory<?>) t.getOperatorFactory()).getOperator();
      var h =
          new KeyedOneInputStreamOperatorTestHarness<>(
              op, selector, selector.getProducedType(), maximum, 1, 0);
      if (rocks) h.setStateBackend(tech.streamfusion.compat.CanonicalTestCompat.rocksBackend());
      h.setup(new RowDataSerializer(output));
      return h;
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
                              instanceof NativeRowWindowOperatorCore)
              .collect(java.util.stream.Collectors.toList());
      assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
      var owner = owners.get(0);
      var operator =
          (NativeRowWindowOperatorCore)
              ((SimpleOperatorFactory<?>) owner.getOperatorFactory()).getOperator();
      var field = AbstractNativeStatefulOperator.class.getDeclaredField("stockStateContract");
      field.setAccessible(true);
      var contract = (StockStateContract) field.get(operator);
      if (query.contains("SESSION(")) {
        assertInstanceOf(tech.streamfusion.state.StockSessionWindowStateContract.class, contract);
      } else {
        assertInstanceOf(tech.streamfusion.state.StockSlicingWindowStateContract.class, contract);
      }
      var h =
          new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
              (OneInputStreamOperator<ArrowBatch, ArrowBatch>) operator,
              ignored -> 0,
              Types.INT,
              maximum,
              1,
              0);
      var config = new Configuration();
      if (rocks)
        config.setString("state.backend.type", RocksDBNativeStateBackendFactory.class.getName());
      CanonicalStateBackend.install(
          StreamExecutionEnvironment.getExecutionEnvironment(),
          config,
          h.getStreamConfig().getOperatorID().toHexString(),
          operator.getClass().getSimpleName(),
          contract,
          getClass().getClassLoader());
      h.setStateBackend(
          new CanonicalStateBackendFactory().createFromConfig(config, getClass().getClassLoader()));
      h.setup(new ArrowBatchSerializer());
      table.getConfig().set("streamfusion.native.enabled", "false");
      assertEquals(json, table.loadPlan(PlanReference.fromJsonString(json)).asJsonString());
      return h;
    }

    RowData row(Long key, Long value, long time) {
      var row = new GenericRowData(input.getFieldCount());
      for (int i = 0; i < input.getFieldCount(); i++) {
        switch (input.getFieldNames().get(i)) {
          case "k":
            row.setField(i, key);
            break;
          case "v":
            row.setField(i, value);
            break;
          case "rt":
            row.setField(i, TimestampData.fromEpochMillis(BASE + time));
            break;
          default:
            throw new AssertionError(input.asSummaryString());
        }
      }
      return row;
    }

    void stockInput(
        KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h, RowData row)
        throws Exception {
      h.processElement(new StreamRecord<>(new RowDataSerializer(input).toBinaryRow(row).copy()));
    }

    void nativeInput(
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h, RowData row)
        throws Exception {
      h.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(List.of(row), input, NativeAllocator.SHARED, true))));
    }

    List<RowData> suffixRows() {
      if (processing)
        return List.of(row(1L, 30L, 11000), row(null, 7L, 15000), row(1L, 40L, 19000));
      return List.of(
          row(1L, 5L, -10000), row(1L, 30L, 11000), row(null, 7L, 15000), row(1L, 40L, 19000));
    }

    long suffixClock(int index) {
      return BASE + new long[] {2500, 7000, 9500}[index];
    }

    void suffix(KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h)
        throws Exception {
      var rows = suffixRows();
      for (int i = 0; i < rows.size(); i++) {
        if (processing) h.setProcessingTime(suffixClock(i));
        stockInput(h, rows.get(i));
      }
      if (processing) h.setProcessingTime(BASE + 30000);
      else h.processWatermark(Watermark.MAX_WATERMARK);
    }

    List<String> output(AbstractStreamOperatorTestHarness<?> h) {
      var result = new ArrayList<String>();
      while (!h.getOutput().isEmpty()) {
        var event = h.getOutput().poll();
        if (!(event instanceof StreamRecord<?>)) continue;
        var value = ((StreamRecord<?>) event).getValue();
        if (value instanceof ArrowBatch) {
          try (var root = ((ArrowBatch) value).root()) {
            for (var row : RowDataArrowConverter.read(root, output)) result.add(bytes(row));
          }
        } else result.add(bytes((RowData) value));
      }
      Collections.sort(result);
      return result;
    }

    String bytes(RowData row) {
      var binary = new RowDataSerializer(output).toBinaryRow(row);
      return Base64.getEncoder()
          .encodeToString(
              BinarySegmentUtils.copyToBytes(
                  binary.getSegments(), binary.getOffset(), binary.getSizeInBytes()));
    }
  }
}

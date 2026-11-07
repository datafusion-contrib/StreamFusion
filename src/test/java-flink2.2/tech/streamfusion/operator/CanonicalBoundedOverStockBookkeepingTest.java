package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.serde.CompiledPlanSerdeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecOverAggregate;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.over.ProcTimeRowsBoundedPrecedingFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.NativePlannerTestEnvironment;

/** Released stock state protocol controls, not native bounded canonical compatibility claims. */
class CanonicalBoundedOverStockBookkeepingTest {
  static Stream<Arguments> modes() {
    return Stream.of(0, 2)
        .flatMap(
            offset ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of(false, true)
                                .map(sameClock -> Arguments.of(offset, rocks, sameClock))));
  }

  @ParameterizedTest
  @MethodSource("modes")
  void stockMinimumClockAndCounterCannotBeReconstructedFromRetainedRowCount(
      int offset, boolean rocks, boolean sameClock) throws Exception {
    var plan = new Plan(offset);
    OperatorSubtaskState snapshot;
    String expected;
    try (var original = plan.harness(rocks)) {
      original.open();
      plan.push(original, 1, 100);
      plan.push(original, 2, sameClock ? 100 : 200);
      var backend = original.getOperator().<RowData>getKeyedStateBackend();
      var smallest =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>("smallestTSState", LongSerializer.INSTANCE));
      var counter =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>("processedCountState", LongSerializer.INSTANCE));
      assertEquals(offset == 0 ? Long.MAX_VALUE : 100L, smallest.value());
      assertEquals(offset == 0 ? 1L : 2L, counter.value());
      assertEquals(offset == 0 ? 1 : 2, plan.retainedRows(original));
      snapshot =
          original
              .snapshotWithLocalState(
                  1, 100, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      expected = plan.push(original, 3, sameClock ? 100 : 300);
      assertEquals(offset == 0 ? "5:2" : "6:3", expected);
      assertEquals(offset == 0 ? 2 : 3, plan.retainedRows(original));
      assertEquals(offset == 0 ? 1L : 3L, counter.value());
    }
    try (var restored = plan.harness(rocks)) {
      restored.initializeState(snapshot);
      restored.open();
      assertEquals(expected, plan.push(restored, 3, sameClock ? 100 : 300));
      assertEquals(offset == 0 ? 2 : 3, plan.retainedRows(restored));
    }
  }

  static final class Plan {
    final PlannerBase planner;
    final String json;
    final RowType input, output;
    final RowDataKeySelector selector;
    final int sumColumn, countColumn;

    Plan(int offset) throws Exception {
      var config = new Configuration();
      config.setString("streamfusion.native.enabled", "false");
      var table = NativePlannerTestEnvironment.create(config);
      table.executeSql(
          "CREATE TABLE bounded_over_input(k BIGINT,v BIGINT,pt AS PROCTIME()) WITH"
              + " ('connector'='datagen')");
      table.executeSql(
          "CREATE TABLE bounded_over_output(k BIGINT,s BIGINT,c BIGINT) WITH"
              + " ('connector'='blackhole')");
      json =
          table
              .compilePlanSql(
                  "INSERT INTO bounded_over_output SELECT k,SUM(v) OVER w,COUNT(v) OVER w FROM"
                      + " bounded_over_input WINDOW w AS (PARTITION BY k ORDER BY pt ROWS BETWEEN "
                      + offset
                      + " PRECEDING AND CURRENT ROW)")
              .asJsonString();
      planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
      var node = node();
      input = (RowType) node.getInputEdges().get(0).getOutputType();
      output = (RowType) node.getOutputType();
      assertEquals(3, input.getFieldCount());
      assertEquals(5, output.getFieldCount());
      var reader = CompiledPlanSerdeUtil.createJsonObjectReader(planner.createSerdeContext());
      org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode definition = null;
      for (var value : reader.readTree(json).path("nodes"))
        if (value.path("id").asInt() == node.getId()) definition = value;
      var spec =
          reader
              .forType(org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec.class)
              .<org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec>readValue(
                  definition.path("overSpec"));
      int sum = -1, count = -1;
      var calls = spec.getGroups().get(0).getAggCalls();
      for (int i = 0; i < calls.size(); i++) {
        var kind = calls.get(i).getAggregation().getKind();
        if (kind == org.apache.calcite.sql.SqlKind.SUM
            || kind == org.apache.calcite.sql.SqlKind.SUM0) sum = input.getFieldCount() + i;
        if (kind == org.apache.calcite.sql.SqlKind.COUNT) count = input.getFieldCount() + i;
      }
      assertTrue(sum >= 3 && count >= 3);
      sumColumn = sum;
      countColumn = count;

      selector =
          KeySelectorUtil.getRowDataSelector(
              getClass().getClassLoader(), new int[] {0}, InternalTypeInfo.of(input));
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

    @SuppressWarnings("unchecked")
    KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(boolean rocks)
        throws Exception {
      var transformation =
          (OneInputTransformation<RowData, RowData>) node().translateToPlan(planner);
      var operator =
          (KeyedProcessOperator<RowData, RowData, RowData>)
              ((SimpleOperatorFactory<?>) transformation.getOperatorFactory()).getOperator();
      assertInstanceOf(ProcTimeRowsBoundedPrecedingFunction.class, operator.getUserFunction());
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

    String push(
        KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness,
        long value,
        long time)
        throws Exception {
      harness.setProcessingTime(time);
      harness.getOutput().clear();
      var row =
          new RowDataSerializer(input)
              .toBinaryRow(GenericRowData.of(1L, value, TimestampData.fromEpochMillis(time)))
              .copy();
      harness.processElement(new StreamRecord<>(row));
      var result = harness.extractOutputValues();
      assertEquals(1, result.size());
      return result.get(0).getLong(sumColumn) + ":" + result.get(0).getLong(countColumn);
    }

    int retainedRows(KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness)
        throws Exception {
      var rows =
          harness
              .getOperator()
              .<RowData>getKeyedStateBackend()
              .getPartitionedState(
                  VoidNamespace.INSTANCE,
                  VoidNamespaceSerializer.INSTANCE,
                  new MapStateDescriptor<Long, List<RowData>>(
                      "inputState",
                      LongSerializer.INSTANCE,
                      new ListSerializer<>(new RowDataSerializer(input))));
      int count = 0;
      for (var entry : rows.entries()) count += entry.getValue().size();
      return count;
    }
  }
}

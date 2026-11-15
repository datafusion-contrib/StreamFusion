package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.NativePlannerTestEnvironment;

class GroupAggregatePreparationTest {
  /** Use real inferred changelog traits and planner context without translating a runtime graph. */
  private StreamPhysicalGroupAggregate aggregate() throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "false");
    var table = NativePlannerTestEnvironment.create(config);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.executeSql("CREATE TABLE src(k BIGINT,v BIGINT) WITH ('connector'='datagen')");
    table.executeSql("CREATE TABLE dst(k BIGINT,n BIGINT) WITH ('connector'='blackhole')");
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var operation =
        (ModifyOperation)
            planner.getParser().parse("INSERT INTO dst SELECT k,SUM(v) FROM src GROUP BY k").get(0);
    return find(planner.optimize(planner.translateToRel(operation)));
  }

  private StreamPhysicalGroupAggregate find(RelNode node) {
    if (node instanceof StreamPhysicalGroupAggregate) return (StreamPhysicalGroupAggregate) node;
    for (RelNode input : node.getInputs()) {
      var found = find(input);
      if (found != null) return found;
    }
    return null;
  }

  private Field field(Object object, String name) throws Exception {
    for (Class<?> type = object.getClass(); type != null; type = type.getSuperclass()) {
      try {
        var field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field;
      } catch (NoSuchFieldException ignored) {
      }
    }
    throw new NoSuchFieldException(name);
  }

  private void replaceCalls(StreamPhysicalGroupAggregate aggregate, AggregateCall call)
      throws Exception {
    // A malformed captured call must be declined before dereferencing its invalid input ordinal.
    field(aggregate, "aggCalls").set(aggregate, JavaScalaConversionUtil.toScala(List.of(call)));
  }

  private AggregateCall call(
      StreamPhysicalGroupAggregate aggregate,
      SqlAggFunction function,
      boolean approximate,
      List<Integer> arguments) {
    return AggregateCall.create(
        function,
        false,
        approximate,
        arguments,
        -1,
        aggregate.aggCalls().apply(0).getType(),
        "candidate");
  }

  @Test
  void unverifiedCallsDeclineBeforePayloadReadsAndPreserveRejectionOrder() throws Exception {
    var aggregate = aggregate();
    assertNotNull(aggregate);
    var unsupported =
        new SqlAggFunction(
            "unverified",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BIGINT,
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.USER_DEFINED_FUNCTION) {};
    var calls =
        List.of(
            call(aggregate, FlinkSqlOperatorTable.COUNT, false, List.of(99, 100)),
            call(aggregate, FlinkSqlOperatorTable.COUNT, true, List.of(99)),
            call(aggregate, unsupported, false, List.of(99)));
    var reasons =
        List.of(
            "GROUP BY: only single-argument aggregates",
            "GROUP BY: an approximate aggregate",
            "GROUP BY: only SUM/MIN/MAX/COUNT/AVG/FIRST_VALUE/LAST_VALUE/SINGLE_VALUE aggregates");
    for (int i = 0; i < calls.size(); i++) {
      replaceCalls(aggregate, calls.get(i));
      var decision = assertDoesNotThrow(() -> GroupAggregateMatcher.prepare(aggregate, null));
      assertFalse(decision.isAccepted());
      assertEquals(reasons.get(i), decision.reason());
    }
    var originalOutput = aggregate.getRowType();
    var types = (FlinkTypeFactory) aggregate.getCluster().getTypeFactory();
    field(aggregate, "rowType")
        .set(
            aggregate,
            types.buildRelNodeRowType(
                RowType.of(new RawType<>(Integer.class, IntSerializer.INSTANCE))));
    replaceCalls(aggregate, calls.get(0));
    assertEquals(
        "GROUP BY: an input or output column type the boundary cannot carry",
        GroupAggregateMatcher.prepare(aggregate, null).reason());
    field(aggregate, "rowType").set(aggregate, originalOutput);
    aggregate.replaceInput(
        0, org.apache.calcite.rel.logical.LogicalValues.createOneRow(aggregate.getCluster()));
    assertEquals(
        "GROUP BY: only single-argument aggregates",
        GroupAggregateMatcher.prepare(aggregate, null).reason());
  }

  @Test
  void acceptedPayloadIsCapturedOnceAndReusedForBuildAndRepeatedExplanation() throws Exception {
    var aggregate = aggregate();
    assertNotNull(aggregate);
    var context = new PlanContext(new StreamPlanScan(), Set.of());
    var rule =
        Substitution.preparing(
            StreamPhysicalGroupAggregate.class, "groupAggregate", GroupAggregateMatcher::prepare);
    assertNull(rule.reasonFor(aggregate, context));
    var prepared = context.admission(rule, aggregate);
    assertNotNull(prepared);
    assertTrue(prepared.isAccepted());
    replaceCalls(aggregate, call(aggregate, FlinkSqlOperatorTable.COUNT, false, List.of(99, 100)));
    var nativeNode =
        assertInstanceOf(
            StreamPhysicalNativeColumnarGroupAggregate.class,
            assertDoesNotThrow(() -> rule.apply(aggregate, context)));
    assertArrayEquals(new int[] {0}, (int[]) field(nativeNode, "aggregateKinds").get(nativeNode));
    assertArrayEquals(new int[] {1}, (int[]) field(nativeNode, "valueColumns").get(nativeNode));
    assertNull(rule.reasonFor(aggregate, context));
    assertNull(rule.reasonFor(aggregate, context));
    assertSame(prepared, context.admission(rule, aggregate));
  }
}

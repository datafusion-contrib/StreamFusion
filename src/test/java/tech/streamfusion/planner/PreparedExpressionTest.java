package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Set;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import tech.streamfusion.planner.common.PreparedExpression;

class PreparedExpressionTest {
  private static int[] inputColumns(PreparedExpression expression) {
    int[] kinds = expression.kinds(), payload = expression.payload();
    return java.util.stream.IntStream.range(0, kinds.length)
        .filter(index -> kinds[index] == 0).map(index -> payload[index]).toArray();
  }

  @Test
  void siblingInputPruningCannotChangeThePreparedPlanOrAnotherBranch() {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var integer = types.createSqlType(SqlTypeName.INTEGER);
    var source = RexExpression.encodeProjections(List.of(
        rex.makeCall(SqlStdOperatorTable.PLUS,
            rex.makeInputRef(integer, 2), rex.makeInputRef(integer, 0))), List.of("total"));
    var prepared = PreparedExpression.copyOf(source);
    int[] originalPayload = prepared.payload();
    var left = prepared.remapInputs(new int[] {1, -1, 0});
    var right = prepared.remapInputs(new int[] {0, -1, 1});
    assertArrayEquals(originalPayload, prepared.payload());
    assertArrayEquals(new int[] {0, 1}, inputColumns(left));
    assertArrayEquals(new int[] {1, 0}, inputColumns(right));
    assertEquals(Set.of(0, 2), prepared.referencedInputs());
    assertEquals(Set.of(0, 1), left.referencedInputs());
    source.remapInputs(new int[] {2, 1, 0});
    source.outputNames()[0] = "changed";
    assertArrayEquals(originalPayload, prepared.payload());
    assertArrayEquals(new String[] {"total"}, prepared.outputNames());
    assertSame(prepared, PreparedExpression.copyOf(prepared));
  }

  @Test
  void exposedWirePoolsCannotMutateAnAdmittedExpression() {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var source = RexExpression.encodeProjections(List.of(rex.makeLiteral("value")), List.of("label"));
    var prepared = PreparedExpression.copyOf(source);
    prepared.kinds()[0] = -1;
    prepared.payload()[0] = -1;
    prepared.childCounts()[0] = -1;
    prepared.projectionRoots()[0] = -1;
    prepared.strings()[0] = "changed";
    prepared.outputNames()[0] = "changed";
    assertArrayEquals(source.kinds(), prepared.kinds());
    assertArrayEquals(source.payload(), prepared.payload());
    assertArrayEquals(source.childCounts(), prepared.childCounts());
    assertArrayEquals(source.projectionRoots(), prepared.projectionRoots());
    assertArrayEquals(new String[] {"value"}, prepared.strings());
    assertArrayEquals(new String[] {"label"}, prepared.outputNames());
    assertNotSame(prepared.udfBinding(), prepared.udfBinding());
  }

  @Test
  void snapshotRetainsNativeTreeDepthAndFunctionMetadata() {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var integer = types.createSqlType(SqlTypeName.INTEGER);
    var input = rex.makeInputRef(integer, 0);
    var call = rex.makeCall(SqlStdOperatorTable.PLUS, input,
        rex.makeCall(SqlStdOperatorTable.MULTIPLY, input, input));
    var source = RexExpression.encodeProjections(List.of(call), List.of("value"));
    var prepared = PreparedExpression.copyOf(source);
    for (int depth = 1; depth <= 4; depth++)
      assertEquals(source.exceedsTreeDepth(depth), prepared.exceedsTreeDepth(depth));
    assertEquals(source.nativeFunctionCalls(), prepared.nativeFunctionCalls());
    assertEquals(source.jvmFunctionCalls(), prepared.jvmFunctionCalls());
    assertEquals(source.jvmExecutionReason(), prepared.jvmExecutionReason());
  }
}

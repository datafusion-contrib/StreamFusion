package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.junit.jupiter.api.Test;
import tech.streamfusion.planner.common.Admission;
import tech.streamfusion.planner.common.ExpressionKind;

class ScalarCastRulesTest {
  private final FlinkTypeFactory types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
  private final RexBuilder rex = new RexBuilder(types);

  private RexCall cast(SqlTypeName source, SqlTypeName target, SqlOperator operator) {
    return (RexCall) rex.makeCall(types.createSqlType(target), operator,
        List.of(rex.makeInputRef(types.createSqlType(source), 0)));
  }

  @Test
  void parsingRulesShareFailureEffectsWithTheirSelectedPlan() {
    for (SqlTypeName target : List.of(SqlTypeName.TINYINT, SqlTypeName.SMALLINT,
        SqlTypeName.INTEGER, SqlTypeName.BIGINT, SqlTypeName.BOOLEAN)) {
      for (SqlOperator operator : List.of(FlinkSqlOperatorTable.CAST, FlinkSqlOperatorTable.TRY_CAST)) {
        var call = cast(SqlTypeName.VARCHAR, target, operator);
        for (boolean legacy : new boolean[] {false, true}) {
          var context = new Context(legacy);
          var prepared = ScalarCastRules.prepare(call, context);
          assertTrue(prepared.isAccepted(), call.toString());
          boolean canFail = operator == FlinkSqlOperatorTable.CAST && !legacy;
          assertEquals(canFail, prepared.value().canFail(), call.toString());
          assertEquals(canFail, ScalarCastRules.mayFailOnRow(call, legacy), call.toString());
          assertTrue(context.emitted.isEmpty(), "Admission must not emit or evaluate operands");
          assertTrue(prepared.value().emit(context));
          assertEquals(1, context.operands);
        }
      }
    }
  }

  @Test
  void tryCastDoesNotCatchAnExceptionWhileEvaluatingItsOperand() {
    var call = cast(SqlTypeName.VARCHAR, SqlTypeName.BOOLEAN, FlinkSqlOperatorTable.TRY_CAST);
    var context = new Context(false);
    context.fallibleOperand = true;
    var prepared = ScalarCastRules.prepare(call, context);
    assertTrue(prepared.isAccepted());
    assertFalse(prepared.value().canFail());
    assertTrue(prepared.value().emit(context));
    assertEquals(1, context.hostExpressions);
    assertEquals(0, context.operands);
    assertTrue(context.emitted.isEmpty());
  }

  @Test
  void binaryRepresentationTracksTheConfiguredWidthContract() {
    var binary = types.createSqlType(SqlTypeName.BINARY, 4);
    var input = rex.makeInputRef(types.createSqlType(SqlTypeName.VARCHAR, 16), 0);
    var call = (RexCall) rex.makeCall(binary, FlinkSqlOperatorTable.CAST, List.of(input));
    assertEquals(ScalarCastRules.Representation.FIXED_BINARY,
        ScalarCastRules.prepare(call, new Context(false)).value().representation());
    assertEquals(ScalarCastRules.Representation.VARIABLE_BINARY,
        ScalarCastRules.prepare(call, new Context(true)).value().representation());
    var unknown = new Context(null);
    var prepared = ScalarCastRules.prepare(call, unknown);
    assertTrue(prepared.isAccepted());
    assertTrue(prepared.value().emit(unknown));
    assertEquals(1, unknown.hostExpressions);
  }

  @Test
  void asciiAndJavaStringProofsUseTheSameCastFamilyAsLowering() {
    var string = types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);
    var integerText = (RexCall) rex.makeCall(string, FlinkSqlOperatorTable.CAST,
        List.of(rex.makeInputRef(types.createSqlType(SqlTypeName.INTEGER), 0)));
    var binaryText = (RexCall) rex.makeCall(string, FlinkSqlOperatorTable.CAST,
        List.of(rex.makeInputRef(types.createSqlType(SqlTypeName.VARBINARY), 0)));
    assertEquals(ScalarCastRules.Representation.ASCII_STRING,
        ScalarCastRules.prepare(integerText, new Context(false)).value().representation());
    assertSame(integerText.getOperands().get(0), ScalarCastRules.integerTextOperand(integerText));
    assertEquals(ScalarCastRules.Representation.JAVA_STRING,
        ScalarCastRules.prepare(binaryText, new Context(false)).value().representation());
    assertNull(ScalarCastRules.integerTextOperand(binaryText));
    assertEquals(Admission.Status.DECLINED,
        ScalarCastRules.prepare(integerText, new Context(null)).status());
    assertEquals(Admission.Status.NO_MATCH,
        ScalarCastRules.prepare(cast(SqlTypeName.DATE, SqlTypeName.TIMESTAMP,
            FlinkSqlOperatorTable.CAST), new Context(false)).status());
  }

  private static final class Context implements ScalarCastRules.Context {
    private final Boolean legacy;
    final List<ExpressionKind> emitted = new ArrayList<>();
    boolean fallibleOperand;
    int operands;
    int hostExpressions;

    Context(Boolean legacy) { this.legacy = legacy; }
    @Override public Boolean legacyBehaviour() { return legacy; }
    @Override public boolean requiresRowEvaluation(RexNode operand) { return fallibleOperand; }
    @Override public boolean supportsBooleanText(RexNode operand) { return true; }
    @Override public boolean operand(RexNode operand) { operands++; return true; }
    @Override public boolean host(RexCall call) { hostExpressions++; return true; }
    @Override public void node(ExpressionKind kind, int payload, int children) { emitted.add(kind); }
    @Override public void integerLiteral(int value) { emitted.add(ExpressionKind.LIT_INT); }
    @Override public void booleanLiteral(boolean value) { emitted.add(ExpressionKind.LIT_BOOL); }
  }
}

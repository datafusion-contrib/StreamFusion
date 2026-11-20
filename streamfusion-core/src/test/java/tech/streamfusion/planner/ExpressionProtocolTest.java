package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import tech.streamfusion.planner.common.CastTarget;
import tech.streamfusion.planner.common.ExpressionKind;
import tech.streamfusion.planner.common.ScalarOp;
import tech.streamfusion.planner.common.GroupAggregateKind;
import tech.streamfusion.planner.common.WindowAggregateKind;

class ExpressionProtocolTest {
  @Test
  void legacyWireCodesKeepTheirMeaningAndRetiredKindsRemainInvalid() {
    assertEquals(ExpressionKind.LIT_INTERVAL, ExpressionKind.fromCode(15));
    assertEquals(ExpressionKind.DECIMAL_DIVIDE, ExpressionKind.fromCode(20));
    assertEquals(ExpressionKind.DECIMAL_MOD, ExpressionKind.fromCode(21));
    assertEquals(ExpressionKind.ROW_UDF, ExpressionKind.fromCode(38));
    assertEquals(ExpressionKind.DECIMAL_IF, ExpressionKind.fromCode(47));
    assertThrows(IllegalArgumentException.class, () -> ExpressionKind.fromCode(10));
    assertThrows(IllegalArgumentException.class, () -> ExpressionKind.fromCode(-1));
    assertThrows(IllegalArgumentException.class, () -> ExpressionKind.fromCode(48));
    assertEquals(ExpressionKind.STRING_COMPARISON, ExpressionKind.fromCode(49));
    assertEquals(CastTarget.TINYINT, CastTarget.fromCode(0));
    assertEquals(CastTarget.STRING, CastTarget.fromCode(6));
    assertThrows(IllegalArgumentException.class, () -> CastTarget.fromCode(7));
  }

  @Test
  void scalarAndAggregateCodeFamiliesKeepTheExistingWireNamespaces() {
    assertEquals(0, ScalarOp.ADD);
    assertEquals(52, ScalarOp.CHAR_LENGTH);
    assertEquals(141, ScalarOp.JSON_VALUE);
    assertEquals(202, ScalarOp.JSON_VALUE_JAVA_INPUT);
    assertEquals(254, ScalarOp.JSON_QUERY);
    assertEquals(9, GroupAggregateKind.SUM_DISTINCT);
    assertEquals(9, WindowAggregateKind.RETRACT_SUM);
    assertEquals(43, GroupAggregateKind.JSON_OBJECT_MERGE_NULL_ON_NULL);
    assertEquals(43, WindowAggregateKind.COUNT_DISTINCT_WITH_COUNTS);
  }
}

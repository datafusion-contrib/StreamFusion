package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import tech.streamfusion.planner.common.CastTarget;
import tech.streamfusion.planner.common.ExpressionKind;

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
    assertEquals(CastTarget.TINYINT, CastTarget.fromCode(0));
    assertEquals(CastTarget.STRING, CastTarget.fromCode(6));
    assertThrows(IllegalArgumentException.class, () -> CastTarget.fromCode(7));
  }
}

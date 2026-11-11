package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.WatermarkExpression;
import tech.streamfusion.planner.ExpressionPlanning;
import tech.streamfusion.planner.common.EncodedExpression;

class WatermarkExpressionPlannerTest {
  private final JavaTypeFactoryImpl types = new JavaTypeFactoryImpl();
  private final RexBuilder rex = new RexBuilder(types);
  private final RexNode rowtime =
      rex.makeInputRef(types.createSqlType(SqlTypeName.TIMESTAMP, 3), 0);

  @Test
  void intervalFamilyAndExactLiteralWidthSurviveTheSharedEncoding() {
    var months = encode("12", TimeUnit.YEAR);
    assertEquals(WatermarkExpression.SUBTRACT_MONTHS, months.payload()[0]);
    assertArrayEquals(new long[] {12}, months.longs());
    assertEquals(7, months.kinds()[2]); // INT literal, as in Flink's year-month representation.
    var days = encode("86400000", TimeUnit.DAY);
    assertEquals(WatermarkExpression.SUBTRACT_MILLIS, days.payload()[0]);
    assertArrayEquals(new long[] {86400000}, days.longs());
    assertEquals(1, days.kinds()[2]);
    assertNotNull(encode("2147483647", TimeUnit.MONTH));
    assertNotNull(encode("9223372036854775807", TimeUnit.SECOND));
  }

  @Test
  void nonIntervalsNegativeFractionalAndTruncatedValuesAreNotAdmitted() {
    assertNull(encode(rex.makeExactLiteral(BigDecimal.ONE)));
    assertNull(encode("-1", TimeUnit.MONTH));
    assertNull(encode("-1", TimeUnit.SECOND));
    assertNull(encode("1.5", TimeUnit.MONTH));
    assertNull(encode("2147483648", TimeUnit.MONTH));
    assertNull(encode("4294967297", TimeUnit.MONTH));
    assertNull(encode("9223372036854775808", TimeUnit.SECOND));
    assertNull(encode("0.5", TimeUnit.SECOND));
  }

  @Test
  void nativeOutputTypesAreCheckedBeforeAdmission() {
    var expression = subtract(rowtime, interval("1", TimeUnit.MONTH));
    assertNotNull(
        WatermarkExpressionPlanner.encode(
            expression, 0, false, types.builder().add("rt", rowtime.getType()).build()));
    assertNull(
        WatermarkExpressionPlanner.encode(
            expression,
            0,
            false,
            types.builder().add("rt", types.createSqlType(SqlTypeName.VARCHAR, 10)).build()));
    assertNull(
        WatermarkExpressionPlanner.encode(
            expression, 1, false, types.builder().add("rt", rowtime.getType()).build()));
  }

  @Test
  void chainedIntervalsRemainSeparateNodesInTheirOriginalOrder() {
    var expression =
        subtract(
            subtract(rowtime, interval("1", TimeUnit.MONTH)), interval("86400000", TimeUnit.DAY));
    var encoded = ExpressionPlanning.encodeWatermark(expression, 0, false);
    assertNotNull(encoded);
    assertEquals(WatermarkExpression.SUBTRACT_MILLIS, encoded.payload()[0]);
    assertEquals(WatermarkExpression.SUBTRACT_MONTHS, encoded.payload()[1]);
    assertArrayEquals(new long[] {1, 86400000}, encoded.longs());
  }

  private EncodedExpression encode(String value, TimeUnit unit) {
    return encode(interval(value, unit));
  }

  private EncodedExpression encode(RexNode interval) {
    return ExpressionPlanning.encodeWatermark(subtract(rowtime, interval), 0, false);
  }

  private RexNode interval(String value, TimeUnit unit) {
    return rex.makeIntervalLiteral(
        new BigDecimal(value), new SqlIntervalQualifier(unit, null, SqlParserPos.ZERO));
  }

  private RexNode subtract(RexNode timestamp, RexNode interval) {
    return rex.makeCall(
        rowtime.getType(), SqlStdOperatorTable.MINUS, java.util.List.of(timestamp, interval));
  }
}

package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

class ExpressionEncodingContractTest {
  private final JavaTypeFactoryImpl types = new JavaTypeFactoryImpl();
  private final RexBuilder rex = new RexBuilder(types);

  @Test
  void mixedProjectionRetainsTheLegacyPreorderPoolsAndInputCoordinates() {
    var integer = types.createSqlType(SqlTypeName.INTEGER);
    RexNode sum =
        rex.makeCall(
            SqlStdOperatorTable.PLUS,
            rex.makeInputRef(integer, 2),
            rex.makeExactLiteral(BigDecimal.valueOf(3), integer));
    var encoded =
        RexExpression.encodeProjections(
            List.of(sum, rex.makeApproxLiteral(new BigDecimal("1.25")), rex.makeLiteral("wire")),
            List.of("sum", "fraction", "text"));
    assertNotNull(encoded);
    // The outer narrowing node retains Java's INTEGER overflow behavior.
    assertArrayEquals(new int[] {18, 6, 0, 7, 2, 3}, encoded.kinds());
    assertArrayEquals(new int[] {2, 0, 2, 0, 0, 0}, encoded.payload());
    assertArrayEquals(new int[] {1, 2, 0, 0, 0, 0}, encoded.childCounts());
    assertArrayEquals(new int[] {0, 4, 5}, encoded.projectionRoots());
    assertArrayEquals(new long[] {3}, encoded.longs());
    assertArrayEquals(new double[] {1.25}, encoded.doubles());
    assertArrayEquals(new String[] {"wire"}, encoded.strings());
    assertEquals(Set.of(2), encoded.referencedInputs());
    assertFalse(encoded.exceedsTreeDepth(3));
    assertTrue(encoded.exceedsTreeDepth(2));
    encoded.remapInputs(new int[] {4, 3, 1});
    assertArrayEquals(new int[] {2, 0, 1, 0, 0, 0}, encoded.payload());
    var payload = encoded.payload();
    payload[2] = 99;
    assertEquals(Set.of(1), encoded.referencedInputs());
    assertArrayEquals(new long[] {3}, encoded.longs());
  }

  @Test
  void nullStringPoolsAndFloatingPayloadBitsSurviveTransportSnapshots() {
    var encoded =
        RexExpression.encodeProjections(
            List.of(rex.makeNullLiteral(types.createSqlType(SqlTypeName.VARCHAR))),
            List.of("null"));
    assertNotNull(encoded);
    assertArrayEquals(new String[] {null}, encoded.strings());
    var buffer = new ExpressionBuffer();
    long nan = 0x7ff8000000000031L;
    buffer.appendDouble(Double.longBitsToDouble(nan));
    buffer.appendDouble(-0.0);
    assertEquals(nan, Double.doubleToRawLongBits(buffer.doubles()[0]));
    assertEquals(Long.MIN_VALUE, Double.doubleToRawLongBits(buffer.doubles()[1]));
    var snapshot = buffer.doubles();
    snapshot[0] = 0;
    assertEquals(nan, Double.doubleToRawLongBits(buffer.doubles()[0]));
  }

  @Test
  void udfResolutionKeepsExactOverloadsVarargsAndAmbiguity() throws Exception {
    var argument = rex.makeInputRef(types.createSqlType(SqlTypeName.INTEGER), 0);
    assertEquals(
        Overloaded.class.getMethod("eval", Integer.class),
        UdfCallResolver.resolveEval(new Overloaded(), List.of(argument)));
    assertEquals(
        Overloaded.class.getMethod("eval", Integer[].class),
        UdfCallResolver.resolveEval(new Overloaded(), List.of(argument, argument)));
    assertNull(UdfCallResolver.resolveEval(new Ambiguous(), List.of(argument)));
  }

  public static final class Overloaded {
    public Object eval(Number value) {
      return value;
    }

    public Object eval(Integer value) {
      return value;
    }

    public Object eval(Integer... values) {
      return values;
    }
  }

  public static final class Ambiguous {
    public Object eval(java.io.Serializable value) {
      return value;
    }

    public Object eval(Comparable<?> value) {
      return value;
    }
  }
}

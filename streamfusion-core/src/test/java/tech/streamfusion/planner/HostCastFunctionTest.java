package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.List;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

class HostCastFunctionTest {

  private static final List<LogicalType> NOT_NULL_STRINGS =
      List.of(
          new CharType(false, 3),
          new VarCharType(false, 5),
          new VarCharType(false, VarCharType.MAX_LENGTH));

  private static final List<LogicalType> NUMBERS =
      List.of(
          new TinyIntType(),
          new SmallIntType(),
          new IntType(),
          new BigIntType(),
          new FloatType(),
          new DoubleType());

  /**
   * A NOT NULL input type's generated cast has no null guard — it trims the argument directly — so
   * warming the executor with a null failed the operator's open() instead of any row.
   */
  @Test
  void opensForNotNullStringToNumberCasts() {
    for (LogicalType input : NOT_NULL_STRINGS) {
      for (LogicalType target : NUMBERS) {
        assertDoesNotThrow(
            () -> new HostCastFunction(input, target).open(null), input + " -> " + target);
      }
    }
  }

  /**
   * open() warms the executor with a value the cast rules accept, so every input type the encoder
   * admits must have one. A type reaching the warm-up without a priming value throws, which is the
   * point: widening the admitted casts has to decide what primes them rather than failing at startup
   * on a null the generated cast dereferences.
   */
  @Test
  void opensForEveryAdmittedInputType() {
    LogicalType string = new VarCharType(false, 12);
    LogicalType decimal = new DecimalType(false, 10, 2);
    for (LogicalType number : NUMBERS) {
      LogicalType input = number.copy(false);
      assertDoesNotThrow(
          () -> new HostCastFunction(input, string).open(null), input + " -> " + string);
    }
    assertDoesNotThrow(() -> new HostCastFunction(decimal, string).open(null));
    assertDoesNotThrow(() -> new HostCastFunction(string, string).open(null));
    assertDoesNotThrow(
        () -> new HostCastFunction(new DoubleType(false), decimal).open(null));
  }

  /** Warming must not consume the executor: the first real row still casts. */
  @Test
  void castsAfterWarmup() {
    HostCastFunction function =
        new HostCastFunction(new VarCharType(false, 5), new IntType());
    function.open(null);
    assertEquals(-7, function.eval("-7"));
  }

  @Test
  void legacyExecutorsKeepFormattingAndLengthRules() {
    var string = new VarCharType();
    var castBoolean = new HostCastFunction(new BooleanType(), string, true);
    assertEquals("true", castBoolean.eval(true));
    assertEquals("false", castBoolean.eval(false));
    var castChar = new HostCastFunction(string, new CharType(3), true);
    assertEquals("12345", castChar.eval("12345"));
    assertEquals("x", castChar.eval("x"));
    assertNull(castChar.eval(null));
    var castDecimalText = new HostCastFunction(new DecimalType(7, 2), string, true);
    assertEquals("1.25", castDecimalText.eval(new BigDecimal("1.25")));
  }

  @Test
  void cachedCastExceptionsSurviveClosingTheFirstJobClassLoader() throws Exception {
    var thread = Thread.currentThread();
    var previous = thread.getContextClassLoader();
    var decimal = new DecimalType(27, 7);
    try {
      for (int job = 0; job < 2; job++) {
        try (var loader =
            org.apache.flink.util.FlinkUserCodeClassLoaders.parentFirst(
                new java.net.URL[0], getClass().getClassLoader(), ignored -> {}, true)) {
          thread.setContextClassLoader(loader);
          var cast = new HostCastFunction(new FloatType(), decimal);
          cast.open(null);
          assertSame(loader, thread.getContextClassLoader());
          assertEquals(new BigDecimal("1.2500000"), cast.eval(1.25f));
          if (job == 1) {
            Throwable failure = assertThrows(Exception.class, () -> cast.eval(Float.NaN));
            while (failure.getCause() != null) failure = failure.getCause();
            assertInstanceOf(NumberFormatException.class, failure);
          }
        }
      }
    } finally {
      thread.setContextClassLoader(previous);
    }
  }
}

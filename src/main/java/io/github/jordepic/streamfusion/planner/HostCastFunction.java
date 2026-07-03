package io.github.jordepic.streamfusion.planner;

import java.math.BigDecimal;
import java.time.ZoneId;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.data.utils.CastExecutor;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.functions.casting.CastRule;
import org.apache.flink.table.planner.functions.casting.CastRuleProvider;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * A SQL {@code CAST} evaluated by Flink's own cast rules through the columnar JVM upcall: the encoder
 * emits this function for the casts whose formatting/parsing the native engine cannot reproduce
 * byte-for-byte — number↔string (Java's float/double rendering is even JDK-version-dependent, so no
 * native port can be identical to <em>the running host</em>), narrowing a string to {@code VARCHAR(n)},
 * and padding to {@code CHAR(n)}. At first use it builds the very {@link CastExecutor} Flink's codegen
 * would embed ({@link CastRuleProvider}), so trailing zeros, scientific-notation thresholds, trim
 * semantics, and failure behavior (an unparsable string fails the job, exactly as the host's default
 * cast does) are the host's own. Serializable — the descriptor travels to task managers and the
 * executor is rebuilt there.
 *
 * <p>The executor is created with the default (non-legacy) cast behavior; the encoder declines these
 * casts when {@code table.exec.legacy-cast-behaviour} is enabled, whose null-on-failure semantics this
 * would not match.
 */
public final class HostCastFunction extends ScalarFunction {

  private static final long serialVersionUID = 1L;

  private final LogicalType inputType;
  private final LogicalType targetType;

  private transient CastExecutor<Object, Object> executor;

  public HostCastFunction(LogicalType inputType, LogicalType targetType) {
    this.inputType = inputType;
    this.targetType = targetType;
  }

  @SuppressWarnings("unchecked")
  public Object eval(Object value) {
    if (value == null) {
      return null;
    }
    if (executor == null) {
      ClassLoader classLoader = HostCastFunction.class.getClassLoader();
      // The admitted casts (number↔string, string length) never consult the zone; UTC is a placeholder.
      executor =
          (CastExecutor<Object, Object>)
              CastRuleProvider.create(
                  CastRule.Context.create(
                      false,
                      false,
                      ZoneId.of("UTC"),
                      classLoader,
                      new CodeGeneratorContext(new Configuration(), classLoader)),
                  inputType,
                  targetType);
      if (executor == null) {
        throw new IllegalStateException(
            "no cast rule for " + inputType + " -> " + targetType);
      }
    }
    return fromInternal(executor.cast(toInternal(value)));
  }

  /** The upcall marshals external values (String/BigDecimal/boxed numbers); the executor speaks
   * Flink's internal data. */
  private Object toInternal(Object value) {
    if (value instanceof String) {
      return StringData.fromString((String) value);
    }
    if (value instanceof BigDecimal) {
      DecimalType type = (DecimalType) inputType;
      return DecimalData.fromBigDecimal((BigDecimal) value, type.getPrecision(), type.getScale());
    }
    return value;
  }

  private static Object fromInternal(Object value) {
    if (value instanceof StringData) {
      return value.toString();
    }
    if (value instanceof DecimalData) {
      return ((DecimalData) value).toBigDecimal();
    }
    return value;
  }
}

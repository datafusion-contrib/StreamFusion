package tech.streamfusion.planner;

import java.math.BigDecimal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.utils.CastExecutor;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
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
 * <p>The executor retains the planner's configured cast behavior, including legacy formatting,
 * length and failure rules.
 */
public final class HostCastFunction extends ScalarFunction {

  private static final long serialVersionUID = 1L;

  /** Enough invocations to cross java.lang.reflect's native-to-generated accessor threshold. */
  private static final int WARMUP_INVOCATIONS = 20;

  private final LogicalType inputType;
  private final LogicalType targetType;
  private final boolean legacyBehaviour;

  private transient CastExecutor<Object, Object> executor;

  public HostCastFunction(LogicalType inputType, LogicalType targetType) {
    this(inputType, targetType, false);
  }

  public HostCastFunction(LogicalType inputType, LogicalType targetType, boolean legacyBehaviour) {
    this.inputType = inputType;
    this.targetType = targetType;
    this.legacyBehaviour = legacyBehaviour;
  }

  @Override
  public void open(FunctionContext context) {
    initializeExecutor();
  }

  @SuppressWarnings("unchecked")
  public Object eval(Object value) {
    if (value == null) {
      return null;
    }
    if (executor == null) {
      initializeExecutor();
    }
    return fromInternal(executor.cast(toInternal(value)));
  }

  @SuppressWarnings("unchecked")
  private void initializeExecutor() {
    if (executor == null) {
      var thread = Thread.currentThread();
      var previous = thread.getContextClassLoader();
      var classLoader = CastRuleProvider.class.getClassLoader();
      // Flink caches Janino expressions without a loader key. Janino inherits the thread's
      // context loader, so the cast context alone cannot prevent retaining a retired job loader.
      thread.setContextClassLoader(classLoader);
      try {
        // The admitted casts (number↔string, string length) never consult the zone; UTC is a
        // placeholder.
        executor =
            (CastExecutor<Object, Object>)
                CastRuleProvider.create(
                    tech.streamfusion.compat.FlinkCompat.castContext(classLoader, legacyBehaviour),
                    inputType,
                    targetType);
        if (executor == null) {
          throw new IllegalStateException("no cast rule for " + inputType + " -> " + targetType);
        }
        // Prime generated reflective accessors before the first native batch. These casts only
        // reference library types; no user function runs under the library loader.
        Object sample = warmupValue(inputType);
        for (int i = 0; i < WARMUP_INVOCATIONS; i++) {
          executor.cast(sample);
        }
      } finally {
        thread.setContextClassLoader(previous);
      }
    }
  }

  /**
   * A value the executor can actually consume. {@code null} is not legal input for a NOT NULL type,
   * whose generated cast dereferences the argument without a guard. Unknown types throw rather than
   * warm with a null, so widening the admitted casts has to decide what primes them.
   */
  private static Object warmupValue(LogicalType type) {
    switch (type.getTypeRoot()) {
      case CHAR:
      case VARCHAR:
        return StringData.fromString("0");
      case BOOLEAN:
        return Boolean.FALSE;
      case TINYINT:
        return (byte) 0;
      case SMALLINT:
        return (short) 0;
      case INTEGER:
        return 0;
      case BIGINT:
        return 0L;
      case FLOAT:
        return 0f;
      case DOUBLE:
        return 0d;
      case DECIMAL:
        DecimalType decimal = (DecimalType) type;
        return DecimalData.fromBigDecimal(BigDecimal.ZERO, decimal.getPrecision(), decimal.getScale());
      default:
        throw new IllegalStateException("no warm-up value for " + type);
    }
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

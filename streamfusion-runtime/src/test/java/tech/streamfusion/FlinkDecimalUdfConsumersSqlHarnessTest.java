package tech.streamfusion;

import java.math.BigDecimal;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkDecimalUdfConsumersSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "decimal_from_text(s) IS NULL",
        "decimal_from_text(s) IS NOT NULL",
        "CASE WHEN decimal_from_text(s) IS NULL THEN 'null' ELSE 'value' END",
        "COALESCE(decimal_from_text(s), CAST(42 AS DECIMAL(5,2)))",
        "CAST(decimal_from_text(s) AS STRING)",
        "external_decimal(decimal_from_text(s))",
        "external_text(decimal_from_text(s))",
        "CASE WHEN s IS NULL THEN CAST(NULL AS DECIMAL(38,9)) "
            + "ELSE external_decimal(decimal_from_text(s)) + 1 END",
        "external_decimal(decimal_from_text(s)) > CAST(999 AS DECIMAL(38,9))"
      })
  void consumersPreserveExternalNullnessAndNestedValues(String expression) throws Exception {
    NativeParity.assertParity(() -> environment(5003), "SELECT id, " + expression + " FROM src");
  }

  @Test
  void predicatesFilterOnExternalNullness() throws Exception {
    NativeParity.assertParity(
        () -> environment(5003),
        "SELECT id, decimal_from_text(s) FROM src WHERE decimal_from_text(s) IS NOT NULL");
  }

  @ParameterizedTest
  @ValueSource(strings = {"decimal_from_text(s) + 1", "decimal_identity(decimal_from_text(s))"})
  void overflowConsumersPreserveHostExceptions(String expression) {
    NativeFailureParity.run(() -> environment(32), "SELECT " + expression + " FROM src")
        .assertFailure(
            NullPointerException.class,
            "",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @Test
  void sharedFunctionsOpenAndCloseOnceAcrossGeneratedAndDirectCalls() throws Exception {
    NativeParity.assertParity(
        () -> environment(5003),
        "SELECT id, lifecycle_decimal(s), lifecycle_decimal(s) IS NULL, "
            + "external_text(lifecycle_decimal(s)), "
            + "CASE WHEN lifecycle_decimal(s) IS NULL THEN 'null' ELSE 'value' END FROM src");
  }

  @Test
  void generatedConsumersKeepSpecializationAndConversionGates() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> environment(32),
        "SELECT decimal_internal(s) IS NULL FROM src",
        "unsupported Java conversion class");
    NativeParity.assertFallbackReasonContains(
        () -> {
          var table = environment(32);
          table.createTemporarySystemFunction(
              "type_of", new FlinkUdfLifecycleSqlHarnessTest.SpecializedTypeOf());
          return table;
        },
        "SELECT type_of(decimal_from_text(s)) FROM src",
        "UDF specialization requires Flink's code-generation context");
  }

  private static TableEnvironment environment(int count) {
    var table = FlinkUdfExactTypesSqlHarnessTest.environment(count);
    table.createTemporarySystemFunction("external_text", ExternalText.class);
    table.createTemporarySystemFunction("external_decimal", ExternalDecimal.class);
    table.createTemporarySystemFunction("lifecycle_decimal", new LifecycleDecimal());
    return table;
  }

  @FunctionHint(input = @DataTypeHint("DECIMAL(5,2)"), output = @DataTypeHint("STRING"))
  public static class ExternalText extends ScalarFunction {
    public String eval(BigDecimal value) {
      return value == null ? "external-null" : value.toPlainString();
    }
  }

  @FunctionHint(input = @DataTypeHint("DECIMAL(5,2)"), output = @DataTypeHint("DECIMAL(38,9)"))
  public static class ExternalDecimal extends ScalarFunction {
    public BigDecimal eval(BigDecimal value) {
      return value;
    }
  }

  @FunctionHint(output = @DataTypeHint("DECIMAL(5,2)"))
  public static class LifecycleDecimal extends ScalarFunction {
    private transient int opens;

    @Override
    public void open(FunctionContext context) {
      if (++opens != 1) throw new IllegalStateException("decimal function opened twice");
    }

    public BigDecimal eval(String value) {
      if (opens != 1) throw new IllegalStateException("decimal function is not open");
      return value == null ? null : new BigDecimal(value);
    }

    @Override
    public void close() {
      if (opens-- != 1) throw new IllegalStateException("decimal function closed twice");
    }
  }
}

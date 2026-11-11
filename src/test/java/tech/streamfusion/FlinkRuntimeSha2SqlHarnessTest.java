package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkRuntimeSha2SqlHarnessTest {
  @ParameterizedTest
  @ValueSource(ints = {1, 128})
  void invalidLiteralWidthKeepsTheInitializationFailure(int bits) throws Exception {
    NativeFailureParity.run(() -> invalid("abc"), "SELECT SHA2(s, " + bits + ") FROM src")
        .assertFailure(
            RuntimeException.class,
            "Unsupported algorithm",
            NativeFailureParity.Phase.INITIALIZATION,
            NativeFailureParity.Route.NATIVE);
    NativeFailureParity.run(() -> invalid(null), "SELECT SHA2(s, " + bits + ") FROM src")
        .assertFailure(
            RuntimeException.class,
            "Unsupported algorithm",
            NativeFailureParity.Phase.INITIALIZATION,
            NativeFailureParity.Route.NATIVE);
    NativeFailureParity.run(
            () -> invalid("abc"), "SELECT SHA2(s, " + bits + ") FROM src WHERE bits <> 128")
        .assertFailure(
            RuntimeException.class,
            "Unsupported algorithm",
            NativeFailureParity.Phase.INITIALIZATION,
            NativeFailureParity.Route.NATIVE);
  }

  @Test
  void runtimeWidthsAndNullLiteralMatchReleasedFlink() throws Exception {
    BuiltinFunctionParity.assertParity(
        this::inputs, "SELECT id, SHA2(s, bits), SHA2(s, CAST(NULL AS INT)) FROM src");
  }

  @Test
  void predicatesCaseAndComputedInputsKeepTheSameValues() throws Exception {
    BuiltinFunctionParity.assertParity(
        this::inputs, "SELECT id FROM src WHERE SHA2(s, bits) LIKE 'a%'");
    BuiltinFunctionParity.assertParity(
        this::inputs,
        "SELECT id, CASE WHEN bits = 224 THEN SHA2(s, bits) ELSE s END, "
            + "SHA2(CONCAT(s, '!'), bits), SHA2(s, bits + 0) FROM src");
  }

  @Test
  void invalidRuntimeWidthKeepsTheJdkException() throws Exception {
    NativeFailureParity.run(() -> invalid("abc"), "SELECT SHA2(s, bits) FROM src")
        .assertFailure(
            java.security.NoSuchAlgorithmException.class,
            "SHA-128",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @Test
  void nullSourceAndSkippedBranchesDoNotEvaluateInvalidWidths() throws Exception {
    BuiltinFunctionParity.assertParity(() -> invalid(null), "SELECT SHA2(s, bits) FROM src");
    BuiltinFunctionParity.assertParity(
        () -> invalid("abc"), "SELECT CASE WHEN bits = 128 THEN s ELSE SHA2(s, bits) END FROM src");
    BuiltinFunctionParity.assertParity(
        () -> invalid("abc"), "SELECT SHA2(s, bits) FROM src WHERE bits <> 128");
  }

  @Test
  void nullWidthDoesNotDiscardAFailingSourceExpression() throws Exception {
    NativeFailureParity.run(
            () -> invalid("abc"),
            "SELECT SHA2(CAST(1 / (bits - bits) AS STRING), CAST(NULL AS INT)) FROM src")
        .assertFailure(
            ArithmeticException.class,
            "/ by zero",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
    BuiltinFunctionParity.assertParity(
        () -> invalid("abc"),
        "SELECT SHA2(CAST(1 / (bits - bits) AS STRING), CAST(NULL AS INT)) FROM src WHERE bits <>"
            + " 128");
  }

  private TableEnvironment invalid(String source) {
    return BuiltinFunctionParity.environment(
        ROW(FIELD("s", STRING()), FIELD("bits", INT())), List.of(Row.of(source, 128)));
  }

  private TableEnvironment inputs() {
    List<Row> rows = new ArrayList<>();
    for (String source :
        new String[] {
          "", "abc", "a\u0000b", "\u4e2d\ud83d\ude00", "\ud800?", "\u00e9".repeat(4097), null
        }) {
      for (Integer bits : new Integer[] {224, 256, 384, 512, null}) {
        rows.add(Row.of(rows.size(), source, bits));
      }
    }
    return BuiltinFunctionParity.environment(
        ROW(FIELD("id", INT()), FIELD("s", STRING()), FIELD("bits", INT())), rows);
  }
}

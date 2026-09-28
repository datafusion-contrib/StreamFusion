package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkFixedBinarySqlHarnessTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void nestedFixedBinaryResultsRemainOutsideTheGeneratedBoundaryWhitelist(boolean legacy) throws Exception {
    NativeParity.assertFallbackReasonContains(() -> strings(legacy),
        "SELECT ARRAY[TRY_CAST(s AS BINARY(4))], JSON_OBJECT('id' VALUE id) FROM src",
        "row-fused UDF output type");
  }

  @Test
  void fixedResultsCrossNativeOperatorBoundariesButLegacyVariableBytesFallBack() throws Exception {
    String sql = "SELECT TRY_CAST(s AS BINARY(4)), COUNT(*) FROM src GROUP BY TRY_CAST(s AS BINARY(4))";
    BuiltinFunctionParity.assertParity(() -> strings(false), sql);
    NativeParity.assertFallbackReasonContains(() -> strings(true), sql, "legacy binary variable bytes");
  }

  @Test
  void singleResultAndLiteralKeepTheirDeclaredWidth() throws Exception {
    BuiltinFunctionParity.assertParity(() -> strings(false), "SELECT TRY_CAST(s AS BINARY(4)) FROM src");
    BuiltinFunctionParity.assertParity(this::binary, "SELECT X'00FF', CAST(NULL AS BINARY(2)) FROM src WHERE n = 2");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void stringCastsRetainWidthsUtf8BytesAndLegacyBehavior(boolean legacy) throws Exception {
    BuiltinFunctionParity.assertParity(() -> strings(legacy),
        "SELECT id, TRY_CAST('123' AS BINARY(4)), CAST(NULL AS BINARY(4)), "
            + "TRY_CAST('é中' AS BINARY(1)) FROM src WHERE id >= 0");
    for (String cast : List.of("CAST", "TRY_CAST")) {
      BuiltinFunctionParity.assertParity(() -> strings(legacy),
          "SELECT id, " + cast + "(s AS BINARY(1)), " + cast + "(s AS BINARY(4)), "
              + cast + "(s AS BINARY(16)), " + cast + "(s AS VARBINARY(4)) FROM src");
      BuiltinFunctionParity.assertParity(() -> strings(legacy),
          "SELECT TO_BASE64(" + cast + "(s AS BINARY(4))), "
              + cast + "(s AS BINARY(4)) IS NULL FROM src WHERE id >= 0");
    }
  }

  @Test
  void fixedBinaryArgumentsResultsAndTypedLiteralsRetainRawBytes() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ELT");
    BuiltinFunctionParity.assertParity(this::binary,
        "SELECT ELT(n,b,p), ELT(1,p,b), ELT(n,b,X'00FF'), "
            + "TO_BASE64(ELT(n,b,p)), STARTSWITH(b,p), ENDSWITH(b,X'00FF') FROM src");
    BuiltinFunctionParity.assertParity(this::binary,
        "SELECT ELT(n,b,p), ELT(n,b,CAST(NULL AS BINARY(2))) FROM src WHERE n = 2");
    BuiltinFunctionParity.assertParity(this::binary,
        "SELECT ELT(n,b,p) FROM src WHERE n = 99");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void binaryWidthCastsPreserveBytesAndNulls(boolean legacy) throws Exception {
    BuiltinFunctionParity.assertParity(() -> configured(binary(), legacy),
        "SELECT CAST(b AS BINARY(1)), CAST(b AS BINARY(4)), "
            + "TRY_CAST(b AS BINARY(4)), CAST(b AS VARBINARY(1)), "
            + "TRY_CAST(b AS VARBINARY(4)), TO_BASE64(b), TO_BASE64(X'00FF') FROM src");
  }

  @Test
  void binarySelectionPreservesFailingIndexEvaluation() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ELT");
    NativeFailureParity.run(this::binary, "SELECT ELT(1 / (n - n), b, p) FROM src")
        .assertFailure(ArithmeticException.class, "zero", NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @Test
  void conversionDoesNotCatchInputFailures() {
    NativeFailureParity.run(() -> strings(false),
        "SELECT TRY_CAST(CAST(1 / (id - id) AS STRING) AS BINARY(4)) FROM src")
        .assertFailure(ArithmeticException.class, "zero", NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @Test
  void unselectedInputFailuresAreSkipped() throws Exception {
    String failing = "TRY_CAST(CAST(1 / (id - id) AS STRING) AS BINARY(4))";
    BuiltinFunctionParity.assertParity(() -> strings(false),
        "SELECT CASE WHEN id >= 0 THEN TRY_CAST(s AS BINARY(4)) ELSE " + failing + " END FROM src");
    BuiltinFunctionParity.assertParity(() -> strings(false),
        "SELECT " + failing + " FROM src WHERE id < 0");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void fixedBinaryExpressionsComposeWithGroupedCounts(boolean selection) throws Exception {
    if (selection) tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ELT");
    java.util.function.Supplier<TableEnvironment> source =
        () -> {
          var table = selection ? binary() : strings(false);
          table.getConfig().set("table.exec.mini-batch.enabled", "true");
          table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
          table.getConfig().set("table.exec.mini-batch.size", "32");
          return table;
        };
    String key = selection ? "ELT(n,b,p)" : "TRY_CAST(s AS BINARY(4))";
    String sql = "SELECT " + key + ", COUNT(*) FROM src GROUP BY " + key;
    NativeParity.assertParity(source, sql);
    String plan = tech.streamfusion.planner.NativePlanner.explain(source.get(), sql);
    org.junit.jupiter.api.Assertions.assertTrue(
        plan.contains("NativeColumnarGroupAggregate"), plan);
    org.junit.jupiter.api.Assertions.assertTrue(plan.contains("NativeCalc"), plan);
  }

  private TableEnvironment binary() {
    List<Row> samples = List.of(
        Row.of(new byte[] {0, (byte) 255}, new byte[] {(byte) 128, 0}, 1),
        Row.of(new byte[] {0, (byte) 255}, new byte[] {0, (byte) 255}, 2),
        Row.of(null, new byte[] {0, 0}, 0), Row.of(new byte[] {1, 2}, null, -1),
        Row.of(new byte[] {1, 2}, null, 2), Row.of(null, null, null),
        Row.of(new byte[] {(byte) 255, (byte) 128}, new byte[] {1, 2}, 3));
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 5003; i++) rows.add(Row.copy(samples.get(i % samples.size())));
    return BuiltinFunctionParity.environment(
        ROW(FIELD("b", BINARY(2)), FIELD("p", BINARY(2)), FIELD("n", INT())), rows);
  }

  private static TableEnvironment strings(boolean legacy) {
    String[] values = {"", "1", "123", "1234", "12345678901234567", "é中😀", "a\u0000b", null};
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < values.length; i++) rows.add(Row.of(i, values[i]));
    return configured(BuiltinFunctionParity.environment(
        ROW(FIELD("id", INT()), FIELD("s", STRING())), rows), legacy);
  }

  private static TableEnvironment configured(TableEnvironment table, boolean legacy) {
    table.getConfig().getConfiguration().setString(
        "table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    return table;
  }
}

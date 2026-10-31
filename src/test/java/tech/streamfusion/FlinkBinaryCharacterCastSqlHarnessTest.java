package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkBinaryCharacterCastSqlHarnessTest {
  private static DataType type() {
    return ROW(FIELD("id", INT().notNull()), FIELD("b", BYTES()));
  }

  private static List<Row> rows(int count) {
    var rows = new ArrayList<Row>();
    byte[][] boundaries = {null, {}, {0}, {(byte) 255}, {(byte) 0xed, (byte) 0xa0, (byte) 0x80},
        {(byte) 0xf0, (byte) 0x90, (byte) 0x80}, {(byte) 0xe0, (byte) 0x80, (byte) 0x80},
        "\ud83d\ude00".getBytes(StandardCharsets.UTF_8), "\ue000".getBytes(StandardCharsets.UTF_8),
        "a\u0301\0z".getBytes(StandardCharsets.UTF_8), "  ab  ".getBytes(StandardCharsets.UTF_8)};
    var random = new Random(493121);
    for (int i = 0; i < count; i++) {
      byte[] b;
      if (i < boundaries.length) b = boundaries[i];
      else {
        b = new byte[i % 19];
        random.nextBytes(b);
      }
      rows.add(Row.of(i, b));
    }
    return rows;
  }

  private static TableEnvironment environment(boolean legacy) {
    var table = BuiltinFunctionParity.environment(type(), rows(273));
    table.getConfig().getConfiguration().setString("table.exec.legacy-cast-behaviour",
        legacy ? "ENABLED" : "DISABLED");
    return table;
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void decodedValuesNullsTypesAndFixedPaddingMatchReleasedFlink(boolean legacy) throws Exception {
    BuiltinFunctionParity.assertParity(() -> environment(legacy),
        "SELECT id, CAST(b AS STRING), TRY_CAST(b AS STRING), "
            + "CAST(CAST(b AS BINARY(8)) AS STRING), "
            + "TRY_CAST(CAST(b AS VARBINARY(4)) AS STRING) FROM src");
    BuiltinFunctionParity.assertParity(() -> environment(legacy),
        "SELECT CAST(X'EDA080' AS STRING), TRY_CAST(CAST(NULL AS BYTES) AS STRING), "
            + "CAST(b AS STRING) FROM src");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void composedConsumersKeepDecodedValuesAndJavaComparisons(boolean legacy) throws Exception {
    BuiltinFunctionParity.assertParity(() -> environment(legacy),
        "SELECT id, CHAR_LENGTH(CAST(b AS STRING)), HEX(CAST(b AS STRING)), "
            + "TO_BASE64(CAST(CAST(b AS STRING) AS BYTES)), "
            + "CAST(b AS STRING) = '?', CAST(b AS STRING) LIKE '%a%', "
            + "CONCAT(CAST(b AS STRING),'!') FROM src");
    NativeParity.assertFallback(() -> environment(legacy),
        "SELECT id, CAST(b AS STRING) < '\ue000', "
            + "GREATEST(CAST(b AS STRING),'\ue000'), "
            + "TRIM(BOTH CAST(b AS STRING) FROM '  ab  ') FROM src WHERE id BETWEEN 2 AND 10");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void lazyBranchesAndEmptyFilteredBatchesDoNotEvaluateFailingOperands(boolean legacy)
      throws Exception {
    String failed = "ENCODE(CAST(1/(id-id) AS STRING),'UTF-8')";
    BuiltinFunctionParity.assertParity(() -> environment(legacy),
        "SELECT CAST(CASE WHEN id>=0 THEN b ELSE " + failed + " END AS STRING) FROM src");
    BuiltinFunctionParity.assertParity(() -> environment(legacy),
        "SELECT TRY_CAST(" + failed + " AS STRING) FROM src WHERE id<0");
  }

  @Test
  void tryCastDoesNotCatchOperandEvaluationFailures() {
    var comparison = NativeFailureParity.run(() -> environment(false),
        "SELECT TRY_CAST(ENCODE(CAST(1/(id-id) AS STRING),'UTF-8') AS STRING) FROM src");
    assertEquals(NativeFailureParity.Route.NATIVE, comparison.nativeRun().route());
    for (var result : List.of(comparison.host(), comparison.nativeRun())) {
      assertEquals(NativeFailureParity.Phase.ROW_EVALUATION, result.phase());
      assertTrue(result.rootCause().getMessage().toLowerCase(java.util.Locale.ROOT).contains("zero"),
          result.toString());
    }
  }

  @Test
  void modernBoundedTargetsKeepUtf16TruncationWhileLegacyIgnoresLength() throws Exception {
    NativeParity.assertFallback(() -> environment(false),
        "SELECT CAST(b AS CHAR(1)), CAST(b AS VARCHAR(1)), TRY_CAST(b AS CHAR(8)) FROM src");
    BuiltinFunctionParity.assertParity(() -> environment(true),
        "SELECT CAST(b AS CHAR(1)), CAST(b AS VARCHAR(1)), TRY_CAST(b AS CHAR(8)) FROM src");
  }

  @Test
  void stringResultsDoNotLoseTheirJavaRepresentationAcrossOrderingBoundaries() throws Exception {
    NativeParity.assertFallback(() -> environment(false),
        "SELECT MIN(CAST(b AS STRING)) FROM src");
    NativeParity.assertFallback(() -> environment(false),
        "SELECT CAST(b AS STRING) FROM src UNION ALL SELECT CAST(b AS STRING) FROM src");
    // The released optimizer can delay a cast until after Top-N; then no decoded value crosses it.
    NativeParity.assertChangelogParity(() -> environment(false),
        "SELECT CAST(b AS STRING) AS s FROM src ORDER BY s LIMIT 3");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void runtimeBytesConsumeTheNativeDecoderWithoutJvmBindings(boolean fixed) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
      var table = StreamTableEnvironment.create(env);
      var input = rows(5003);
      DataType inputType = fixed ? ROW(FIELD("id", INT().notNull()), FIELD("b", BINARY(8))) : type();
      if (fixed) for (var row : input) {
        byte[] b = (byte[]) row.getField(1);
        if (b != null) row.setField(1, java.util.Arrays.copyOf(b, 8));
      }
      table.createTemporaryView("src", tech.streamfusion.compat.FlinkTestSources.fromData(env,
          input, ExternalTypeInfo.<Row>of(inputType)),
          org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(inputType).build());
      var scan = NativePlanner.install(table);
      String sql = "SELECT CAST(b AS STRING),TRY_CAST(b AS STRING) FROM src";
      assertTrue(table.explainSql(sql).contains("NativeCalc"));
      assertFalse(scan.functionExecutions().isEmpty());
      assertTrue(scan.functionExecutions().stream().allMatch(route -> route.contains("jvm=[]")),
          scan.functionExecutions().toString());
      int count = 0;
      try (var output = table.executeSql(sql).collect()) {
        while (output.hasNext()) {
          byte[] b = (byte[]) input.get(count++).getField(1);
          String expected = b == null ? null : new String(b, StandardCharsets.UTF_8);
          assertEquals(Row.of(expected, expected), output.next());
        }
      }
      assertEquals(input.size(), count);
      assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    }
  }
}

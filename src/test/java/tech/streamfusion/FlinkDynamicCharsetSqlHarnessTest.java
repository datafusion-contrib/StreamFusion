package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.compat.FlinkTestCapabilities;

class FlinkDynamicCharsetSqlHarnessTest {
  @Test
  void runtimeEncodeUsesJdkCharsetsAliasesAndNulls() throws Exception {
    String sql = "SELECT ENCODE(s,c) FROM src";
    if (FlinkTestCapabilities.VARIABLE_LENGTH_ENCODE) {
      BuiltinFunctionParity.assertParity(FlinkDynamicCharsetSqlHarnessTest::samples, sql);
    } else {
      NativeParity.assertFallbackReasonContains(
          FlinkDynamicCharsetSqlHarnessTest::samples, sql, "BINARY(1)");
    }
  }

  @Test
  void runtimeDecodeUsesJdkMalformedSequenceGroupingAcrossBatches() throws Exception {
    BuiltinFunctionParity.assertParity(
        FlinkDynamicCharsetSqlHarnessTest::samples,
        "SELECT DECODE(b,c), CHAR_LENGTH(DECODE(b,c)) FROM src");
  }

  @Test
  void decodedSurrogatesReachConsumersBeforeUtf8Conversion() throws Exception {
    BuiltinFunctionParity.assertParity(
        FlinkDynamicCharsetSqlHarnessTest::samples,
        "SELECT DECODE(b,c) = '?', DECODE(b,c) LIKE '?',"
            + " CASE WHEN DECODE(b,c) = '?' THEN 1 ELSE 0 END,"
            + " ENCODE(DECODE(b,c),'CESU-8') = b FROM src");
    BuiltinFunctionParity.assertParity(
        FlinkDynamicCharsetSqlHarnessTest::samples, "SELECT c, b FROM src WHERE DECODE(b,c) = '?'");
  }

  @Test
  void sensitiveDecodedStringsCrossingOperatorsRetainSafeFallback() throws Exception {
    NativeParity.assertFallbackReasonContains(
        FlinkDynamicCharsetSqlHarnessTest::samples,
        "SELECT DECODE(b,c), COUNT(*) FROM src GROUP BY DECODE(b,c)",
        "string");
  }

  @ParameterizedTest
  @ValueSource(strings = {"not-a-charset", "UTF@8", ""})
  void invalidRuntimeNamesKeepTheHostError(String charset) throws Exception {
    for (String expression : List.of("DECODE(b,c)", "ENCODE(s,c)")) {
      NativeFailureParity.run(() -> invalid(charset), "SELECT " + expression + " FROM src")
          .assertFailure(
              UnsupportedEncodingException.class,
              charset,
              NativeFailureParity.Phase.ROW_EVALUATION,
              expression.startsWith("ENCODE") && !FlinkTestCapabilities.VARIABLE_LENGTH_ENCODE
                  ? NativeFailureParity.Route.FALLBACK
                  : NativeFailureParity.Route.NATIVE);
    }
  }

  @Test
  void nullInputsAndUntakenBranchesDoNotResolveInvalidCharsets() throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> invalid("not-a-charset"),
        "SELECT DECODE(CASE WHEN n = 1 THEN CAST(NULL AS BYTES) ELSE b END,c),"
            + " CASE WHEN n = 1 THEN 'ok' ELSE DECODE(b,c) END FROM src");
    if (FlinkTestCapabilities.VARIABLE_LENGTH_ENCODE) {
      BuiltinFunctionParity.assertParity(
          () -> invalid("not-a-charset"),
          "SELECT ENCODE(CASE WHEN n = 1 THEN CAST(NULL AS STRING) ELSE s END,c),"
              + " CASE WHEN n = 1 THEN CAST(NULL AS BYTES) ELSE ENCODE(s,c) END FROM src");
    }
  }

  @Test
  void emptyFilteredBatchesDoNotResolveInvalidCharsets() throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> invalid("not-a-charset"), "SELECT DECODE(b,c) FROM src WHERE n = 99");
    if (FlinkTestCapabilities.VARIABLE_LENGTH_ENCODE) {
      BuiltinFunctionParity.assertParity(
          () -> invalid("not-a-charset"), "SELECT ENCODE(s,c) FROM src WHERE n = 99");
    }
  }

  @Test
  void filtersRemoveInvalidCharsetsBeforeEvaluatingSurvivingRows() throws Exception {
    java.util.function.Supplier<TableEnvironment> input =
        () ->
            environment(
                List.of(
                    Row.of(1, "abc", new byte[] {65}, "not-a-charset"),
                    Row.of(2, "abc", new byte[] {65}, "UTF-8")));
    BuiltinFunctionParity.assertParity(input, "SELECT DECODE(b,c) FROM src WHERE n = 2");
    if (FlinkTestCapabilities.VARIABLE_LENGTH_ENCODE) {
      BuiltinFunctionParity.assertParity(input, "SELECT ENCODE(s,c) FROM src WHERE n = 2");
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT DECODE(b,c1), DECODE(b,c2) FROM src",
        "SELECT DECODE(b,c2) FROM src WHERE DECODE(b,c1) <> ''"
      })
  void charsetFailuresFollowFlinkRowAndProjectionOrder(String sql) {
    NativeFailureParity.run(
            () ->
                BuiltinFunctionParity.environment(
                    ROW(FIELD("b", BYTES()), FIELD("c1", STRING()), FIELD("c2", STRING())),
                    List.of(
                        Row.of(new byte[] {65}, "UTF-8", "invalid-first-row"),
                        Row.of(new byte[] {66}, "invalid-second-row", "UTF-8"))),
            sql)
        .assertFailure(
            UnsupportedEncodingException.class,
            "invalid-first-row",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  private static TableEnvironment invalid(String charset) {
    return environment(List.of(Row.of(1, "abc", new byte[] {65}, charset)));
  }

  private static TableEnvironment samples() {
    List<Row> rows = new ArrayList<>();
    String[] charsets = {
      "UTF-8",
      "utf8",
      "ASCII",
      "latin1",
      "UTF-16",
      "UTF-16BE",
      "UnicodeLittleUnmarked",
      "windows-1252",
      "Shift_JIS",
      "GB18030",
      "UTF-32",
      "CESU-8",
      null
    };
    byte[][] bytes = {
      null,
      {},
      {65, 0, 66},
      {(byte) 0xc0, (byte) 0xaf},
      {(byte) 0xed, (byte) 0xa0, (byte) 0x80},
      {(byte) 0xed, (byte) 0xb0, (byte) 0x80},
      {63},
      {(byte) 0xfe, (byte) 0xff, 0, 65},
      {(byte) 0xd8, 0, 0, 65},
      {(byte) 0xf0, (byte) 0x9f, (byte) 0x98, (byte) 0x80},
      {(byte) 0x80, (byte) 0xff}
    };
    String[] strings = {null, "", "abc\u0000", "\u00e9\u20ac\u4e2d\ud83d\ude00"};
    for (int i = 0; i < 5003; i++) {
      rows.add(
          Row.of(
              i,
              strings[i % strings.length],
              bytes[i % bytes.length],
              charsets[i % charsets.length]));
    }
    return environment(rows);
  }

  private static TableEnvironment environment(List<Row> rows) {
    return BuiltinFunctionParity.environment(
        ROW(FIELD("n", INT()), FIELD("s", STRING()), FIELD("b", BYTES()), FIELD("c", STRING())),
        rows);
  }
}

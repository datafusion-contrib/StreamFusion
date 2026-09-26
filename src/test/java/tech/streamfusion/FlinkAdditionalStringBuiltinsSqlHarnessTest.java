package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkAdditionalStringBuiltinsSqlHarnessTest {
  @Test
  void classificationNumericSeparatorsAndDynamicHashes() throws Exception {
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT IS_DECIMAL(s), IS_DIGIT(s), IS_ALPHA(s), SPLIT_INDEX(s,sep,n), SHA2(s,bits) FROM src");
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT SPLIT_INDEX(s,sep,n) FROM src WHERE IS_ALPHA(s) OR IS_DIGIT(s)");
  }

  @Test
  void base64StringAndBinaryInputsPreserveInvalidEncodingAndNulls() throws Exception {
    NativeParity.assertFallbackReasonContains(this::base64Environment,
        "SELECT FROM_BASE64(s), FROM_BASE64(b), FROM_BASE64(s) = '?', "
            + "CHAR_LENGTH(FROM_BASE64(s)), TO_BASE64(FROM_BASE64(s)), "
            + "CAST(FROM_BASE64(s) AS BYTES) FROM src",
        "binary-backed STRING requires a final scalar projection");
    NativeParity.assertFallbackReasonContains(this::base64Environment,
        "SELECT FROM_BASE64(s), SHA2(s,bits) FROM src WHERE n = 99",
        "binary-backed STRING requires a final scalar projection");
  }

  @Test
  void decodedBytesReachTheRowSinkWithoutUtf8Normalization() throws Exception {
    for (boolean nativeEnabled : new boolean[] {false, true}) {
      var table = (org.apache.flink.table.api.bridge.java.StreamTableEnvironment) base64Environment();
      var scan = nativeEnabled ? tech.streamfusion.planner.NativePlanner.install(table) : null;
      var query = table.sqlQuery("SELECT FROM_BASE64(s) AS v FROM src");
      var output = table.<org.apache.flink.table.data.RowData>toDataStream(
          query, ROW(FIELD("v", STRING())).bridgedTo(org.apache.flink.table.data.RowData.class));
      List<String> actual = new java.util.ArrayList<>();
      try (var rows = output.map(row -> row.isNullAt(0) ? "NULL"
              : java.util.Base64.getEncoder().encodeToString(row.getString(0).toBytes()))
          .returns(org.apache.flink.api.common.typeinfo.Types.STRING).executeAndCollect()) {
        rows.forEachRemaining(actual::add);
      }
      actual.sort(String::compareTo);
      org.junit.jupiter.api.Assertions.assertEquals(
          List.of("", "/w==", "7aCA", "AA==", "NULL", "aGVsbG8="), actual);
      if (nativeEnabled) org.junit.jupiter.api.Assertions.assertTrue(scan.substitutions() > 0,
          scan.fallbackReasons().toString());
    }
  }

  @Test
  void invalidBase64RetainsTheHostFailure() throws Exception {
    NativeFailureParity.run(this::environment, "SELECT FROM_BASE64(s) FROM src")
        .assertFailure(IllegalArgumentException.class, "Illegal base64 character",
            NativeFailureParity.Phase.ROW_EVALUATION, NativeFailureParity.Route.NATIVE);
  }

  @Test
  void unsupportedHashWidthsRetainFailuresAndShortCircuiting() throws Exception {
    BuiltinFunctionParity.assertParity(this::invalidHashEnvironment,
        "SELECT n = 0 OR SHA2(s,bits) = 'x', n <> 0 AND SHA2(s,bits) = 'x' FROM src");
    NativeFailureParity.run(this::invalidHashEnvironment, "SELECT SHA2(s,bits) FROM src")
        .assertFailure(java.security.NoSuchAlgorithmException.class, "SHA-128", NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  private TableEnvironment invalidHashEnvironment() {
    return BuiltinFunctionParity.environment(ROW(FIELD("s", STRING()), FIELD("bits", INT()),
        FIELD("n", INT())), List.of(Row.of("test", 128, 0)));
  }

  private TableEnvironment base64Environment() {
    List<Row> rows = new java.util.ArrayList<>();
    for (String value : new String[] {"aGVsbG8=", "/w==", "7aCA", "AA==", "", null}) {
      rows.add(Row.of(value, value == null ? null : value.getBytes(StandardCharsets.US_ASCII), 0, 256));
    }
    return BuiltinFunctionParity.environment(
        ROW(FIELD("s", STRING()), FIELD("b", BYTES()), FIELD("n", INT()), FIELD("bits", INT())), rows);
  }

  private TableEnvironment environment() {
    return BuiltinFunctionParity.environment(
        ROW(FIELD("s", STRING()), FIELD("b", BYTES()), FIELD("sep", INT()), FIELD("n", INT()),
            FIELD("bits", INT())),
        List.of(Row.of("aGVsbG8=", "aGVsbG8=".getBytes(StandardCharsets.US_ASCII), 73, 0, 224),
            Row.of("/w==", "/w==".getBytes(StandardCharsets.US_ASCII), 256, 1, 256),
            Row.of("!invalid!", new byte[] {(byte) 255, 0}, 0, -1, 384),
            Row.of("123", new byte[0], 49, 0, 512), Row.of("-12.50", null, 46, 1, 256),
            Row.of("abc", null, 98, 0, 256), Row.of("\u0661\u0662", null, -1, 0, 256),
            Row.of("\u4e2d", null, 44, 0, 256), Row.of("", null, 44, 0, null),
            Row.of(null, null, null, null, 256)));
  }
}

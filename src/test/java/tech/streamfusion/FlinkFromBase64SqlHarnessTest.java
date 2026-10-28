package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkFromBase64SqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"STRING", "BYTES", "BINARY(8)", "CHAR(8)"})
  void consumedRawBytesUseNativeDecoding(String inputType) throws Exception {
    var rows = new ArrayList<Row>();
    var expected = new ArrayList<String>();
    for (int i = 0; i < 2053; i++) {
      int length = inputType.endsWith("(8)") ? 6 : i % 258;
      byte[] bytes = new byte[length];
      for (int j = 0; j < length; j++) bytes[j] = (byte) (i * 131 + j * 127);
      String encoded = i % 7 == 0 ? null : Base64.getEncoder().encodeToString(bytes);
      expected.add(encoded == null ? "NULL" : encoded);
      rows.add(
          Row.of(
              encoded == null || inputType.startsWith("CHAR") || inputType.equals("STRING")
                  ? encoded
                  : encoded.getBytes(StandardCharsets.US_ASCII)));
    }
    try (var metrics = new RecoveryExecutionMetrics()) {
      var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
      var table = StreamTableEnvironment.create(env);
      var valueType =
          inputType.equals("STRING")
              ? STRING()
              : inputType.equals("BYTES")
                  ? BYTES()
                  : inputType.startsWith("CHAR") ? CHAR(8) : BINARY(8);
      var type = ROW(FIELD("v", valueType));
      table.createTemporaryView(
          "src",
          tech.streamfusion.compat.FlinkTestSources.fromData(
              env, rows, ExternalTypeInfo.<Row>of(type)),
          Schema.newBuilder().fromRowDataType(type).build());
      var scan = NativePlanner.install(table);
      var query = table.sqlQuery("SELECT FROM_BASE64(v) AS decoded FROM src");
      var output =
          table.<RowData>toDataStream(
              query, ROW(FIELD("decoded", STRING())).bridgedTo(RowData.class));
      List<String> actual = new ArrayList<>();
      try (var results =
          output
              .map(
                  row ->
                      row.isNullAt(0)
                          ? "NULL"
                          : Base64.getEncoder().encodeToString(row.getString(0).toBytes()))
              .returns(org.apache.flink.api.common.typeinfo.Types.STRING)
              .executeAndCollect()) {
        results.forEachRemaining(actual::add);
      }
      assertEquals(expected, actual);
      assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
      assertTrue(
          scan.functionExecutions().stream()
              .anyMatch(route -> route.contains("FROM_BASE64") && route.contains("jvm=[]")),
          scan.functionExecutions().toString());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    }
  }

  @Test
  void missingPaddingAndUnusedBitsRetainReleasedResults() throws Exception {
    BuiltinFunctionParity.assertParity(
        () ->
            source(
                List.of("", "YR==", "YR", "YWL=", "YWL", "YWJj", "/w==", "7aCA", "AA==", "NULL")),
        "SELECT FROM_BASE64(v) FROM src");
    BuiltinFunctionParity.assertParity(
        () -> source(List.of("YQ==", "YWI=", "YWJj", "NULL")),
        "SELECT FROM_BASE64(CAST(v AS BYTES)), FROM_BASE64(CONCAT(v,'')) FROM src");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"A", "!A", "Y WJj", "YQ-_", "YQ=", "====", "A=", "AAAAA", "YQ===", "YQ==Z"})
  void invalidInputsKeepReleasedExceptionTypesAndMessages(String value) throws Exception {
    String message;
    try {
      Base64.getDecoder().decode(value);
      throw new AssertionError("Expected invalid Base64");
    } catch (IllegalArgumentException expected) {
      message = expected.getMessage();
    }
    NativeFailureParity.run(() -> source(List.of(value)), "SELECT FROM_BASE64(v) FROM src")
        .assertFailure(
            IllegalArgumentException.class,
            message,
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @Test
  void invalidBinaryBytesKeepSignedHexadecimalDiagnostics() throws Exception {
    NativeFailureParity.run(
            () ->
                BuiltinFunctionParity.environment(
                    ROW(FIELD("v", BYTES())), List.of(Row.of(new byte[] {(byte) 255, 'A'}))),
            "SELECT FROM_BASE64(v) FROM src")
        .assertFailure(
            IllegalArgumentException.class,
            "Illegal base64 character -1",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @Test
  void binaryBackedCharacterInputRetainsItsOriginalInvalidByte() throws Exception {
    NativeFailureParity.run(
            () -> {
              var env =
                  org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
                      .getExecutionEnvironment();
              env.setParallelism(1);
              var table = StreamTableEnvironment.create(env);
              var type = ROW(FIELD("v", STRING()));
              List<RowData> rows =
                  List.of(
                      org.apache.flink.table.data.GenericRowData.of(
                          org.apache.flink.table.data.binary.BinaryStringData.fromBytes(
                              new byte[] {(byte) 255, 'A'})));
              table.createTemporaryView(
                  "src",
                  tech.streamfusion.compat.FlinkTestSources.fromData(
                      env,
                      rows,
                      org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(
                          (org.apache.flink.table.types.logical.RowType) type.getLogicalType())),
                  Schema.newBuilder().fromRowDataType(type).build());
              return table;
            },
            "SELECT FROM_BASE64(v) FROM src")
        .assertFailure(
            IllegalArgumentException.class,
            "Illegal base64 character -1",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void fixedBinaryCastsKeepConfiguredLengths(boolean legacy) throws Exception {
    java.util.function.Supplier<TableEnvironment> input =
        () -> {
          var table = source(List.of("YQ==", "YWI=", "YWJj", "NULL"));
          table
              .getConfig()
              .set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
          return table;
        };
    BuiltinFunctionParity.assertParity(
        input,
        "SELECT FROM_BASE64(CAST(v AS BINARY(4))), FROM_BASE64(TRY_CAST(v AS BINARY(4))) FROM src");
    if (legacy) {
      BuiltinFunctionParity.assertParity(
          input, "SELECT FROM_BASE64(CAST(v AS BINARY(8))) FROM src");
    } else {
      NativeFailureParity.run(input, "SELECT FROM_BASE64(CAST(v AS BINARY(8))) FROM src")
          .assertFailure(
              IllegalArgumentException.class,
              "incorrect ending byte at 4",
              NativeFailureParity.Phase.ROW_EVALUATION,
              NativeFailureParity.Route.NATIVE);
    }
  }

  @Test
  void allFilteredBatchesAndUnselectedBranchesDoNotDecode() throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> source(List.of("!A", "A", "NULL")), "SELECT FROM_BASE64(v) FROM src WHERE v='never'");
    BuiltinFunctionParity.assertParity(
        () -> source(List.of("!A", "A", "NULL")),
        "SELECT CASE WHEN v='never' THEN FROM_BASE64(v) ELSE 'ok' END FROM src");
    BuiltinFunctionParity.assertParity(
        () -> source(List.of("!A", "A", "NULL")),
        "SELECT v='never' AND FROM_BASE64(v)='a', v<>'never' OR FROM_BASE64(v)='a' FROM src");
  }

  @Test
  void sensitiveConsumersRetainTheExactGeneratedRoute() throws Exception {
    String sql =
        "SELECT FROM_BASE64(v)='?', CHAR_LENGTH(FROM_BASE64(v)),"
            + " TO_BASE64(FROM_BASE64(v)), CAST(FROM_BASE64(v) AS BYTES) FROM src";
    var table = source(List.of("/w==", "7aCA", "AA==", "YQ==", "", "NULL"));
    var scan = NativePlanner.install(table);
    table.explainSql(sql);
    assertTrue(
        scan.functionExecutions().stream()
            .anyMatch(route -> route.contains("jvm=[") && !route.contains("jvm=[]")),
        scan.functionExecutions().toString());
    BuiltinFunctionParity.assertParity(
        () -> source(List.of("/w==", "7aCA", "AA==", "YQ==", "", "NULL")), sql);
  }

  private static TableEnvironment source(List<String> values) {
    var rows = new ArrayList<Row>();
    for (String value : values) rows.add(Row.of(value.equals("NULL") ? null : value));
    return BuiltinFunctionParity.environment(ROW(FIELD("v", STRING())), rows);
  }
}

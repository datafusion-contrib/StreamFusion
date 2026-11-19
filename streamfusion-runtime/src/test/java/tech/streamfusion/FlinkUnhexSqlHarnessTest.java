package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkUnhexSqlHarnessTest {
  @org.junit.jupiter.api.BeforeEach
  void requireReleasedHostFunction() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("UNHEX");
  }

  @Test
  void oddLengthsAndInvalidInputNulls() throws Exception {
    parity("SELECT id, UNHEX(hex_text) FROM encodings");
  }

  @Test
  void literalsNullsAndNonNullableArguments() throws Exception {
    parity(
        "SELECT id, UNHEX('ABC'), UNHEX(CAST(NULL AS STRING)), UNHEX(COALESCE(hex_text, '')) FROM"
            + " encodings");
  }

  @Test
  void unhexRejectsInvalidTailsAfterDecodingValidPrefixes() throws Exception {
    NativeParity.assertParity(
        () -> {
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
          tEnv.createTemporaryView(
              "unhex_inputs",
              fromData(
                  env,
                  Types.ROW_NAMED(new String[] {"s"}, Types.STRING),
                  Row.of("aF0012GG"),
                  Row.of("Af"),
                  Row.of("A12G4"),
                  Row.of("ABC"),
                  Row.of((Object) null),
                  Row.of("0011\uff11\uff12"),
                  Row.of("00\u0000f"),
                  Row.of(""),
                  Row.of("aF".repeat(4097) + "0G"),
                  Row.of("1234")));
          return tEnv;
        },
        "SELECT UNHEX(s) FROM unhex_inputs");
  }

  @Test
  void foldedBinaryConstantsStayNative() throws Exception {
    NativeParity.assertParity(
        StringFunctionTestInputs::encodings,
        "SELECT id, UNHEX(''), UNHEX('FF00'), UNHEX('ABC'),"
            + " UNHEX(hex_text) = UNHEX('aF00') FROM encodings"
            + " WHERE UNHEX(hex_text) IS NOT NULL");
  }

  private static void parity(String sql) throws Exception {
    NativeParity.assertParity(StringFunctionTestInputs::encodings, sql);
  }
}

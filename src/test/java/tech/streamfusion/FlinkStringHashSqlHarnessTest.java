package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkStringHashSqlHarnessTest {

  @Test
  void concatPropagatesNullsAndPreservesEmptyStrings() throws Exception {
    NativeParity.assertParity(
        FlinkStringHashSqlHarnessTest::environment,
        "SELECT id, CONCAT(s, other), CONCAT(s, ':', other, '!'), s || other,"
            + " CONCAT(s), CONCAT(s, CAST(NULL AS STRING)) FROM strings");
  }

  @Test
  void concatWsSkipsNullValuesButPreservesEmptyStrings() throws Exception {
    NativeParity.assertParity(
        FlinkStringHashSqlHarnessTest::environment,
        "SELECT id, CONCAT_WS(sep, s, other), CONCAT_WS(':', s, other, ''),"
            + " CONCAT_WS('', s, other), CONCAT_WS(sep, s),"
            + " CONCAT_WS(sep),"
            + " CONCAT_WS(sep, CAST(NULL AS STRING), CAST(NULL AS STRING)) FROM strings");
  }

  @Test
  void hashesMatchHostForNullEmptyUnicodeAndLongInputs() throws Exception {
    NativeParity.assertParity(
        FlinkStringHashSqlHarnessTest::environment,
        "SELECT id, MD5(s), SHA224(s), SHA256(s), SHA384(s), SHA512(s),"
            + " SHA2(s, 224), SHA2(s, 256), SHA2(s, 384), SHA2(s, 512) FROM strings");
  }

  @Test
  void nestedFunctionsAndFiltersMatchHost() throws Exception {
    NativeParity.assertParity(
        FlinkStringHashSqlHarnessTest::environment,
        "SELECT id, SHA256(CONCAT(s, other)), CONCAT_WS(sep, MD5(s), SHA2(other, 512)),"
            + " CONCAT(MD5(s), SHA224(other)) FROM strings"
            + " WHERE MD5(CONCAT_WS('', s, other)) <> MD5('abc')");
  }

  @Test
  void dynamicSha2BitLengthMatchesHost() throws Exception {
    BuiltinFunctionParity.assertParity(
        FlinkStringHashSqlHarnessTest::environment,
        "SELECT SHA2(s, bits) FROM strings");
  }

  @ParameterizedTest
  @ValueSource(
      longs = {
        4294967520L, 4294967552L, 4294967680L, 4294967808L,
        -4294967072L, -4294967040L, Long.MIN_VALUE, Long.MAX_VALUE
      })
  void oversizedSha2BitLengthFallsBack(long bits) {
    // Flink rejects these algorithms; narrowing to int must not admit their low 32 bits.
    String plan =
        NativePlanner.explain(
            environment(), "SELECT SHA2(s, CAST(" + bits + " AS BIGINT)) FROM strings");
    assertFalse(plan.contains("NativeCalc"), plan);
    assertTrue(plan.contains("literal bit length"), plan);
  }

  @Test
  void validBigintSha2BitLengthsMatchHost() throws Exception {
    NativeParity.assertParity(
        FlinkStringHashSqlHarnessTest::environment,
        "SELECT SHA2(s, CAST(224 AS BIGINT)), SHA2(s, CAST(256 AS BIGINT)),"
            + " SHA2(s, CAST(384 AS BIGINT)), SHA2(s, CAST(512 AS BIGINT)) FROM strings");
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.createTemporaryView(
        "strings",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "s", "other", "sep", "bits"},
                Types.INT,
                Types.STRING,
                Types.STRING,
                Types.STRING,
                Types.INT),
            Row.of(0, "abc", "", ":", 224),
            Row.of(1, "", "abc", "", 256),
            Row.of(2, null, "abc", ":", 384),
            Row.of(3, "abc", null, ":", 512),
            Row.of(4, null, null, ":", 256),
            Row.of(5, "", "", ":", 256),
            Row.of(6, "abc", "def", null, 256),
            Row.of(7, "Gr\u00fc\u00dfe \u4e2d\u6587", "\ud83d\ude00", "\u2603", 384),
            Row.of(8, "a\u0000b", "\n\t", "|", 512),
            Row.of(9, "\u00e9".repeat(4097), "tail", ":", 224)),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("s", DataTypes.STRING())
            .column("other", DataTypes.STRING())
            .column("sep", DataTypes.STRING())
            .column("bits", DataTypes.INT())
            .build());
    return tEnv;
  }
}

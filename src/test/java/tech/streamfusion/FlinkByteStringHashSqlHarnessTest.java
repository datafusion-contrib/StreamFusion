package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static tech.streamfusion.FlinkByteStringConcatSqlHarnessTest.run;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkByteStringHashSqlHarnessTest {
  @ParameterizedTest
  @MethodSource("queries")
  void hashesUseRawBytesAndReturnValidHex(String sql) throws Exception {
    assertEquals(run(sql, false), run(sql, true));
  }

  @ParameterizedTest
  @MethodSource("sha2Queries")
  void literalSha2UsesTheMatchingByteDigest(String sql) throws Exception {
    assertEquals(run(sql, false), run(sql, true));
  }

  static Stream<String> sha2Queries() {
    return Stream.of(224, 256, 384, 512)
        .flatMap(
            bits ->
                Stream.of(
                    "SELECT SHA2(s, " + bits + ") AS v FROM src",
                    "SELECT SHA2(CONCAT(s, t), " + bits + ") AS v FROM src",
                    "SELECT SHA2(CONCAT_WS(s, t, '!'), " + bits + ") AS v FROM src",
                    "SELECT CONCAT(SHA2(s, "
                        + bits
                        + "), '!') AS v FROM src WHERE SHA2(s, "
                        + bits
                        + ") LIKE 'a%'",
                    "SELECT SHA2(s, " + bits + ") AS v FROM src WHERE id = -1"));
  }

  static Stream<String> queries() {
    return Stream.of("MD5", "SHA1", "SHA224", "SHA256", "SHA384", "SHA512")
        .flatMap(
            function ->
                Stream.of(
                    "SELECT " + function + "(s) AS v FROM src",
                    "SELECT " + function + "(CONCAT(s, t)) AS v FROM src",
                    "SELECT CONCAT("
                        + function
                        + "(s), '!') AS v FROM src WHERE "
                        + function
                        + "(s) LIKE 'a%'",
                    "SELECT " + function + "(CONCAT_WS(s, t, '!')) AS v FROM src",
                    "SELECT " + function + "(s) AS v FROM src WHERE id = -1"));
  }
}

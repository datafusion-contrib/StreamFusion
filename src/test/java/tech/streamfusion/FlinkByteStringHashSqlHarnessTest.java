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

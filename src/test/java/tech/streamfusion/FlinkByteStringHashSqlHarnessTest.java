package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static tech.streamfusion.FlinkByteStringConcatSqlHarnessTest.run;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkByteStringHashSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT MD5(s) AS v FROM src",
        "SELECT MD5(CONCAT(s, t)) AS v FROM src",
        "SELECT CONCAT(MD5(s), '!') AS v FROM src WHERE MD5(s) LIKE 'a%'",
        "SELECT MD5(CONCAT_WS(s, t, '!')) AS v FROM src",
        "SELECT MD5(s) AS v FROM src WHERE id = -1"
      })
  void md5UsesRawBytesAndReturnsValidHex(String sql) throws Exception {
    assertEquals(run(sql, false), run(sql, true));
  }
}

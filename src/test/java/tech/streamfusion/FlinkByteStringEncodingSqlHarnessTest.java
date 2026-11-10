package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static tech.streamfusion.FlinkByteStringConcatSqlHarnessTest.run;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkByteStringEncodingSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT TO_BASE64(s) AS v FROM src",
        "SELECT TO_BASE64(CONCAT(s, t)) AS v FROM src",
        "SELECT TO_BASE64(CONCAT_WS(s, t, '!')) AS v FROM src",
        "SELECT CONCAT(TO_BASE64(s), '!') AS v FROM src WHERE TO_BASE64(s) LIKE '/%'",
        "SELECT MD5(TO_BASE64(s)) AS v FROM src",
        "SELECT TO_BASE64(MD5(s)) AS v FROM src",
        "SELECT TO_BASE64(s) AS v FROM src WHERE id = -1"
      })
  void base64EncodesTheOriginalBytes(String sql) throws Exception {
    assertEquals(run(sql, false), run(sql, true));
  }
}

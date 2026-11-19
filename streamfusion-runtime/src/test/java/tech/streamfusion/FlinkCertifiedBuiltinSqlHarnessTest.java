package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkCertifiedBuiltinSqlHarnessTest {
  @Test
  void regexCertificatesIncludeEmptyMatchesAndJavaLiteralReplacement() throws Exception {
    BuiltinFunctionParity.assertParity(
        this::patterns,
        "SELECT REGEXP(s,p), REGEXP_REPLACE(s,p,r), REGEXP_EXTRACT(s,p,0), "
            + "REGEXP_EXTRACT(s,p,CAST(4294967296 AS BIGINT)) FROM src");
  }

  @Test
  void regexPositionsAndCountsPreserveJavaEmptyMatchProgression() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("REGEXP_COUNT");
    BuiltinFunctionParity.assertParity(
        this::patterns, "SELECT REGEXP_COUNT(s,p), REGEXP_INSTR(s,p), REGEXP_SUBSTR(s,p) FROM src");
  }

  @Test
  void likeEscapeAndSimilarPreserveNewlinesAndDynamicPatterns() throws Exception {
    BuiltinFunctionParity.assertParity(
        () ->
            BuiltinFunctionParity.environment(
                ROW(FIELD("s", STRING()), FIELD("p", STRING()), FIELD("e", STRING())),
                Arrays.asList(
                    Row.of("a_b", "a!_b", "!"),
                    Row.of("a\nb", "a_b", "!"),
                    Row.of("a\rb", "a%b", "!"),
                    Row.of("abc", "a(b|d)c", "!"),
                    Row.of("ac", "a.c", "!"),
                    Row.of(null, "%", "!"))),
        "SELECT s LIKE p ESCAPE e, s NOT LIKE p ESCAPE e, s SIMILAR TO p, s NOT SIMILAR TO p FROM"
            + " src");
  }

  @Test
  void rawUrlPartsAndLiteralQueryKeysMatchJavaUrl() throws Exception {
    BuiltinFunctionParity.assertParity(
        () ->
            BuiltinFunctionParity.environment(
                ROW(FIELD("s", STRING()), FIELD("p", STRING()), FIELD("k", STRING())),
                Arrays.asList(
                    Row.of("http://user:pw@EXAMPLE.com:80/a/../b?a=b=c&a.b=x+y", "QUERY", "a=b"),
                    Row.of("https://host?", "PATH", "a.b"),
                    Row.of("http://[::1]/x", "HOST", "x"),
                    Row.of("not a URL", "HOST", "x"),
                    Row.of(null, null, null))),
        "SELECT PARSE_URL(s,p), PARSE_URL(s,'FILE'), PARSE_URL(s,'QUERY',k) FROM src");
  }

  @Test
  void printfDecimalFormattingWidthIndicesAndNullsMatchFormatter() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("PRINTF");
    BuiltinFunctionParity.assertParity(
        () ->
            BuiltinFunctionParity.environment(
                ROW(FIELD("n", BIGINT()), FIELD("d", DECIMAL(20, 3)), FIELD("s", STRING())),
                Arrays.asList(
                    Row.of(42L, new BigDecimal("12.345"), "text"),
                    Row.of(Long.MIN_VALUE, new BigDecimal("-0.001"), "a"),
                    Row.of(null, null, null))),
        "SELECT PRINTF('%1$+020d|%2$.2f|%3$-8.3s',n,d,s), PRINTF('%x',n), "
            + "PRINTF('%s',d), PRINTF('%+08.2f',d), PRINTF('%+s',s) FROM src");
  }

  @Test
  void printfUppercaseCharacterAndNullLiteralMatchFormatter() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("PRINTF");
    BuiltinFunctionParity.assertParity(
        () ->
            BuiltinFunctionParity.environment(
                ROW(FIELD("i", INT())),
                Arrays.asList(Row.of(97), Row.of(122), Row.of((Object) null))),
        "SELECT PRINTF('%c|%C',i,i), PRINTF('%c',CAST(i AS BIGINT)), "
            + "PRINTF('%.2147483648s',i) FROM src");
  }

  @Test
  void similarDoubledRegexEscapeRetainsFlinkTranslation() throws Exception {
    BuiltinFunctionParity.assertParity(
        () ->
            BuiltinFunctionParity.environment(
                ROW(FIELD("s", STRING()), FIELD("p", STRING()), FIELD("e", STRING())),
                Arrays.asList(
                    Row.of("x", "..", "."),
                    Row.of(".", "..", "."),
                    Row.of("", "$$", "$"),
                    Row.of("$", "$$", "$"),
                    Row.of("aa", "a++a", "!"))),
        "SELECT s SIMILAR TO p ESCAPE e FROM src");
  }

  @Test
  void literalLikeEscapeRetainsFlinkCodeGenerationSpecialization() throws Exception {
    var comparison =
        NativeFailureParity.run(
            () ->
                BuiltinFunctionParity.environment(
                    ROW(FIELD("s", STRING())),
                    Arrays.asList(Row.of("abc"), Row.of("abcd"), Row.of((Object) null))),
            "SELECT s LIKE 'abc' ESCAPE '', s LIKE 'ab%' ESCAPE '', s LIKE 'a!_b' ESCAPE '!' FROM"
                + " src");
    if (comparison.host().failure() == null) {
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    } else {
      org.junit.jupiter.api.Assertions.assertEquals(
          "Invalid escape character ''", comparison.host().rootCause().getMessage());
      org.junit.jupiter.api.Assertions.assertNotNull(comparison.nativeRun().failure());
      org.junit.jupiter.api.Assertions.assertEquals(
          comparison.host().rootCause().getClass(), comparison.nativeRun().rootCause().getClass());
      org.junit.jupiter.api.Assertions.assertEquals(
          comparison.host().rootCause().getMessage(),
          comparison.nativeRun().rootCause().getMessage());
    }
  }

  private TableEnvironment patterns() {
    return BuiltinFunctionParity.environment(
        ROW(FIELD("s", STRING()), FIELD("p", STRING()), FIELD("r", STRING())),
        Arrays.asList(
            Row.of("aa", "a*", "$1"),
            Row.of("a\rb", ".", "X"),
            Row.of("abc", "", "X"),
            Row.of("ab", "a(?=b)", "X"),
            Row.of("aab", "(a)\\1", "X"),
            Row.of("abc", "[", "X"),
            Row.of("\ud800", ".", "X"),
            Row.of("\ud800", "\\p{Cs}", "X"),
            Row.of(null, null, null)));
  }
}

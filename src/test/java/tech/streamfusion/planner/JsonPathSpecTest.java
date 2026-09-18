package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.nio.charset.Charset;
import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;
import org.junit.jupiter.api.Test;

class JsonPathSpecTest {
  @Test
  void onlyDefiniteMemberAndSignedIndexPathsAreAdmitted() {
    for (String path :
        List.of(
            "$",
            "$.a.b[0]",
            "$['a b'][2147483647]",
            "$[01].a",
            "$[-1]",
            "$[-0]",
            "$[-0001].a",
            "$[-2147483648]",
            "$.\u7528\u6237['\u59d3.\u540d']",
            "$[\"O'Reilly\"]",
            "$['a\"b']",
            "$['']",
            "$[\"\"][''].a[0]",
            "$['\ud83d\ude00']")) {
      assertEquals("strict " + path, JsonPathSpec.normalize(path));
    }
    assertEquals("lax $.a", JsonPathSpec.normalize(" \tLaX $.a"));
    assertEquals("strict $.a", JsonPathSpec.normalize("$.a "));
    for (String path :
        List.of(
            "",
            "a",
            "$.*",
            "$..a",
            "$[-2147483649]",
            "$[--1]",
            "$[-]",
            "$[2147483648]",
            "$[]",
            "$[\"a\",\"b\"]",
            "$['\ud800']",
            "$['a\n']",
            "$[?(@.a)]")) {
      assertNull(JsonPathSpec.normalize(path), path);
    }
  }

  @Test
  void literalDotNamesPreservePunctuationAndControls() {
    assertEquals("strict $[\"order-id\"][\"123\"]", JsonPathSpec.normalize("$.order-id.123"));
    assertEquals("strict $[\"a\\tb\"]", JsonPathSpec.normalize("$.a\tb"));
    assertEquals("strict $[\"a\\t\"]", JsonPathSpec.normalize("$.a\t"));
    assertEquals("strict $[\"a*b\"]", JsonPathSpec.normalize("$.a*b"));
    assertEquals("strict $[\"a\\\\u0061\"]", JsonPathSpec.normalize("$.a\\u0061"));
    assertEquals("strict $[\"😀\"]", JsonPathSpec.normalize("$.😀"));
    for (String path :
        List.of("$.*", "$.*name", "$..name", "$.name()", "$.a b", "$.a.", "$.a[", "$.\ud800")) {
      assertNull(JsonPathSpec.normalize(path), path);
    }
  }

  @Test
  void verifiedEscapesUseCanonicalJsonNamesWithoutLosingIdentity() {
    assertEquals("strict $[\"a\\\\b\"]", JsonPathSpec.normalize("$['a\\\\b']"));
    assertEquals("strict $[\"a'b\"]", JsonPathSpec.normalize("$['a\\'b']"));
    assertEquals("strict $[\"a\\\"b\"]", JsonPathSpec.normalize("$[\"a\\\"b\"]"));
    assertEquals("strict $[\"a\\bb\"]", JsonPathSpec.normalize("$['a\\bb']"));
    assertEquals("lax $[\"a/b\"][0]", JsonPathSpec.normalize("lax $[ 'a\\/b' ][ 0 ]"));
    assertEquals("strict $[\"用户\"]", JsonPathSpec.normalize("$['\\u7528\\u6237']"));
    assertEquals("strict $[\"😀\"]", JsonPathSpec.normalize("$['\\uD83D\\uDE00']"));
    for (String name : List.of("\\uD800", "\\uDC00", "\\uD800x\\uDC00", "\\u12", "\\uGGGG")) {
      assertNull(JsonPathSpec.normalize("$['" + name + "']"), name);
    }
    assertEquals("strict $[\"aq\"]", JsonPathSpec.normalize("$['a\\q']"));
    assertEquals("strict $[\"x61\"]", JsonPathSpec.normalize("$['\\x61']"));
    assertEquals("strict $[\"*\"]", JsonPathSpec.normalize("$['\\*']"));
    assertEquals("strict $[\"用户\"]", JsonPathSpec.normalize("$['\\用户']"));
    assertEquals("strict $[\"😀\"]", JsonPathSpec.normalize("$['\\😀']"));
    assertEquals("strict $[\"a\\nb\"]", JsonPathSpec.normalize("$['a\\\nb']"));
    assertEquals("strict $[\"a\\u0000b\"]", JsonPathSpec.normalize("$['a\\\u0000b']"));
  }

  @Test
  void onlyVerifiedPathSpacesAreNormalized() {
    assertEquals("strict $[0001]", JsonPathSpec.normalize("$[ 0001 ]"));
    assertEquals("lax $[' a b '][1]", JsonPathSpec.normalize(" \tLaX $[ ' a b ' ][ 1 ]  "));
    assertEquals("strict $[\"a'b\"]", JsonPathSpec.normalize("$[ \"a'b\" ]"));
    assertEquals("strict $[''][\"\"]", JsonPathSpec.normalize("$[ '' ][ \"\" ]"));
    for (String path : List.of(" $[1]", "$[\t1]", "$[\t'a']", "$[1 2]", "$[1]\t", "$[1] \n")) {
      assertNull(JsonPathSpec.normalize(path), path);
    }
  }

  @Test
  void arrayIndexesTrimOnlyTrailingAsciiControlsInsideTheBrackets() {
    for (int c = 0; c <= 0x20; c++) {
      assertEquals("strict $[-0001]", JsonPathSpec.normalize("$[ -0001 " + (char) c + " ]"));
      assertEquals("lax $.a[0]['q']", JsonPathSpec.normalize("lax $.a[0" + (char) c + "]['q']"));
    }
    for (String path :
        List.of(
            "$[1\u0021]",
            "$[1\u007f]",
            "$[1\u0085]",
            "$[1\u00a0]",
            "$[1\u2003]",
            "$[1\u2028]",
            "$[1\t2]",
            "$[-\t1]",
            "$[\t1]",
            "$[1]\t")) {
      assertNull(JsonPathSpec.normalize(path), path);
    }
  }

  @Test
  void columnPathsFallBackBeforeNativeCompilation() {
    var types =
        new JavaTypeFactoryImpl() {
          @Override
          public Charset getDefaultCharset() {
            return Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
          }
        };
    var rex = new RexBuilder(types);
    var text = types.createSqlType(SqlTypeName.VARCHAR);
    for (String name : List.of("JSON_VALUE", "JSON_EXISTS")) {
      var function =
          new SqlFunction(
              name, SqlKind.OTHER_FUNCTION, null, null, null, SqlFunctionCategory.SYSTEM);
      var output = name.equals("JSON_VALUE") ? text : types.createSqlType(SqlTypeName.BOOLEAN);
      var call =
          rex.makeCall(
              output, function, List.of(rex.makeInputRef(text, 0), rex.makeInputRef(text, 1)));
      var config = new org.apache.flink.configuration.Configuration();
      try (var ignored = NativeConfig.usePlannerConfig(config)) {
        assertNull(RexExpression.encodeProjections(List.of(call), List.of("v")));
        for (String path :
            List.of(
                "$.a",
                "$.order-id.123",
                "$.a\tb",
                "$.a\\u0061",
                "$[-1]",
                "$[-2147483648]",
                "$[-0]",
                "$[ 01\t\n ]",
                "$['a\\'b']",
                "$['a\\\\b']",
                "$['\\用户']",
                "$['a\\\nb']",
                "$['\\u0000']")) {
          var literalPath =
              rex.makeCall(
                  output, function, List.of(rex.makeInputRef(text, 0), rex.makeLiteral(path)));
          var encoded = RexExpression.encodeProjections(List.of(literalPath), List.of("v"));
          assertNotNull(encoded);
          var binding = encoded.udfBinding();
          long[] constants = encoded.longs();
          try {
            assertSame(
                constants, binding.bind(constants), "literal paths must register no JVM UDF");
          } finally {
            binding.unbind();
          }
        }
      }
    }
  }
}

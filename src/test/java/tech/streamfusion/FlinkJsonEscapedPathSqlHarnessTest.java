package tech.streamfusion;

import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.io.JsonStringEncoder;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkJsonEscapedPathSqlHarnessTest {
  static Stream<Arguments> names() {
    return Stream.of(
        Arguments.of("a\\'b", "a'b"),
        Arguments.of("a\\\"b", "a\"b"),
        Arguments.of("a\\\\b", "a\\b"),
        Arguments.of("a\\/b", "a/b"),
        Arguments.of("a\\bb", "a\bb"),
        Arguments.of("a\\fb", "a\fb"),
        Arguments.of("a\\nb", "a\nb"),
        Arguments.of("a\\rb", "a\rb"),
        Arguments.of("a\\tb", "a\tb"),
        Arguments.of("\\u0000", "\u0000"),
        Arguments.of("\\u001f", "\u001f"),
        Arguments.of("\\u0027", "'"),
        Arguments.of("\\u0022", "\""),
        Arguments.of("\\u005c", "\\"),
        Arguments.of("\\u005cu0061", "\\u0061"),
        Arguments.of("\\u7528\\u6237", "用户"),
        Arguments.of("\\uD83D\\uDE00", "😀"),
        Arguments.of("a\\\\\\'b", "a\\'b"),
        Arguments.of("\\u002a", "*"),
        Arguments.of("\\u005d\\u002e", "]."));
  }

  static Stream<Arguments> nonstandardAsciiNames() {
    return IntStream.rangeClosed(0x20, 0x7e)
        .filter(c -> "\"'\\/bfnrtu".indexOf(c) < 0)
        .mapToObj(c -> Arguments.of("a\\" + (char) c + "b", "a" + (char) c + "b"));
  }

  @ParameterizedTest
  @MethodSource("nonstandardAsciiNames")
  void nonstandardAsciiEscapesDropOnlyTheBackslash(String escaped, String name) throws Exception {
    escapedNamesPreserveSelectionAndPolicies(escaped, name);
  }

  static Stream<Arguments> unicodeAndControlNames() {
    return IntStream.concat(
            IntStream.rangeClosed(0, 0x1f),
            IntStream.of(
                0x7f, 0x85, 0xa0, 0xff, 0x301, 0x3a3, 0x7528, 0x2028, 0x2029, 0xfffd, 0xffff,
                0x1f600, 0x1d11e, 0x10ffff))
        .mapToObj(c -> new String(Character.toChars(c)))
        .map(c -> Arguments.of("a\\" + c + "b", "a" + c + "b"));
  }

  @ParameterizedTest
  @MethodSource("unicodeAndControlNames")
  void unicodeAndControlEscapesPreserveMemberIdentity(String escaped, String name)
      throws Exception {
    escapedNamesPreserveSelectionAndPolicies(escaped, name);
  }

  @ParameterizedTest
  @MethodSource("names")
  void escapedNamesPreserveSelectionAndPolicies(String escaped, String name) throws Exception {
    for (char quote : new char[] {'\'', '"'}) {
      String path = "$[ " + quote + escaped + quote + " ]";
      String literal = path.replace("'", "''");
      NativeParity.assertParity(
          () -> documents(name, 39),
          "SELECT id, JSON_VALUE(s, '"
              + literal
              + "'), "
              + "JSON_VALUE(s, 'lax "
              + literal
              + "' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR), "
              + "JSON_VALUE(s, 'strict "
              + literal
              + "' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR), "
              + "JSON_EXISTS(s, '"
              + literal
              + "' UNKNOWN ON ERROR), "
              + "JSON_EXISTS(s, 'lax "
              + literal
              + "'), "
              + "JSON_EXISTS(s, '"
              + literal
              + "' TRUE ON ERROR) FROM inputs");
    }
  }

  @Test
  void nestedEscapesAndIndependentPathsCrossBatchBoundaries() throws Exception {
    String key = json("a\\'b");
    String[] rows = new String[5003];
    for (int i = 0; i < rows.length; i++) {
      rows[i] =
          "{"
              + key
              + ":[{\"\\u0000\":"
              + i
              + "}],\"a\\nb\":true,\"a/b\":1.25,\"aq\":true,\"x61\":1.25,\"*\":null,"
              + "\"用户\":{\"姓.名\":"
              + i
              + "}}";
    }
    NativeParity.assertParity(
        () -> TextTimeFunctionTestInputs.textRows(rows),
        "SELECT id, JSON_VALUE(s, '$[\"a\\\\\\''b\"][-1][\"\\u0000\"]' RETURNING INTEGER), "
            + "JSON_VALUE(s, '$[\"a\\nb\"]' RETURNING BOOLEAN), "
            + "JSON_VALUE(s, '$[\"a\\/b\"]' RETURNING DOUBLE), "
            + "JSON_EXISTS(s, '$[\"a\\\\\\''b\"][-1][\"\\u0000\"]'), "
            + "JSON_VALUE(s, '$[\"a\\q\"]' RETURNING BOOLEAN), "
            + "JSON_VALUE(s, '$[\"\\x61\"]' RETURNING DOUBLE), "
            + "JSON_EXISTS(s, '$[\"\\*\"]'), "
            + "JSON_VALUE(s, '$[\"\\用户\"][\"\\姓.\\名\"]' RETURNING INTEGER), "
            + "JSON_VALUE(s, '$[\"a\\\nb\"]' RETURNING BOOLEAN) FROM inputs");
  }

  @Test
  void escapedSelectionKeepsConversionFailureOutsideOnError() {
    NativeFailureParity.run(
            () -> TextTimeFunctionTestInputs.textRows("{\"a'b\":\"wrong type\"}"),
            "SELECT JSON_VALUE(s, '$[\"a\\''b\"]' RETURNING INTEGER NULL ON ERROR) FROM inputs")
        .assertFailure(
            ClassCastException.class,
            "java.lang.String cannot be cast to class java.lang.Integer",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @Test
  void missingEscapedNamesRetainErrorPolicies() {
    JsonFunctionTestInputs.assertFailsLikeFlink(
        "{}", "JSON_EXISTS(s, '$[\"a\\nb\"]' ERROR ON ERROR)");
    JsonFunctionTestInputs.assertFails(
        "{}", "JSON_VALUE(s, '$[\"a\\nb\"]' ERROR ON ERROR)", "JSON_VALUE ERROR");
    JsonFunctionTestInputs.assertFails(
        "{}", "JSON_VALUE(s, 'lax $[\"a\\nb\"]' ERROR ON EMPTY)", "JSON_VALUE EMPTY");
  }

  @ParameterizedTest
  @ValueSource(strings = {"\\uD800", "\\uDC00", "\\uD800x\\uDC00", "\\u12", "\\uGGGG"})
  void invalidAndSurrogateEscapesUseHostSemantics(String name) throws Exception {
    NativeParity.assertParity(
        () -> documents("?", 19),
        "SELECT JSON_VALUE(s, '$[\""
            + name
            + "\"]'), JSON_EXISTS(s, '$[\""
            + name
            + "\"]') FROM inputs");
  }

  private static String json(String text) {
    return "\"" + new String(JsonStringEncoder.getInstance().quoteAsString(text)) + "\"";
  }

  private static TableEnvironment documents(String name, int count) {
    String key = json(name);
    String[] values = {
      null,
      "",
      "null",
      "{}",
      "[]",
      "true",
      "42",
      "\"root\"",
      "{" + key + ":\"value\"}",
      "{" + key + ":null}",
      "{" + key + ":false}",
      "{" + key + ":42}",
      "{" + key + ":[]}",
      "{" + key + ":{}}",
      "{" + key + ":\"old\"," + key + ":\"new\"}",
      "{" + key + ":\"old\"," + key + ":null}",
      "{" + key + ":\"value\",\"bad\":[}",
      "{" + key + ":\"value\",\"bad\":1e2147483648}",
      "{" + key + ":\"value\"} trailing"
    };
    String[] rows = new String[count];
    for (int i = 0; i < count; i++) rows[i] = values[i % values.length];
    return TextTimeFunctionTestInputs.textRows(rows);
  }
}

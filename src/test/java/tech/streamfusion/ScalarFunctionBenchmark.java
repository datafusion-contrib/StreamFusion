package tech.streamfusion;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

/** Per-function end-to-end diagnostics, with source-matched identity controls. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class ScalarFunctionBenchmark {
  private static final long ROWS = Long.getLong("scalar.rows", 2_000_000L);
  private static final int BYTES = Integer.getInteger("scalar.bytes", 264);
  private static final int WARMUP = Integer.getInteger("scalar.warmup", 2);
  private static final int RUNS = Integer.getInteger("scalar.runs", 5);
  private static final boolean UNICODE = Boolean.getBoolean("scalar.unicode");
  private static final int NULL_EVERY = Integer.getInteger("scalar.nullEvery", 0);
  private static final int JSON_FIELDS = Integer.getInteger("scalar.json.fields", 0);
  private static final String ENGINE =
      System.getProperty("scalar.engine", "both").toLowerCase(Locale.ROOT);

  private record Query(String name, String input, String expression, String outputType) {
    Query(String name, String input, String expression) {
      this(name, input, expression, input.equals("numbers") ? "BIGINT" : "STRING");
    }

    String sql() {
      return "INSERT INTO sink SELECT " + expression + ", TRUE FROM inputs";
    }

    String ddl() {
      return "CREATE TABLE sink (v "
          + outputType()
          + ", anchor BOOLEAN) WITH ('connector' = 'blackhole')";
    }
  }

  private static final List<Query> SCALAR_FUNCTIONS =
      List.of(
          new Query("HASH_CODE_STRING", "text", "HASH_CODE(s)", "INT"),
          new Query("HASH_CODE_BIGINT", "bigint", "HASH_CODE(n)", "INT"),
          new Query("HASH_CODE_DECIMAL", "tt_decimal", "HASH_CODE(n)", "INT"),
          new Query("STRING_TO_INT", "integer_text", "CAST(s AS INT)", "INT"),
          new Query(
              "TRY_STRING_TO_DECIMAL",
              "integer_text",
              "TRY_CAST(s AS DECIMAL(38,9))",
              "DECIMAL(38,9)"),
          new Query(
              "TRY_DECIMAL_NARROW", "tt_decimal", "TRY_CAST(n AS DECIMAL(20,2))", "DECIMAL(20,2)"),
          new Query("INT_TO_STRING", "integer", "CAST(n AS STRING)", "STRING"),
          new Query("RAND_LITERAL", "integer", "RAND(42)", "DOUBLE"),
          new Query("RAND_DYNAMIC", "integer", "RAND(n)", "DOUBLE"),
          new Query("RAND_INTEGER_LITERAL", "integer", "RAND_INTEGER(42, 100)", "INT"),
          new Query("IFNULL_STRING", "text", "IFNULL(s, 'missing')", "STRING"),
          new Query("IF_STRING", "text", "IF(s IS NULL, 'missing', s)", "STRING"),
          new Query("IF_BIGINT", "bigint", "IF(n > 0, n, CAST(0 AS BIGINT))", "BIGINT"),
          new Query("IFNULL_BIGINT", "bigint", "IFNULL(n, CAST(-1 AS BIGINT))", "BIGINT"),
          new Query(
              "IFNULL_DECIMAL",
              "tt_decimal",
              "IFNULL(n, CAST(0 AS DECIMAL(38,9)))",
              "DECIMAL(38,9)"),
          new Query("STRING_TO_BOOLEAN", "boolean_text", "CAST(s AS BOOLEAN)", "BOOLEAN"),
          new Query("DECIMAL_ROUND_POS", "tt_decimal", "ROUND(n, 2)", "DECIMAL(32,2)"),
          new Query("DECIMAL_TRUNCATE_POS", "tt_decimal", "TRUNCATE(n, 2)", "DECIMAL(32,2)"),
          new Query("DECIMAL_TRUNCATE_NEG", "tt_decimal", "TRUNCATE(n, -3)", "DECIMAL(30,0)"),
          new Query("DECIMAL_ROUND_NEG", "tt_decimal", "ROUND(n, -3)", "DECIMAL(30,0)"),
          new Query("DECIMAL_ROUND_EXPAND", "tt_decimal", "ROUND(n, 12)", "DECIMAL(38,9)"),
          new Query("DECIMAL_TO_BIGINT", "tt_decimal", "CAST(n AS BIGINT)", "BIGINT"),
          new Query("DECIMAL_TO_FLOAT", "tt_decimal", "CAST(n AS FLOAT)", "FLOAT"),
          new Query(
              "DECIMAL_ARRAY_TO_FLOAT",
              "tt_decimal_array",
              "CAST(a AS ARRAY<FLOAT>)",
              "ARRAY<FLOAT>"),
          new Query("POWER_EXACT", "numbers", "POWER(CAST(n AS DOUBLE), 0.5)", "DOUBLE"),
          new Query("FROM_UNIXTIME_DEFAULT", "tt_unix_time", "FROM_UNIXTIME(n)", "STRING"),
          new Query(
              "FROM_UNIXTIME_LITERAL",
              "tt_unix_time",
              "FROM_UNIXTIME(n, 'yyyyMMddHHmm')",
              "STRING"),
          new Query("FROM_UNIXTIME_BRIDGE", "tt_unix_time", "FROM_UNIXTIME(n, p)", "STRING"),
          new Query("ASCII", "tt_ascii", "ASCII(s)", "INT"),
          new Query("CHR", "bigint", "CHR(n)", "STRING"),
          new Query("GREATEST", "numbers", "GREATEST(n, m, 17)"),
          new Query("LEAST", "numbers", "LEAST(n, m, 17)"),
          new Query("INITCAP", "text", "INITCAP(s)"),
          new Query("TRANSLATE", "text", "TRANSLATE(s, 'abcdef', 'ABCDEF')"),
          new Query("BTRIM", "text", "BTRIM(s)"),
          new Query("TRIM_LEADING", "tt_text", "TRIM(LEADING FROM s)"),
          new Query("TRIM_TRAILING", "tt_text", "TRIM(TRAILING FROM s)"),
          new Query("TRIM_LITERAL_SET", "tt_text", "TRIM(BOTH ' |ab' FROM s)"),
          new Query("ELT", "elt", "ELT(i, s, t)"),
          new Query("URL_ENCODE", "text", "URL_ENCODE(s)"),
          new Query("URL_DECODE", "encoded", "URL_DECODE(s)"),
          new Query("OVERLAY", "overlay", "OVERLAY(s PLACING t FROM i FOR n)"));

  private static final List<Query> SEARCH_FUNCTIONS =
      List.of(
          new Query("STARTSWITH_LITERAL", "search", "STARTSWITH(s, 'row:')", "BOOLEAN"),
          new Query("STARTSWITH_COLUMN", "search_prefix", "STARTSWITH(s, needle)", "BOOLEAN"),
          new Query("ENDSWITH_LITERAL", "search", "ENDSWITH(s, ':match')", "BOOLEAN"),
          new Query("ENDSWITH_COLUMN", "search_needle", "ENDSWITH(s, needle)", "BOOLEAN"),
          new Query("INSTR_LITERAL", "search", "INSTR(s, ':match')", "INT"),
          new Query("INSTR_COLUMN", "search_needle", "INSTR(s, needle)", "INT"),
          new Query("INSTR3_COLUMN", "search_needle_start", "INSTR(s, needle, start_pos)", "INT"),
          new Query("INSTR4_FORWARD", "search", "INSTR(s, 'x', 1, 3)", "INT"),
          new Query("INSTR4_REVERSE", "search", "INSTR(s, 'x', -1, 3)", "INT"),
          new Query("LOCATE2_LITERAL", "search", "LOCATE(':match', s)", "INT"),
          new Query("LOCATE2_COLUMN", "search_needle", "LOCATE(needle, s)", "INT"),
          new Query("LOCATE3_LITERAL", "search_start", "LOCATE(':match', s, start_pos)", "INT"),
          new Query(
              "LOCATE3_COLUMN", "search_needle_start", "LOCATE(needle, s, start_pos)", "INT"));

  private static final List<Query> ENCODING_FUNCTIONS =
      Stream.concat(
              Stream.of("tinyint", "smallint", "integer", "bigint")
                  .flatMap(
                      type ->
                          Stream.of(
                              new Query("BIN_" + type.toUpperCase(Locale.ROOT), type, "BIN(n)"),
                              new Query("HEX_" + type.toUpperCase(Locale.ROOT), type, "HEX(n)"))),
              Stream.of(
                  new Query("HEX_STRING", "text", "HEX(s)"),
                  new Query("TO_BASE64", "text", "TO_BASE64(s)"),
                  new Query("TO_BASE64_BINARY", "tt_bytes", "TO_BASE64(b)"),
                  new Query("ENCODE_UTF16", "tt_text", "ENCODE(s, 'UTF-16')", "BYTES"),
                  new Query("ENCODE_UTF16BE", "tt_text", "ENCODE(s, 'UTF-16BE')", "BYTES"),
                  new Query("ENCODE_UTF16LE", "tt_text", "ENCODE(s, 'UTF-16LE')", "BYTES"),
                  new Query("DECODE_UTF16", "tt_utf16", "DECODE(b, 'UTF-16')"),
                  new Query("DECODE_UTF16BE", "tt_utf16be", "DECODE(b, 'UTF-16BE')"),
                  new Query("DECODE_UTF16LE", "tt_utf16le", "DECODE(b, 'UTF-16LE')"),
                  new Query("UNHEX", "hex", "UNHEX(s)", "BYTES")))
          .toList();

  private static final List<Query> FUNCTIONS =
      Stream.of(
              SCALAR_FUNCTIONS,
              SEARCH_FUNCTIONS,
              ENCODING_FUNCTIONS,
              TextTimeFunctions.QUERIES,
              List.of(
                  new Query("UDF_DECIMAL", "tt_decimal", "decimal_identity(n)", "DECIMAL(38,9)"),
                  new Query(
                      "UDF_DECIMAL_IS_NULL",
                      "tt_udf_decimal",
                      "decimal_from_text(s) IS NULL",
                      "BOOLEAN"),
                  new Query(
                      "UDF_DECIMAL_NESTED",
                      "tt_udf_decimal",
                      "decimal_external(decimal_from_text(s))",
                      "DECIMAL(38,9)"),
                  new Query("UDF_BINARY", "tt_bytes", "binary_identity(b)", "BYTES"),
                  new Query("SHA1", "tt_text", "SHA1(s)"),
                  new Query("JSON_STRING_TEXT", "tt_text", "JSON_STRING(s)"),
                  new Query("JSON_STRING_ARRAY", "tt_json_array", "JSON_STRING(a)"),
                  new Query(
                      "JSON_QUERY_ARRAY_RESULT",
                      "tt_json_array",
                      "ROW(a, JSON_QUERY(s, '$.items[*]'))",
                      "ROW<items ARRAY<STRING>, json_result STRING>"),
                  new Query("JSON_STRING_BOOLEAN", "tt_boolean", "JSON_STRING(b)"),
                  new Query("JSON_STRING_INTEGER", "bigint", "JSON_STRING(n)", "STRING"),
                  new Query("JSON_STRING_DECIMAL", "tt_decimal", "JSON_STRING(n)", "STRING"),
                  new Query(
                      "JSON_OBJECT_DECIMAL", "tt_decimal", "JSON_OBJECT('n' VALUE n)", "STRING"),
                  new Query(
                      "JSON_OBJECT_NULL",
                      "tt_json_object",
                      "JSON_OBJECT('text' VALUE s, 'id' VALUE n, 'flag' VALUE b NULL ON NULL)"),
                  new Query(
                      "JSON_OBJECT_ABSENT",
                      "tt_json_object",
                      "JSON_OBJECT('text' VALUE s, 'id' VALUE n, 'flag' VALUE b ABSENT ON NULL)"),
                  new Query("IS_JSON_VALUE", "tt_json_predicate", "s IS JSON VALUE", "BOOLEAN"),
                  new Query("IS_JSON_OBJECT", "tt_json_predicate", "s IS JSON OBJECT", "BOOLEAN"),
                  new Query("IS_JSON_ARRAY", "tt_json_predicate", "s IS JSON ARRAY", "BOOLEAN"),
                  new Query("IS_JSON_SCALAR", "tt_json_predicate", "s IS JSON SCALAR", "BOOLEAN"),
                  new Query("JSON_VALUE", "tt_json", "JSON_VALUE(s, 'lax $.user.name')", "STRING"),
                  new Query(
                      "JSON_VALUE_ACTIVE",
                      "tt_json",
                      "JSON_VALUE(s, 'lax $.user.active')",
                      "STRING"),
                  new Query(
                      "JSON_VALUE_ERROR",
                      "tt_json",
                      "JSON_VALUE(s, 'lax $.user.active' ERROR ON EMPTY ERROR ON ERROR)",
                      "STRING"),
                  new Query(
                      "JSON_VALUE_ESCAPED_PATH",
                      "tt_json_escaped",
                      "JSON_VALUE(s, '$[\"a\\\\b\"][\"a\\nb\"]')",
                      "STRING"),
                  new Query(
                      "JSON_EXISTS_ESCAPED_PATH",
                      "tt_json_escaped",
                      "JSON_EXISTS(s, '$[\"a\\\\b\"][\"a\\nb\"]')",
                      "BOOLEAN"),
                  new Query(
                      "JSON_VALUE_NEGATIVE",
                      "tt_json_negative",
                      "JSON_VALUE(s, '$.a[-1]')",
                      "STRING"),
                  new Query(
                      "JSON_VALUE_DOT_MEMBER",
                      "tt_json_dot_member",
                      "JSON_VALUE(s, '$.order-id.123.a\tb')",
                      "STRING"),
                  new Query(
                      "JSON_EXISTS_DOT_MEMBER",
                      "tt_json_dot_member",
                      "JSON_EXISTS(s, '$.order-id.123.a\tb')",
                      "BOOLEAN"),
                  new Query("JSON_QUERY_SLICE", "tt_json_negative", "JSON_QUERY(s, '$.a[0:2]')"),
                  new Query(
                      "JSON_EXISTS_SLICE",
                      "tt_json_negative",
                      "JSON_EXISTS(s, '$.a[0:2]')",
                      "BOOLEAN"),
                  new Query(
                      "JSON_VALUE_WILDCARD",
                      "tt_json_negative",
                      "JSON_VALUE(s, 'lax $.a[*]' DEFAULT 'empty' ON EMPTY)",
                      "STRING"),
                  new Query(
                      "JSON_EXISTS_WILDCARD",
                      "tt_json_negative",
                      "JSON_EXISTS(s, '$.a[*]')",
                      "BOOLEAN"),
                  new Query(
                      "JSON_VALUE_INDEX_UNION",
                      "tt_json_negative",
                      "JSON_VALUE(s, 'lax $.a[0,16,-1]' DEFAULT 'empty' ON EMPTY)",
                      "STRING"),
                  new Query(
                      "JSON_EXISTS_INDEX_UNION",
                      "tt_json_negative",
                      "JSON_EXISTS(s, '$.a[0,16,-1]')",
                      "BOOLEAN"),
                  new Query(
                      "JSON_VALUE_INDEX_WHITESPACE",
                      "tt_json_negative",
                      "JSON_VALUE(s, '$.a[31\t\n ]')",
                      "STRING"),
                  new Query(
                      "JSON_EXISTS_INDEX_WHITESPACE",
                      "tt_json_negative",
                      "JSON_EXISTS(s, '$.a[31\t\n ]')",
                      "BOOLEAN"),
                  new Query(
                      "JSON_VALUE_POSITIVE_CONTROL",
                      "tt_json_negative",
                      "JSON_VALUE(s, '$.a[31]')",
                      "STRING"),
                  new Query(
                      "JSON_EXISTS_NEGATIVE",
                      "tt_json_negative",
                      "JSON_EXISTS(s, '$.a[-1]')",
                      "BOOLEAN"),
                  new Query(
                      "JSON_VALUE_IDENTITY",
                      "tt_json_surrogate",
                      "JSON_VALUE(s, '$') = '?'",
                      "BOOLEAN"),
                  new Query(
                      "JSON_UNQUOTE_IDENTITY",
                      "tt_json_surrogate",
                      "JSON_UNQUOTE(s) = '?'",
                      "BOOLEAN"),
                  new Query(
                      "JSON_VALUE_EMPTY_MEMBER",
                      "tt_json_empty_member",
                      "JSON_VALUE(s, 'lax $['''']')",
                      "STRING"),
                  new Query(
                      "JSON_EXISTS_EMPTY_MEMBER",
                      "tt_json_empty_member",
                      "JSON_EXISTS(s, 'lax $[\"\"]')",
                      "BOOLEAN"),
                  new Query(
                      "JSON_VALUE_SPACED_PATH",
                      "tt_json",
                      "JSON_VALUE(s, 'lax $[ ''user'' ][ ''name'' ]')",
                      "STRING"),
                  new Query(
                      "JSON_VALUE_NONSTANDARD_ESCAPE",
                      "tt_json",
                      "JSON_VALUE(s, 'lax $[\"u\\ser\"][\"na\\me\"]')",
                      "STRING"),
                  new Query(
                      "JSON_EXISTS_NONSTANDARD_ESCAPE",
                      "tt_json",
                      "JSON_EXISTS(s, 'lax $[\"u\\ser\"][\"na\\me\"]')",
                      "BOOLEAN"),
                  new Query(
                      "JSON_EXISTS_SPACED_PATH",
                      "tt_json",
                      "JSON_EXISTS(s, 'lax $[ ''user'' ][ ''name'' ]')",
                      "BOOLEAN"),
                  new Query(
                      "JSON_VALUE_ACTIVE",
                      "tt_json",
                      "JSON_VALUE(s, 'lax $.user.active')",
                      "STRING"),
                  new Query(
                      "JSON_VALUE_ERROR",
                      "tt_json",
                      "JSON_VALUE(s, 'lax $.user.active' ERROR ON EMPTY ERROR ON ERROR)",
                      "STRING"),
                  new Query(
                      "JSON_VALUE_ESCAPED_PATH",
                      "tt_json_escaped",
                      "JSON_VALUE(s, '$[\"a\\\\b\"][\"a\\nb\"]')",
                      "STRING"),
                  new Query(
                      "JSON_EXISTS_ESCAPED_PATH",
                      "tt_json_escaped",
                      "JSON_EXISTS(s, '$[\"a\\\\b\"][\"a\\nb\"]')",
                      "BOOLEAN"),
                  new Query(
                      "JSON_VALUE_NEGATIVE",
                      "tt_json_negative",
                      "JSON_VALUE(s, '$.a[-1]')",
                      "STRING"),
                  new Query(
                      "JSON_VALUE_DOT_MEMBER",
                      "tt_json_dot_member",
                      "JSON_VALUE(s, '$.order-id.123.a\tb')",
                      "STRING"),
                  new Query(
                      "JSON_EXISTS_DOT_MEMBER",
                      "tt_json_dot_member",
                      "JSON_EXISTS(s, '$.order-id.123.a\tb')",
                      "BOOLEAN"),
                  new Query("JSON_QUERY_SLICE", "tt_json_negative", "JSON_QUERY(s, '$.a[0:2]')"),
                  new Query(
                      "JSON_EXISTS_SLICE",
                      "tt_json_negative",
                      "JSON_EXISTS(s, '$.a[0:2]')",
                      "BOOLEAN"),
                  new Query(
                      "JSON_VALUE_WILDCARD",
                      "tt_json_negative",
                      "JSON_VALUE(s, 'lax $.a[*]' DEFAULT 'empty' ON EMPTY)",
                      "STRING"),
                  new Query(
                      "JSON_EXISTS_WILDCARD",
                      "tt_json_negative",
                      "JSON_EXISTS(s, '$.a[*]')",
                      "BOOLEAN"),
                  new Query(
                      "JSON_VALUE_INDEX_UNION",
                      "tt_json_negative",
                      "JSON_VALUE(s, 'lax $.a[0,16,-1]' DEFAULT 'empty' ON EMPTY)",
                      "STRING"),
                  new Query(
                      "JSON_EXISTS_INDEX_UNION",
                      "tt_json_negative",
                      "JSON_EXISTS(s, '$.a[0,16,-1]')",
                      "BOOLEAN"),
                  new Query(
                      "JSON_VALUE_INDEX_WHITESPACE",
                      "tt_json_negative",
                      "JSON_VALUE(s, '$.a[31\t\n ]')",
                      "STRING"),
                  new Query(
                      "JSON_EXISTS_INDEX_WHITESPACE",
                      "tt_json_negative",
                      "JSON_EXISTS(s, '$.a[31\t\n ]')",
                      "BOOLEAN"),
                  new Query(
                      "JSON_VALUE_POSITIVE_CONTROL",
                      "tt_json_negative",
                      "JSON_VALUE(s, '$.a[31]')",
                      "STRING"),
                  new Query(
                      "JSON_EXISTS_NEGATIVE",
                      "tt_json_negative",
                      "JSON_EXISTS(s, '$.a[-1]')",
                      "BOOLEAN"),
                  new Query(
                      "JSON_VALUE_IDENTITY",
                      "tt_json_surrogate",
                      "JSON_VALUE(s, '$') = '?'",
                      "BOOLEAN"),
                  new Query(
                      "JSON_UNQUOTE_IDENTITY",
                      "tt_json_surrogate",
                      "JSON_UNQUOTE(s) = '?'",
                      "BOOLEAN"),
                  new Query(
                      "JSON_VALUE_EMPTY_MEMBER",
                      "tt_json_empty_member",
                      "JSON_VALUE(s, 'lax $['''']')",
                      "STRING"),
                  new Query(
                      "JSON_EXISTS_EMPTY_MEMBER",
                      "tt_json_empty_member",
                      "JSON_EXISTS(s, 'lax $[\"\"]')",
                      "BOOLEAN"),
                  new Query(
                      "JSON_VALUE_SPACED_PATH",
                      "tt_json",
                      "JSON_VALUE(s, 'lax $[ ''user'' ][ ''name'' ]')",
                      "STRING"),
                  new Query(
                      "JSON_VALUE_NONSTANDARD_ESCAPE",
                      "tt_json",
                      "JSON_VALUE(s, 'lax $[\"u\\ser\"][\"na\\me\"]')",
                      "STRING"),
                  new Query(
                      "JSON_EXISTS_NONSTANDARD_ESCAPE",
                      "tt_json",
                      "JSON_EXISTS(s, 'lax $[\"u\\ser\"][\"na\\me\"]')",
                      "BOOLEAN"),
                  new Query(
                      "JSON_EXISTS_SPACED_PATH",
                      "tt_json",
                      "JSON_EXISTS(s, 'lax $[ ''user'' ][ ''name'' ]')",
                      "BOOLEAN"),
                  new Query(
                      "JSON_VALUE_UNICODE_PATH",
                      "tt_json_member",
                      "JSON_VALUE(s, 'lax $.\u7528\u6237[\"\u59d3.\u540d\"]')",
                      "STRING"),
                  new Query(
                      "JSON_EXISTS_UNICODE_PATH",
                      "tt_json_member",
                      "JSON_EXISTS(s, 'lax $.\u7528\u6237[\"\u59d3.\u540d\"]')",
                      "BOOLEAN"),
                  new Query(
                      "JSON_VALUE_UNICODE_ESCAPE",
                      "tt_json_member",
                      "JSON_VALUE(s, 'lax $[\"\\用户\"][\"\\姓.\\名\"]')",
                      "STRING"),
                  new Query(
                      "JSON_EXISTS_UNICODE_ESCAPE",
                      "tt_json_member",
                      "JSON_EXISTS(s, 'lax $[\"\\用户\"][\"\\姓.\\名\"]')",
                      "BOOLEAN"),
                  new Query(
                      "JSON_VALUE_BOOLEAN",
                      "tt_json_boolean",
                      "JSON_VALUE(s, '$.v' RETURNING BOOLEAN)",
                      "BOOLEAN"),
                  new Query(
                      "JSON_VALUE_INTEGER",
                      "tt_json_integer",
                      "JSON_VALUE(s, '$.v' RETURNING INTEGER)",
                      "INT"),
                  new Query(
                      "JSON_VALUE_DOUBLE",
                      "tt_json_double",
                      "JSON_VALUE(s, '$.v' RETURNING DOUBLE)",
                      "DOUBLE"),
                  new Query(
                      "JSON_EXISTS", "tt_json", "JSON_EXISTS(s, 'lax $.user.name')", "BOOLEAN")))
          .flatMap(List::stream)
          .toList();

  @Test
  void individualFunctions() throws Exception {
    if (ROWS < 1 || BYTES < 0 || WARMUP < 0 || RUNS < 1 || NULL_EVERY < 0) {
      throw new IllegalArgumentException("Invalid scalar benchmark sizes/trial counts");
    }
    if (!List.of("both", "flink", "native").contains(ENGINE)) {
      throw new IllegalArgumentException("scalar.engine must be both, flink, or native");
    }
    List<Query> selected = selected();
    Map<String, Query> baselines = new LinkedHashMap<>();
    for (Query query : selected) {
      if (query.input().startsWith("tt_")) {
        baselines.putIfAbsent(
            query.input(),
            new Query(
                "BASELINE_" + query.input(),
                query.input(),
                TextTimeBenchmarkInputs.baselineExpression(query.input()),
                TextTimeBenchmarkInputs.baselineType(query.input())));
        continue;
      }
      boolean integer = isIntegerInput(query.input());
      baselines.putIfAbsent(
          query.input(),
          new Query(
              "BASELINE_" + query.input(),
              query.input(),
              integer || query.input().equals("numbers") ? "n" : "s",
              integer
                  ? query.input().toUpperCase(Locale.ROOT)
                  : query.input().equals("numbers") ? "BIGINT" : "STRING"));
    }
    List<Query> queries = new ArrayList<>(baselines.values());
    queries.addAll(selected);
    List<String> csv =
        new ArrayList<>(
            List.of(
                "function,input,output_type,payload_bytes,json_fields,unicode,null_every,rows,engine,trial,seconds"));
    Path output = Path.of(System.getProperty("scalar.output", "target/scalar-functions.csv"));
    if (output.getParent() != null && !Files.isDirectory(output.getParent())) {
      Files.createDirectories(output.getParent());
    }
    for (Query query : queries) {
      assertPlan(query);
      double[][] times = new double[2][RUNS];
      for (int trial = 0; trial < WARMUP + RUNS; trial++) {
        for (int turn = 0; turn < 2; turn++) {
          int engine = (trial + turn) % 2;
          if (ENGINE.equals(engine == 1 ? "flink" : "native")) {
            continue;
          }
          double seconds = run(query, engine == 1);
          if (trial >= WARMUP) {
            times[engine][trial - WARMUP] = seconds;
            csv.add(
                String.format(
                    Locale.ROOT,
                    "%s,%s,\"%s\",%d,%d,%s,%d,%d,%s,%d,%.6f",
                    query.name(),
                    query.input(),
                    query.outputType(),
                    BYTES,
                    query.input().equals("tt_json") ? JSON_FIELDS : 0,
                    UNICODE,
                    NULL_EVERY,
                    ROWS,
                    engine == 1 ? "native" : "flink",
                    trial - WARMUP,
                    seconds));
          }
        }
      }
      double host = median(times[0]);
      double nativeTime = median(times[1]);
      if (ENGINE.equals("both")) {
        System.out.printf(
            Locale.ROOT,
            "[scalar] %s bytes=%d rows=%d Flink median=%.3fs %s Native median=%.3fs %s"
                + " ratio=%.3fx%n",
            query.name(),
            BYTES,
            ROWS,
            host,
            Arrays.toString(times[0]),
            nativeTime,
            Arrays.toString(times[1]),
            host / nativeTime);
      } else {
        int engine = ENGINE.equals("native") ? 1 : 0;
        System.out.printf(
            Locale.ROOT,
            "[scalar] %s bytes=%d rows=%d engine=%s median=%.3fs %s%n",
            query.name(),
            BYTES,
            ROWS,
            ENGINE,
            median(times[engine]),
            Arrays.toString(times[engine]));
      }
      Files.write(output, csv);
    }
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PROFILE_SCALAR", matches = "true")
  void profileFunction() throws Exception {
    Query query = selected().get(0);
    boolean useNative = Boolean.parseBoolean(System.getProperty("scalar.native", "true"));
    assertPlan(query);
    for (int warmup = 0; warmup < WARMUP; warmup++) {
      run(query, useNative);
    }
    System.out.printf(
        "[scalar-profile] ready pid=%d function=%s native=%s%n",
        ProcessHandle.current().pid(), query.name(), useNative);
    String profiler = System.getProperty("profile.asprof", "asprof");
    Path directory = Path.of(System.getProperty("profile.outputDir", "target/profiles/scalar"));
    Files.createDirectories(directory);
    Path recording =
        directory
            .resolve(
                query.name().toLowerCase(Locale.ROOT) + (useNative ? "-native.jfr" : "-flink.jfr"))
            .toAbsolutePath();
    profileCommand(
        profiler,
        "start",
        "-e",
        "cpu",
        "-i",
        "1ms",
        "-f",
        recording.toString(),
        Long.toString(ProcessHandle.current().pid()));
    try {
      long deadline = System.nanoTime() + Long.getLong("scalar.seconds", 45L) * 1_000_000_000L;
      do {
        run(query, useNative);
      } while (System.nanoTime() < deadline);
    } finally {
      profileCommand(profiler, "stop", Long.toString(ProcessHandle.current().pid()));
    }
  }

  private static void profileCommand(String... command) throws Exception {
    int exit = new ProcessBuilder(command).inheritIO().start().waitFor();
    if (exit != 0) {
      throw new IllegalStateException("Profiler exited " + exit);
    }
  }

  private static List<Query> selected() {
    String names = System.getProperty("scalar.functions", "ALL").toUpperCase(Locale.ROOT);
    Map<String, Query> queries = new LinkedHashMap<>();
    for (String name : names.split(",", -1)) {
      List<Query> matches =
          switch (name.trim()) {
            case "ALL" -> FUNCTIONS;
            case "SCALAR" -> SCALAR_FUNCTIONS;
            case "SEARCH" -> SEARCH_FUNCTIONS;
            case "ENCODING" -> ENCODING_FUNCTIONS;
            case "TEXT_TIME" -> TextTimeFunctions.QUERIES;
            default -> FUNCTIONS.stream().filter(q -> q.name().equals(name.trim())).toList();
          };
      if (matches.isEmpty()) {
        throw new IllegalArgumentException("Unknown scalar.functions: " + name);
      }
      matches.forEach(query -> queries.putIfAbsent(query.name(), query));
    }
    return List.copyOf(queries.values());
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    int middle = sorted.length / 2;
    return sorted.length % 2 == 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
  }

  private static void assertPlan(Query query) {
    TableEnvironment tables = environment(query.input());
    tables.executeSql(query.ddl());
    String plan = NativePlanner.explain(tables, query.sql());
    if (!plan.contains("NativeCalc")
        || !plan.contains("RowDataToArrow")
        || !plan.contains("ArrowToRowData")) {
      throw new IllegalStateException(
          query.name() + " must include NativeCalc and both transposes: " + plan);
    }
  }

  private static double run(Query query, boolean useNative) throws Exception {
    TableEnvironment tables = environment(query.input());
    tables.executeSql(query.ddl());
    PhysicalPlanScan scan = useNative ? NativePlanner.install(tables) : null;
    long start = System.nanoTime();
    tables.executeSql(query.sql()).await();
    double seconds = (System.nanoTime() - start) / 1e9;
    if (useNative && scan.substitutions() == 0) {
      throw new IllegalStateException(query.name() + " fell back: " + scan.fallbackReasons());
    }
    return seconds;
  }

  private static TableEnvironment environment(String input) {
    if (input.startsWith("tt_")) {
      TableEnvironment tables =
          TextTimeBenchmarkInputs.environment(input, ROWS, BYTES, UNICODE, NULL_EVERY);
      tables.createTemporarySystemFunction("decimal_identity", DecimalIdentity.class);
      tables.createTemporarySystemFunction(
          "decimal_from_text", FlinkUdfExactTypesSqlHarnessTest.DecimalFromText.class);
      tables.createTemporarySystemFunction(
          "decimal_external", FlinkDecimalUdfConsumersSqlHarnessTest.ExternalDecimal.class);
      tables.createTemporarySystemFunction("binary_identity", BinaryIdentity.class);
      return tables;
    }
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tables = StreamTableEnvironment.create(env);
    String[] text =
        input.equals("integer_text")
            ? new String[] {"123456789", "-2147483648", "  +0042.9  ", "0", "2147483647"}
            : input.equals("boolean_text")
            ? new String[] {"true", "FALSE", "t", "0", "yes", "n"}
            : UNICODE
            ? new String[] {
              payload(" \u4e2dAbC \ud83d\ude00dEf "), payload(" \u00e9dEf \ud83d\ude42AbC ")
            }
            : new String[] {payload(" abC def_09 "), payload(" dEf abc_90 ")};
    if (input.startsWith("search")) {
      String padding = payload(UNICODE ? "\u4e2d\ud83d\ude00x" : "x");
      String[] samples = {"row:" + padding + ":match", "other:" + padding + ":miss"};
      String[] needles =
          input.equals("search_prefix")
              ? new String[] {"row:", "other:"}
              : new String[] {":match", ":miss"};
      boolean columnNeedle = input.equals("search_prefix") || input.contains("needle");
      boolean start = input.endsWith("start");
      int step = padding.codePointCount(0, padding.length()) / 3;
      List<String> names = new ArrayList<>(List.of("s"));
      List<TypeInformation<?>> types = new ArrayList<>(List.of(Types.STRING));
      Schema.Builder schema = Schema.newBuilder().column("s", DataTypes.STRING());
      if (columnNeedle) {
        names.add("needle");
        types.add(Types.STRING);
        schema.column("needle", DataTypes.STRING());
      }
      if (start) {
        names.add("start_pos");
        types.add(Types.INT);
        schema.column("start_pos", DataTypes.INT());
      }
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, ROWS - 1)
              .map(
                  i -> {
                    Row row = new Row(1 + (columnNeedle ? 1 : 0) + (start ? 1 : 0));
                    row.setField(0, isNull(i) ? null : samples[(int) (i % 2)]);
                    if (columnNeedle) {
                      row.setField(1, needles[(int) (i / 2 % 2)]);
                    }
                    if (start) {
                      row.setField(columnNeedle ? 2 : 1, (int) (i % 3) * step + 1);
                    }
                    return row;
                  })
              .returns(
                  Types.ROW_NAMED(
                      names.toArray(String[]::new), types.toArray(TypeInformation<?>[]::new))),
          schema.build());
    } else if (isIntegerInput(input)) {
      TypeInformation<?> type =
          switch (input) {
            case "tinyint" -> Types.BYTE;
            case "smallint" -> Types.SHORT;
            case "integer" -> Types.INT;
            default -> Types.LONG;
          };
      DataType dataType =
          switch (input) {
            case "tinyint" -> DataTypes.TINYINT();
            case "smallint" -> DataTypes.SMALLINT();
            case "integer" -> DataTypes.INT();
            default -> DataTypes.BIGINT();
          };
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, ROWS - 1)
              .map(i -> Row.of(isNull(i) ? null : integerValue(i, input)))
              .returns(Types.ROW_NAMED(new String[] {"n"}, type)),
          Schema.newBuilder().column("n", dataType).build());
    } else if (input.equals("numbers")) {
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, ROWS - 1)
              .map(i -> Row.of(isNull(i) ? null : i - ROWS / 2, ROWS / 3 - i))
              .returns(Types.ROW_NAMED(new String[] {"n", "m"}, Types.LONG, Types.LONG)),
          Schema.newBuilder()
              .column("n", DataTypes.BIGINT())
              .column("m", DataTypes.BIGINT())
              .build());
    } else if (input.equals("elt")) {
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, ROWS - 1)
              .map(
                  i ->
                      Row.of(
                          isNull(i) ? null : text[(int) (i % 2)],
                          text[(int) ((i + 1) % 2)],
                          (int) (i % 2) + 1))
              .returns(
                  Types.ROW_NAMED(
                      new String[] {"s", "t", "i"}, Types.STRING, Types.STRING, Types.INT)),
          Schema.newBuilder()
              .column("s", DataTypes.STRING())
              .column("t", DataTypes.STRING())
              .column("i", DataTypes.INT())
              .build());
    } else if (input.equals("overlay")) {
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, ROWS - 1)
              .map(
                  i -> Row.of(isNull(i) ? null : text[(int) (i % 2)], "ABC", (int) (i % 2) + 1, 2L))
              .returns(
                  Types.ROW_NAMED(
                      new String[] {"s", "t", "i", "n"},
                      Types.STRING,
                      Types.STRING,
                      Types.INT,
                      Types.LONG)),
          Schema.newBuilder()
              .column("s", DataTypes.STRING())
              .column("t", DataTypes.STRING())
              .column("i", DataTypes.INT())
              .column("n", DataTypes.BIGINT())
              .build());
    } else {
      String pattern = UNICODE ? "%E4%B8%AD+%F0%9F%98%80" : "a+b%2B%20";
      String encoded = pattern.repeat(Math.max(1, BYTES / pattern.length()));
      String[] hex = {"aF".repeat(BYTES), "01".repeat(BYTES)};
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, ROWS - 1)
              .map(
                  i ->
                      Row.of(
                          isNull(i)
                              ? null
                              : input.equals("encoded")
                                  ? encoded
                                  : input.equals("hex")
                                      ? hex[(int) (i % 2)]
                                      : text[(int) (i % text.length)]))
              .returns(Types.ROW_NAMED(new String[] {"s"}, Types.STRING)),
          Schema.newBuilder().column("s", DataTypes.STRING()).build());
    }
    return tables;
  }

  @FunctionHint(input = @DataTypeHint("DECIMAL(38,9)"), output = @DataTypeHint("DECIMAL(38,9)"))
  public static class DecimalIdentity extends ScalarFunction {
    public BigDecimal eval(BigDecimal value) {
      return value;
    }
  }

  public static class BinaryIdentity extends ScalarFunction {
    public byte[] eval(byte[] value) {
      return value;
    }
  }

  private static final class TextTimeFunctions {
    static final List<Query> QUERIES =
        List.of(
            new Query("ENCODE_UTF8", "tt_text", "ENCODE(s, 'UTF-8')", "BYTES"),
            new Query("DECODE_UTF8", "tt_bytes", "DECODE(b, 'UTF-8')", "STRING"),
            new Query("JSON_QUOTE", "tt_text", "JSON_QUOTE(s)", "STRING"),
            new Query("JSON_UNQUOTE", "tt_quoted", "JSON_UNQUOTE(s)", "STRING"),
            new Query("SPLIT", "tt_text", "SPLIT(s, '|')", "ARRAY<STRING>"),
            new Query("SUBSTRING_DYNAMIC", "tt_substring", "SUBSTRING(s, n, len)", "STRING"),
            new Query("LEFT_DYNAMIC", "tt_counted", "LEFT(s, n)", "STRING"),
            new Query("RIGHT_DYNAMIC", "tt_counted", "RIGHT(s, n)", "STRING"),
            new Query("LPAD_DYNAMIC", "tt_pad", "LPAD(s, n, p)", "STRING"),
            new Query("RPAD_DYNAMIC", "tt_pad", "RPAD(s, n, p)", "STRING"),
            new Query("SPLIT_INDEX_DYNAMIC", "tt_split", "SPLIT_INDEX(s, p, n)", "STRING"),
            new Query("TO_DATE", "tt_date_text", "TO_DATE(s)", "DATE"),
            new Query("TO_TIMESTAMP", "tt_timestamp_text", "TO_TIMESTAMP(s)", "TIMESTAMP(3)"),
            new Query("TIMESTAMP_FLOOR", "tt_timestamp", "FLOOR(ts TO MINUTE)", "TIMESTAMP(9)"),
            new Query("TIMESTAMP_CEIL", "tt_timestamp", "CEIL(ts TO SECOND)", "TIMESTAMP(9)"),
            new Query(
                "TIMESTAMP_ADD", "tt_timestamp", "TIMESTAMPADD(MONTH, 1, ts)", "TIMESTAMP(9)"),
            new Query(
                "TIMESTAMP_DIFF",
                "tt_timestamp",
                "TIMESTAMPDIFF(DAY, ts, TIMESTAMP '2024-01-01 00:00:00')",
                "BIGINT"),
            new Query("QUARTER", "tt_timestamp", "QUARTER(ts)", "BIGINT"),
            new Query("WEEK", "tt_timestamp", "WEEK(ts)", "BIGINT"),
            new Query("DAYOFYEAR", "tt_timestamp", "DAYOFYEAR(ts)", "BIGINT"),
            new Query("DAYOFWEEK", "tt_timestamp", "DAYOFWEEK(ts)", "BIGINT"),
            new Query("LTRIM_LITERAL_SET", "tt_trim", "LTRIM(s, ' |ab')", "STRING"),
            new Query("RTRIM_LITERAL_SET", "tt_trim", "RTRIM(s, ' |ab')", "STRING"));
  }

  private static String payload(String pattern) {
    int bytes = pattern.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    return pattern.repeat(BYTES / bytes) + "x".repeat(BYTES % bytes);
  }

  private static boolean isIntegerInput(String input) {
    return List.of("tinyint", "smallint", "integer", "bigint").contains(input);
  }

  private static Number integerValue(long row, String input) {
    long value = row % 2 == 0 ? row / 2 : -(row / 2) - 1;
    return switch (input) {
      case "tinyint" -> (byte) value;
      case "smallint" -> (short) value;
      case "integer" -> (int) value;
      default -> value;
    };
  }

  private static boolean isNull(long row) {
    return NULL_EVERY > 0 && row % NULL_EVERY == 0;
  }
}

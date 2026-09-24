package tech.streamfusion.compat;

import org.junit.jupiter.api.Assumptions;

/** Explicit N/A checks for syntax or host operations absent from the selected released line. */
public final class FlinkTestCapabilities {
  private FlinkTestCapabilities() {}

  public static final boolean CHECKPOINT_REUSE_NOTIFICATION = false;

  private static final java.util.Set<String> ABSENT_SQL_FUNCTIONS =
      java.util.Set.of(
          "BTRIM",
          "PRINTF",
          "REGEXP_COUNT",
          "REGEXP_INSTR",
          "REGEXP_SUBSTR",
          "REGEXP_EXTRACT_ALL",
          "ENDS_WITH",
          "ENDSWITH",
          "SPLIT",
          "URL_ENCODE",
          "ELT",
          "JSON",
          "JSON_QUOTE",
          "JSON_UNQUOTE",
          "STARTSWITH",
          "TRANSLATE3",
          "UNHEX",
          "URL_DECODE");
  public static final boolean RETRACTING_WINDOW_TVF = false;
  public static final boolean SESSION_TABLE_FUNCTION = false;
  public static final boolean DYNAMIC_JSON_QUERY = false;

  public static final boolean VARIABLE_LENGTH_ENCODE = false;
  public static final boolean CORRECTED_AVRO_TIMESTAMPS = false;
  public static final boolean AVRO_ENCODING_OPTION = false;
  public static final boolean JSON_PARSER_OPTION = false;
  public static final boolean UNNEST_ORDINALITY = false;
  public static final boolean SINK_LENGTH_ERROR = false;
  public static final boolean TIMESTAMP_LTZ_TEXT_OVERLOADS = false;
  public static final boolean TIMESTAMP_LTZ_DEFAULT_PRECISION = false;
  public static final boolean SMALL_INTEGER_ARRAY_LOOKUP = false;
  public static final boolean SUB_HOUR_TIMESTAMP_ROUNDING = false;
  public static final boolean NEGATIVE_TIMESTAMP_TO_TIME = false;
  public static final boolean RANK_OVER_AGGREGATE = false;

  public static org.apache.flink.table.types.DataType ifNullStringType() {
    return org.apache.flink.table.api.DataTypes.CHAR(7).notNull();
  }

  public static final String JSON_FRACTION_CLASS = "java.lang.Double";

  public static void requireJsonFunctions(String expression) {
    for (String name : java.util.List.of("JSON_QUOTE", "JSON_UNQUOTE")) {
      if (expression.contains(name + "(")) requireSqlFunction(name);
    }
  }

  public static void requireSqlFunction(String name) {
    Assumptions.assumeFalse(
        ABSENT_SQL_FUNCTIONS.contains(name),
        name + " is absent from Flink 1.18's released SQL function catalog");
  }

  public static void requireSessionTableFunction() {
    Assumptions.assumeTrue(
        SESSION_TABLE_FUNCTION,
        "The SESSION table function is absent from Flink 1.18; grouped session windows are"
            + " separate");
  }

  public static final boolean KEY_ORDERED_ASYNC_LOOKUP = false;
  public static final boolean STATE_TTL_HINTS = false;
  public static final boolean TEMPORAL_FIRST_LAST = false;
  public static final boolean KAFKA_TRANSACTION_NAMING = false;

  public static void requireStateTtlHint() {
    Assumptions.assumeTrue(
        STATE_TTL_HINTS,
        "STATE_TTL hints are absent from Flink 1.18; global retention is tested separately");
  }

  public static void requireFirstLastType(String type) {
    boolean temporal = type.equals("DATE") || type.startsWith("TIMESTAMP");
    Assumptions.assumeTrue(
        !temporal || TEMPORAL_FIRST_LAST,
        "Flink 1.18 cannot plan FIRST_VALUE/LAST_VALUE over " + type);
  }
}

package tech.streamfusion.compat;

import org.junit.jupiter.api.Assumptions;

/** Explicit N/A checks for syntax or host operations absent from the selected released line. */
public final class FlinkTestCapabilities {
  public static final boolean TIME_TRY_CAST_REJECTS_OUT_OF_RANGE = true;
  public static final boolean CAST_TIME_SINGLE_VALUE = true;

  private FlinkTestCapabilities() {}

  public static final boolean MIXED_TIMESTAMP_COMPARISONS = true;

  public static final boolean CHECKPOINT_REUSE_NOTIFICATION = true;

  private static final java.util.Set<String> ABSENT_SQL_FUNCTIONS = java.util.Set.of();
  public static final boolean RETRACTING_WINDOW_TVF = true;
  public static final boolean SESSION_TABLE_FUNCTION = true;
  public static final boolean DYNAMIC_JSON_QUERY = true;

  public static final boolean VARIABLE_LENGTH_ENCODE = true;
  public static final boolean CORRECTED_AVRO_TIMESTAMPS = true;
  public static final boolean AVRO_ENCODING_OPTION = true;
  public static final boolean JSON_PARSER_OPTION = true;
  public static final boolean UNNEST_ORDINALITY = true;
  public static final boolean SINK_LENGTH_ERROR = true;
  public static final boolean TIMESTAMP_LTZ_TEXT_OVERLOADS = true;
  public static final boolean TIMESTAMP_LTZ_DEFAULT_PRECISION = true;
  public static final boolean SMALL_INTEGER_ARRAY_LOOKUP = true;
  public static final boolean SUB_HOUR_TIMESTAMP_ROUNDING = true;
  public static final boolean NEGATIVE_TIMESTAMP_TO_TIME = true;
  public static final boolean RANK_OVER_AGGREGATE = true;

  public static org.apache.flink.table.types.DataType ifNullStringType() {
    return org.apache.flink.table.api.DataTypes.STRING().notNull();
  }

  public static final String JSON_FRACTION_CLASS = "java.math.BigDecimal";

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

  public static final boolean KEY_ORDERED_ASYNC_LOOKUP = true;
  public static final boolean STATE_TTL_HINTS = true;
  public static final boolean TEMPORAL_FIRST_LAST = true;
  public static final boolean KAFKA_TRANSACTION_NAMING = true;

  public static void requireStateTtlHint() {
    Assumptions.assumeTrue(
        STATE_TTL_HINTS,
        "STATE_TTL hints are absent from Flink 1.18; global retention is tested separately");
  }

  public static void requireFirstLastType(String type) {
    boolean temporal = type.equals("DATE") || type.startsWith("TIME");
    Assumptions.assumeTrue(
        !temporal || TEMPORAL_FIRST_LAST,
        "Flink 1.18 cannot plan FIRST_VALUE/LAST_VALUE over " + type);
  }
}

package tech.streamfusion.compat;

import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.*;
import org.apache.flink.table.types.logical.RowType;

public final class JsonTestSchemas {
  private JsonTestSchemas() {}

  public static final boolean[] IGNORE_NULL_FIELD_MODES = new boolean[] {false};

  public static org.apache.flink.formats.json.JsonRowDataSerializationSchema json(
      RowType type,
      TimestampFormat timestamps,
      JsonFormatOptions.MapNullKeyMode mode,
      String literal,
      boolean plainDecimal,
      boolean ignoreNulls) {
    org.junit.jupiter.api.Assumptions.assumeFalse(
        ignoreNulls, "JSON encode.ignore-null-fields is absent from Flink 1.18");
    return new org.apache.flink.formats.json.JsonRowDataSerializationSchema(
        type, timestamps, mode, literal, plainDecimal);
  }

  public static org.apache.flink.formats.json.debezium.DebeziumJsonSerializationSchema debezium(
      RowType type,
      TimestampFormat timestamps,
      JsonFormatOptions.MapNullKeyMode mode,
      String literal,
      boolean plainDecimal,
      boolean ignoreNulls) {
    org.junit.jupiter.api.Assumptions.assumeFalse(
        ignoreNulls, "JSON encode.ignore-null-fields is absent from Flink 1.18");
    return new org.apache.flink.formats.json.debezium.DebeziumJsonSerializationSchema(
        type, timestamps, mode, literal, plainDecimal);
  }

  public static org.apache.flink.formats.json.canal.CanalJsonSerializationSchema canal(
      RowType type,
      TimestampFormat timestamps,
      JsonFormatOptions.MapNullKeyMode mode,
      String literal,
      boolean plainDecimal,
      boolean ignoreNulls) {
    org.junit.jupiter.api.Assumptions.assumeFalse(
        ignoreNulls, "JSON encode.ignore-null-fields is absent from Flink 1.18");
    return new org.apache.flink.formats.json.canal.CanalJsonSerializationSchema(
        type, timestamps, mode, literal, plainDecimal);
  }

  public static org.apache.flink.formats.json.maxwell.MaxwellJsonSerializationSchema maxwell(
      RowType type,
      TimestampFormat timestamps,
      JsonFormatOptions.MapNullKeyMode mode,
      String literal,
      boolean plainDecimal,
      boolean ignoreNulls) {
    org.junit.jupiter.api.Assumptions.assumeFalse(
        ignoreNulls, "JSON encode.ignore-null-fields is absent from Flink 1.18");
    return new org.apache.flink.formats.json.maxwell.MaxwellJsonSerializationSchema(
        type, timestamps, mode, literal, plainDecimal);
  }

  public static org.apache.flink.formats.json.ogg.OggJsonSerializationSchema ogg(
      RowType type,
      TimestampFormat timestamps,
      JsonFormatOptions.MapNullKeyMode mode,
      String literal,
      boolean plainDecimal,
      boolean ignoreNulls) {
    org.junit.jupiter.api.Assumptions.assumeFalse(
        ignoreNulls, "JSON encode.ignore-null-fields is absent from Flink 1.18");
    return new org.apache.flink.formats.json.ogg.OggJsonSerializationSchema(
        type, timestamps, mode, literal, plainDecimal);
  }
}

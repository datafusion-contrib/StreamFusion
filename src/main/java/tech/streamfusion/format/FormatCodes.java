package tech.streamfusion.format;

import java.util.Map;

/**
 * The {@code MessageDecoder} format-code protocol: one int per value format, shared across the JNI
 * boundary and mirrored by the named constants in {@code native/src/formats.rs}. The codes are wire
 * format — never renumber them.
 */
public final class FormatCodes {

  private FormatCodes() {}

  public static final int UNSUPPORTED = -1;
  public static final int JSON = 0;
  /** Confluent-framed Avro; writer schemas fetched from the registry by frame id. */
  public static final int AVRO_CONFLUENT = 1;
  public static final int CSV = 2;
  public static final int RAW = 3;
  /** Bare Avro; the reader schema is derived from the table's RowType. */
  public static final int AVRO = 4;
  /** Descriptor derived from the message-class-name's generated class. */
  public static final int PROTOBUF = 5;
  public static final int DEBEZIUM_JSON = 6;
  public static final int OGG_JSON = 7;
  public static final int MAXWELL_JSON = 8;
  public static final int CANAL_JSON = 9;
  /** Debezium envelope with Confluent-framed Avro bodies; writer schemas fetched by frame id. */
  public static final int DEBEZIUM_AVRO_CONFLUENT = 10;

  private static final Map<String, Integer> BY_IDENTIFIER =
      Map.ofEntries(
          Map.entry("json", JSON),
          Map.entry("avro-confluent", AVRO_CONFLUENT),
          Map.entry("csv", CSV),
          Map.entry("raw", RAW),
          Map.entry("avro", AVRO),
          Map.entry("protobuf", PROTOBUF),
          Map.entry("debezium-json", DEBEZIUM_JSON),
          Map.entry("ogg-json", OGG_JSON),
          Map.entry("maxwell-json", MAXWELL_JSON),
          Map.entry("canal-json", CANAL_JSON),
          Map.entry("debezium-avro-confluent", DEBEZIUM_AVRO_CONFLUENT));

  /** The code for a Flink format identifier, or {@link #UNSUPPORTED}. */
  public static int forIdentifier(String identifier) {
    return identifier == null ? UNSUPPORTED : BY_IDENTIFIER.getOrDefault(identifier, UNSUPPORTED);
  }

  /** Whether the code is an insert-only format (at most one decoded row per message). */
  public static boolean isInsertOnly(int code) {
    return code >= JSON && code <= PROTOBUF;
  }

  /** Whether the code is a CDC changelog envelope (a message fans out to changelog rows). */
  public static boolean isCdc(int code) {
    return code >= DEBEZIUM_JSON && code <= DEBEZIUM_AVRO_CONFLUENT;
  }

  /** Whether the identifier decodes JSON text: plain {@code json} or a JSON CDC envelope. */
  public static boolean isJsonFamily(String identifier) {
    int code = forIdentifier(identifier);
    return code == JSON || (code >= DEBEZIUM_JSON && code <= CANAL_JSON);
  }
}

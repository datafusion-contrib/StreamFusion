package tech.streamfusion.iceberg;

import com.fasterxml.jackson.databind.ObjectMapper;

/** Reuses metadata serializers across file tasks and writers without mutable per-task state. */
final class IcebergJson {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private IcebergJson() {}

  static ObjectMapper mapper() {
    return MAPPER;
  }
}

package tech.streamfusion.format.avroconfluent;

import tech.streamfusion.format.EncodeFormat;
import tech.streamfusion.kafka.ConfluentSchemaRegistry;
import java.io.IOException;

/**
 * Registers the sink's derived writer schema under its subject at sink open and completes the
 * encode options with the returned id — the id the native encoder frames every message with, just
 * as Flink's serializer frames the id its registry client returns. Registration failure (an
 * incompatible schema, an unreachable registry) fails the job the way Flink's serializer fails its
 * first record.
 */
final class ConfluentSchemaRegistration implements EncodeFormat.OpenCompletion {

  private static final long serialVersionUID = 1L;

  private final ConfluentSchemaRegistry registry;
  private final String subject;
  private final String schemaJson;

  ConfluentSchemaRegistration(
      ConfluentSchemaRegistry registry, String subject, String schemaJson) {
    this.registry = registry;
    this.subject = subject;
    this.schemaJson = schemaJson;
  }

  @Override
  public String complete(String options) throws IOException {
    return options + "schema-id=" + registry.register(subject, schemaJson) + "\n";
  }
}

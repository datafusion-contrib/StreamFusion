package tech.streamfusion.format.avroconfluent;

import tech.streamfusion.format.EncodeFormat;
import tech.streamfusion.format.FormatCodes;
import tech.streamfusion.format.NativeFormatContext;
import tech.streamfusion.format.NativeFormatProvider;
import tech.streamfusion.format.NativeMessageDecoderFactory;
import tech.streamfusion.format.avro.AvroDecodeGate;
import tech.streamfusion.format.avro.AvroEncodeGate;
import tech.streamfusion.kafka.ConfluentSchemaRegistry;
import java.util.Map;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.types.logical.RowType;

/** Native provider for Flink's {@code avro-confluent} format. */
public final class AvroConfluentFormatProvider implements NativeFormatProvider {

  @Override
  public String formatIdentifier() {
    return "avro-confluent";
  }

  @Override
  public boolean honorsProjection() {
    return true;
  }

  @Override
  public boolean supportsIgnoreParseErrors() {
    return false;
  }

  @Override
  public boolean supports(NativeFormatContext context) {
    // Flink's avro-confluent factory has no timestamp-mapping option: it is hard-wired to the
    // legacy mapping, so the gate always checks the legacy derivation.
    return !context.ignoreParseErrors()
        && ConfluentSchemaRegistry.fromOptions(context.options()) != null
        && AvroDecodeGate.supports(context.writerType(), true);
  }

  @Override
  public NativeMessageDecoderFactory createDecoder(NativeFormatContext context) {
    ConfluentSchemaRegistry registry = ConfluentSchemaRegistry.fromOptions(context.options());
    String readerSchema = AvroSchemaConverter.convertToSchema(context.outputType().copy(false)).toString();
    return () -> new RegistryAvroDecoder(registry, readerSchema, false);
  }

  @Override
  public EncodeFormat encodeFormat(NativeFormatContext context) {
    RowType rowType = context.writerType();
    Map<String, String> options = context.options();
    ConfluentSchemaRegistry registry = ConfluentSchemaRegistry.fromFormatOptions(options);
    // The subject is required for serialization; the sink translator auto-completes it from the
    // topic under the fallback spelling exactly like Flink's Kafka factories, so a missing one
    // means an option shape (multiple topics) the translator already declined.
    String subject = options.getOrDefault("subject", options.get("schema-registry.subject"));
    // Flink's avro-confluent serializer hard-wires the legacy timestamp mapping, like the decode.
    if (registry == null || subject == null || !AvroEncodeGate.supports(rowType, true)) {
      return null;
    }
    String schema = AvroEncodeGate.derivedSchema(rowType, true);
    return EncodeFormat.resolved(
        FormatCodes.AVRO_CONFLUENT,
        "avro-schema=" + schema + "\n",
        new ConfluentSchemaRegistration(registry, subject, schema));
  }
}

package tech.streamfusion.format.avroconfluent;

import tech.streamfusion.format.EncodeFormat;
import tech.streamfusion.format.FormatCodes;
import tech.streamfusion.format.NativeFormatContext;
import tech.streamfusion.format.NativeFormatProvider;
import tech.streamfusion.format.NativeMessageDecoderFactory;
import tech.streamfusion.format.avro.AvroDecodeGate;
import tech.streamfusion.format.avro.AvroEncodeGate;
import tech.streamfusion.kafka.ConfluentSchemaRegistry;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.types.logical.RowType;

/**
 * Native provider for Flink's {@code debezium-avro-confluent} format: the Debezium changelog
 * envelope with Confluent-framed Avro bodies. The reader schema is derived from the envelope row
 * type {@code ROW<before <physical>.nullable(), after <physical>.nullable(), op STRING>} — the
 * exact derivation Flink's deserializer performs — so the plan-time gate and the runtime registry
 * lookup both operate on the envelope, and the native decode fans the envelope out to changelog
 * rows. Like {@code avro-confluent}, the mapping is hard-wired legacy and the registry options
 * gate through the shared registry accessor (header-only auth is translated; SSL, client
 * properties, and the other credential sources fall back); an explicit {@code schema} option
 * (Flink validates it against the envelope) also falls back. The format defines no {@code
 * ignore-parse-errors}, so a corrupt message fails the job on both engines.
 */
public final class DebeziumAvroConfluentFormatProvider implements NativeFormatProvider {

  @Override
  public String formatIdentifier() {
    return "debezium-avro-confluent";
  }

  @Override
  public boolean honorsProjection() {
    return false;
  }

  @Override
  public boolean supportsIgnoreParseErrors() {
    return false;
  }

  @Override
  public boolean supports(NativeFormatContext context) {
    return !context.ignoreParseErrors()
        && ConfluentSchemaRegistry.fromOptions(context.options()) != null
        && AvroDecodeGate.supports(envelopeType(context.outputType()), true);
  }

  @Override
  public NativeMessageDecoderFactory createDecoder(NativeFormatContext context) {
    ConfluentSchemaRegistry registry = ConfluentSchemaRegistry.fromOptions(context.options());
    String readerSchema =
        AvroSchemaConverter.convertToSchema(envelopeType(context.outputType()).copy(false))
            .toString();
    return () -> new RegistryAvroDecoder(registry, readerSchema, true);
  }

  /** Flink's Debezium envelope row type over the table's physical row. */
  static RowType envelopeType(RowType physical) {
    return DebeziumAvroEnvelope.rowType(physical);
  }

  /**
   * The sink side: Flink's serializer wraps each changelog row in the same envelope and
   * Confluent-frames the Avro record, registering the ENVELOPE schema under the subject at open.
   * The envelope row type Flink derives has a NULLABLE root, so the registered schema is a
   * {@code [null, record]} union and Flink's datum writer emits the union's branch marker before
   * every record — the native encoder serializes against the record branch and splices the marker
   * into the frame. The timestamp mapping is hard-wired legacy, like every registry Avro path.
   */
  @Override
  public EncodeFormat encodeFormat(NativeFormatContext context) {
    RowType envelope = envelopeType(context.writerType());
    java.util.Map<String, String> options = context.options();
    ConfluentSchemaRegistry registry = ConfluentSchemaRegistry.fromFormatOptions(options);
    String subject = options.getOrDefault("subject", options.get("schema-registry.subject"));
    if (registry == null || subject == null || !AvroEncodeGate.supports(envelope, true)) {
      return null;
    }
    String unionSchema = AvroEncodeGate.derivedSchema(envelope, true);
    String recordSchema =
        AvroEncodeGate.derivedSchema(DebeziumAvroEnvelope.recordBranch(context.writerType()), true);
    return EncodeFormat.resolved(
        FormatCodes.DEBEZIUM_AVRO_CONFLUENT,
        "avro-schema=" + recordSchema + "\n",
        new ConfluentSchemaRegistration(registry, subject, unionSchema));
  }
}

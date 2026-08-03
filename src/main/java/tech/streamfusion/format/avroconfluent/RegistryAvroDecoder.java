package tech.streamfusion.format.avroconfluent;

import tech.streamfusion.format.NativeSchemaMessageDecoder;
import tech.streamfusion.format.avro.NativeAvroFormat;
import tech.streamfusion.kafka.ConfluentSchemaRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.avro.Schema;

/**
 * Registry-driven decoder shared by the {@code avro-confluent} and {@code debezium-avro-confluent}
 * providers: the first time a batch carries an unseen schema id, the writer schema is fetched from
 * the registry (the same lazy per-id lookup Flink's deserializer makes), aligned onto the reader's
 * record names, and registered with the native store. Only the native decoder construction differs
 * — a plain record decode versus the Debezium envelope fan-out.
 */
final class RegistryAvroDecoder extends NativeSchemaMessageDecoder {

  private final ConfluentSchemaRegistry registry;
  private final String readerSchemaText;
  private final boolean debeziumEnvelope;
  private Set<Integer> registeredSchemaIds;
  private Schema readerSchema;

  RegistryAvroDecoder(
      ConfluentSchemaRegistry registry, String readerSchemaText, boolean debeziumEnvelope) {
    this.registry = registry;
    this.readerSchemaText = readerSchemaText;
    this.debeziumEnvelope = debeziumEnvelope;
  }

  @Override
  protected long createHandle(long schemaArrayAddress, long schemaAddress) {
    registeredSchemaIds = new HashSet<>();
    readerSchema = new Schema.Parser().parse(readerSchemaText);
    return debeziumEnvelope
        ? NativeAvroFormat.createDebeziumDecoder(readerSchemaText, schemaArrayAddress, schemaAddress)
        : NativeAvroFormat.createDecoder(
            true, "", readerSchemaText, schemaArrayAddress, schemaAddress);
  }

  @Override
  public void beforeDecode(VarBinaryVector bodies, int count) {
    for (int i = 0; i < count; i++) {
      byte[] message = bodies.get(i);
      if (message == null || message.length < 5 || message[0] != 0) {
        continue;
      }
      int id =
          ((message[1] & 0xff) << 24)
              | ((message[2] & 0xff) << 16)
              | ((message[3] & 0xff) << 8)
              | (message[4] & 0xff);
      if (registeredSchemaIds.add(id)) {
        try {
          Schema writer = registry.fetchWriterSchema(id);
          NativeAvroFormat.registerWriterSchema(
              handle, id, ConfluentSchemaRegistry.alignedToReader(writer, readerSchema).toString());
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }
  }

  @Override
  public void decodeInto(long inArray, long inSchema, long outArray, long outSchema) {
    NativeAvroFormat.decodeInto(handle, inArray, inSchema, outArray, outSchema);
  }

  @Override
  public void close() {
    if (handle != 0) {
      NativeAvroFormat.closeDecoder(handle);
      handle = 0;
    }
  }
}

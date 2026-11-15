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
 * — a plain record decode versus the Debezium envelope fan-out. A keyed table whose key is also
 * Confluent-framed resolves its keys the same way, through the key format's own registry and
 * reader schema, into the key decoder's separate store.
 */
final class RegistryAvroDecoder extends NativeSchemaMessageDecoder {

  private final ConfluentSchemaRegistry registry;
  private final String readerSchemaText;
  private final boolean debeziumEnvelope;
  private final String formatOptions;
  /** The key format's registry and reader schema; null unless the key is Confluent-framed. */
  private final ConfluentSchemaRegistry keyRegistry;
  private final String keyReaderSchemaText;
  private Set<Integer> registeredSchemaIds;
  private Set<Integer> registeredKeySchemaIds;
  private Schema readerSchema;
  private Schema keyReaderSchema;

  RegistryAvroDecoder(
      ConfluentSchemaRegistry registry,
      String readerSchemaText,
      boolean debeziumEnvelope,
      String formatOptions,
      ConfluentSchemaRegistry keyRegistry,
      String keyReaderSchemaText) {
    this.registry = registry;
    this.readerSchemaText = readerSchemaText;
    this.debeziumEnvelope = debeziumEnvelope;
    this.formatOptions = formatOptions;
    this.keyRegistry = keyRegistry;
    this.keyReaderSchemaText = keyReaderSchemaText;
  }

  @Override
  protected long createHandle(long schemaArrayAddress, long schemaAddress) {
    registeredSchemaIds = new HashSet<>();
    registeredKeySchemaIds = new HashSet<>();
    readerSchema = new Schema.Parser().parse(readerSchemaText);
    if (keyRegistry != null) {
      keyReaderSchema = new Schema.Parser().parse(keyReaderSchemaText);
    }
    return debeziumEnvelope
        ? NativeAvroFormat.createDebeziumDecoder(
            readerSchemaText, schemaArrayAddress, schemaAddress, formatOptions)
        : NativeAvroFormat.createDecoder(
            true, "", readerSchemaText, schemaArrayAddress, schemaAddress, formatOptions);
  }

  @Override
  public void beforeDecode(VarBinaryVector bodies, int count) {
    for (int i = 0; i < count; i++) {
      registerWriterSchemaOf(bodies.get(i));
    }
  }

  @Override
  public void beforeDecode(byte[][] bodies, int count) {
    for (int i = 0; i < count; i++) {
      registerWriterSchemaOf(bodies[i]);
    }
  }

  @Override
  public void beforeDecodeKeys(VarBinaryVector keys, int count) {
    for (int i = 0; keyRegistry != null && i < count; i++) {
      registerKeyWriterSchemaOf(keys.get(i));
    }
  }

  @Override
  public void beforeDecodeKeys(byte[][] keys, int count) {
    for (int i = 0; keyRegistry != null && i < count; i++) {
      registerKeyWriterSchemaOf(keys[i]);
    }
  }

  private void registerWriterSchemaOf(byte[] message) {
    Integer id = schemaId(message);
    if (id != null && registeredSchemaIds.add(id)) {
      NativeAvroFormat.registerWriterSchema(handle, id, aligned(registry, id, readerSchema));
    }
  }

  private void registerKeyWriterSchemaOf(byte[] key) {
    Integer id = schemaId(key);
    if (id != null && registeredKeySchemaIds.add(id)) {
      NativeAvroFormat.registerKeyWriterSchema(
          handle, id, aligned(keyRegistry, id, keyReaderSchema));
    }
  }

  /** The Confluent frame's schema id, or null for a message the native decode rejects itself. */
  private static Integer schemaId(byte[] message) {
    if (message == null || message.length < 5 || message[0] != 0) {
      return null;
    }
    return ((message[1] & 0xff) << 24)
        | ((message[2] & 0xff) << 16)
        | ((message[3] & 0xff) << 8)
        | (message[4] & 0xff);
  }

  private static String aligned(ConfluentSchemaRegistry registry, int id, Schema reader) {
    try {
      return ConfluentSchemaRegistry.alignedToReader(registry.fetchWriterSchema(id), reader)
          .toString();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void decodeInto(long inArray, long inSchema, long outArray, long outSchema) {
    NativeAvroFormat.decodeInto(handle, inArray, inSchema, outArray, outSchema);
  }

  @Override
  public void decodeRecordsInto(
      long slabAddress,
      long slabLength,
      long[] layout,
      int count,
      String topic,
      int partition,
      long outArray,
      long outSchema) {
    NativeAvroFormat.decodeRecordsInto(
        handle, slabAddress, slabLength, layout, count, topic, partition, outArray, outSchema);
  }

  @Override
  public void close() {
    if (handle != 0) {
      NativeAvroFormat.closeDecoder(handle);
      handle = 0;
    }
  }
}

package tech.streamfusion.kafka;

import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import tech.streamfusion.format.EncodeFormat;
import tech.streamfusion.format.EncodedMessages;
import tech.streamfusion.format.NativeMessageEncoder;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Kafka owns key/value composition; installed formats own the byte encoders and their handles. */
final class KafkaMessageEncoder implements AutoCloseable {
  private final NativeMessageEncoder keys;
  private final NativeMessageEncoder values;
  private final boolean upsert;
  private boolean closed;

  KafkaMessageEncoder(
      EncodeFormat valueFormat,
      EncodeFormat keyFormat,
      String[] types,
      String[] names,
      int[] keyFields,
      int[] valueFields,
      boolean upsert)
      throws Exception {
    this.upsert = upsert;
    NativeMessageEncoder acquiredKeys = null;
    try {
      if (keyFields.length != 0) acquiredKeys = keyFormat.createEncoder(types, names, keyFields);
      values = valueFormat.createEncoder(types, names, valueFields);
    } catch (Exception | Error failure) {
      if (acquiredKeys != null) {
        try {
          acquiredKeys.close();
        } catch (Exception | Error cleanup) {
          failure.addSuppressed(cleanup);
        }
      }
      throw failure;
    }
    keys = acquiredKeys;
  }

  NativeKafkaEncodedBatch encode(VectorSchemaRoot input) throws Exception {
    if (closed) throw new IllegalStateException("Kafka encoder is closed");
    EncodedMessages encodedKeys = keys == null ? null : keys.encode(input);
    EncodedMessages encodedValues = values.encode(input);
    int rows = input.getRowCount();
    if (encodedValues.size() != rows || (encodedKeys != null && encodedKeys.size() != rows))
      throw new IllegalStateException("format encoder changed the input row count");
    int[] lengths = encodedValues.lengths.clone();
    if (upsert) {
      var kinds = (TinyIntVector) input.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
      if (kinds != null)
        for (int i = 0; i < rows; i++) if (kinds.get(i) == 1 || kinds.get(i) == 3) lengths[i] = -1;
    }
    int[] keyOffsets = encodedKeys == null ? new int[rows] : encodedKeys.offsets;
    int[] keyLengths = encodedKeys == null ? new int[rows] : encodedKeys.lengths;
    if (encodedKeys == null) java.util.Arrays.fill(keyLengths, -1);
    return new NativeKafkaEncodedBatch(
        encodedKeys == null ? new byte[0] : encodedKeys.bytes,
        keyOffsets,
        keyLengths,
        encodedValues.bytes,
        encodedValues.offsets,
        lengths);
  }

  @Override
  public void close() throws Exception {
    if (closed) return;
    closed = true;
    org.apache.flink.util.IOUtils.closeAll(java.util.Arrays.asList(values, keys), Throwable.class);
  }
}

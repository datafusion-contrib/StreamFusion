package tech.streamfusion.kafka;

/** Contiguous native-encoded Kafka keys and values with one offset/length pair per record. */
public final class NativeKafkaEncodedBatch {

  private final byte[] keyBytes;
  private final int[] keyOffsets;
  private final int[] keyLengths;
  private final byte[] valueBytes;
  private final int[] valueOffsets;
  private final int[] valueLengths;

  public NativeKafkaEncodedBatch(
      byte[] keyBytes,
      int[] keyOffsets,
      int[] keyLengths,
      byte[] valueBytes,
      int[] valueOffsets,
      int[] valueLengths) {
    this.keyBytes = keyBytes;
    this.keyOffsets = keyOffsets;
    this.keyLengths = keyLengths;
    this.valueBytes = valueBytes;
    this.valueOffsets = valueOffsets;
    this.valueLengths = valueLengths;
    if (keyOffsets.length != valueOffsets.length
        || keyLengths.length != valueOffsets.length
        || valueLengths.length != valueOffsets.length) {
      throw new IllegalArgumentException("Kafka batch metadata has inconsistent row counts");
    }
  }

  public int size() {
    return valueOffsets.length;
  }

  public long serializedBytes(int index) {
    return (long) Math.max(0, keyLengths[index]) + Math.max(0, valueLengths[index]);
  }

  public PreSerializedKafkaRecord record(int index) {
    return new PreSerializedKafkaRecord(
        keyBytes,
        keyOffsets[index],
        keyLengths[index],
        valueBytes,
        valueOffsets[index],
        valueLengths[index]);
  }
}

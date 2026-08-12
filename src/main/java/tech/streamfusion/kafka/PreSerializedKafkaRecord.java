package tech.streamfusion.kafka;

import java.io.Serializable;
import java.util.Arrays;

/** Final Kafka key/value bytes produced by one native batch serialization pass. */
public final class PreSerializedKafkaRecord implements Serializable {

  private final byte[] keyBytes;
  private final int keyOffset;
  private final int keyLength;
  private final byte[] valueBytes;
  private final int valueOffset;
  private final int valueLength;

  public PreSerializedKafkaRecord(byte[] key, byte[] value) {
    this(key, 0, key == null ? -1 : key.length, value, 0, value == null ? -1 : value.length);
  }

  PreSerializedKafkaRecord(
      byte[] keyBytes,
      int keyOffset,
      int keyLength,
      byte[] valueBytes,
      int valueOffset,
      int valueLength) {
    this.keyBytes = keyBytes;
    this.keyOffset = keyOffset;
    this.keyLength = keyLength;
    this.valueBytes = valueBytes;
    this.valueOffset = valueOffset;
    this.valueLength = valueLength;
  }

  public byte[] key() {
    return materialize(keyBytes, keyOffset, keyLength);
  }

  public byte[] value() {
    return materialize(valueBytes, valueOffset, valueLength);
  }

  private static byte[] materialize(byte[] bytes, int offset, int length) {
    if (length < 0) {
      return null;
    }
    if (offset == 0 && length == bytes.length) {
      return bytes;
    }
    return Arrays.copyOfRange(bytes, offset, offset + length);
  }
}

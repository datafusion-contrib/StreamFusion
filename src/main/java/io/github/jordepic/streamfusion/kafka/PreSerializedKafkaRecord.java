package io.github.jordepic.streamfusion.kafka;

import java.io.Serializable;

/** Final Kafka key/value bytes produced by one native batch serialization pass. */
public final class PreSerializedKafkaRecord implements Serializable {

  private final byte[] key;
  private final byte[] value;

  public PreSerializedKafkaRecord(byte[] key, byte[] value) {
    this.key = key;
    this.value = value;
  }

  public byte[] key() {
    return key;
  }

  public byte[] value() {
    return value;
  }
}

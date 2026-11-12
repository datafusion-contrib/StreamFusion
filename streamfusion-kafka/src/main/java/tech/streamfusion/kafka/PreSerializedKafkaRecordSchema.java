package tech.streamfusion.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;

/** Hands native-serialized values to Flink's Kafka writer without another serialization pass. */
public final class PreSerializedKafkaRecordSchema
    extends tech.streamfusion.kafka.compat.KafkaRecordSchemaCompat {

  private final String topic;

  public PreSerializedKafkaRecordSchema(String topic) {
    this.topic = topic;
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(
      PreSerializedKafkaRecord record, KafkaSinkContext context, Long timestamp) {
    return new ProducerRecord<>(topic, record.key(), record.value());
  }

  @Override
  protected String topic() {
    return topic;
  }
}

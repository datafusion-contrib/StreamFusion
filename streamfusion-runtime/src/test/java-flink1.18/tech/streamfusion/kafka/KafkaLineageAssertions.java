package tech.streamfusion.kafka;

final class KafkaLineageAssertions {
  private KafkaLineageAssertions() {}

  static void assertTopic(PreSerializedKafkaRecordSchema schema, String topic) {
    // Dataset lineage is absent from the released Kafka 3.2.0-1.18 connector.
  }
}

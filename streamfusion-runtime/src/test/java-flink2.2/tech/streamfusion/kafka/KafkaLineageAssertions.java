package tech.streamfusion.kafka;

final class KafkaLineageAssertions {
  private KafkaLineageAssertions() {}

  static void assertTopic(PreSerializedKafkaRecordSchema schema, String topic) {
    org.junit.jupiter.api.Assertions.assertEquals(
        java.util.List.of(topic),
        schema.getKafkaDatasetFacet().orElseThrow().getTopicIdentifier().getTopics());
  }
}

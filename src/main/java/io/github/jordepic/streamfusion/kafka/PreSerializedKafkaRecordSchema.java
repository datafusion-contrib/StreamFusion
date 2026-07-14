package io.github.jordepic.streamfusion.kafka;

import java.util.List;
import java.util.Optional;
import org.apache.flink.connector.kafka.lineage.DefaultKafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.DefaultKafkaDatasetIdentifier;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacetProvider;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Hands native-serialized values to Flink's Kafka writer without another serialization pass. */
public final class PreSerializedKafkaRecordSchema
    implements KafkaRecordSerializationSchema<PreSerializedKafkaRecord>, KafkaDatasetFacetProvider {

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
  public Optional<KafkaDatasetFacet> getKafkaDatasetFacet() {
    return Optional.of(
        new DefaultKafkaDatasetFacet(DefaultKafkaDatasetIdentifier.ofTopics(List.of(topic))));
  }
}

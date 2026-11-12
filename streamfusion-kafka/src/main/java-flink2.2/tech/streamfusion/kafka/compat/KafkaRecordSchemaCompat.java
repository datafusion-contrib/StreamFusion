package tech.streamfusion.kafka.compat;

import java.util.List;
import java.util.Optional;
import org.apache.flink.connector.kafka.lineage.*;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import tech.streamfusion.kafka.PreSerializedKafkaRecord;

/** Retains connector lineage on releases that provide that SPI. */
public abstract class KafkaRecordSchemaCompat
    implements KafkaRecordSerializationSchema<PreSerializedKafkaRecord>, KafkaDatasetFacetProvider {
  protected abstract String topic();

  @Override
  public Optional<KafkaDatasetFacet> getKafkaDatasetFacet() {
    return Optional.of(
        new DefaultKafkaDatasetFacet(DefaultKafkaDatasetIdentifier.ofTopics(List.of(topic()))));
  }
}

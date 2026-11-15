package tech.streamfusion.kafka.compat;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import tech.streamfusion.kafka.PreSerializedKafkaRecord;

/** Retains connector lineage on releases that provide that SPI. */
public abstract class KafkaRecordSchemaCompat
    implements KafkaRecordSerializationSchema<PreSerializedKafkaRecord> {
  protected abstract String topic();
}

package io.github.jordepic.streamfusion.kafka;

import io.github.jordepic.streamfusion.operator.NativeBytesDecodeOperator;
import io.github.jordepic.streamfusion.operator.NullableBytesTypeInformation;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * The keyed shallow-decode edge: each record's key and value bytes travel as one
 * {@link NativeBytesDecodeOperator#frame} element, so the chained edge reuses the plain
 * nullable-bytes serializer and the keyed decode operator splits the frame back into its two
 * binary columns.
 */
public final class KeyedKafkaBytesDeserialization
    implements KafkaRecordDeserializationSchema<byte[]> {

  private static final long serialVersionUID = 1L;

  @Override
  public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<byte[]> out) {
    out.collect(NativeBytesDecodeOperator.frame(record.key(), record.value()));
  }

  @Override
  public TypeInformation<byte[]> getProducedType() {
    return NullableBytesTypeInformation.INSTANCE;
  }
}

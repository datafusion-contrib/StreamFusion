package io.github.jordepic.streamfusion.kafka;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import java.io.IOException;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumStateSerializer;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumerator;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.types.logical.RowType;

/**
 * A FLIP-27 Kafka source that emits {@link ArrowBatch}es decoded natively. It reuses the standard
 * connector's {@link KafkaSourceEnumerator} (partition discovery + offset resolution + split assignment)
 * and its split / enumerator-state serializers verbatim — that coordination is on the JobManager, not
 * the hot path — and swaps only the per-subtask reader for the native rdkafka one
 * ({@link NativeKafkaSourceReader} over {@link NativeKafkaSplitReader}). The enumerator hands concrete
 * starting offsets to the readers, which assign+seek natively, so checkpoint/restore semantics match
 * Flink's Kafka source.
 *
 * <p>Constructed by the planner ({@code NativeKafkaSourceExecNode}) only when the table's consumer
 * settings translate to librdkafka and the format is one the native decoder supports; otherwise the
 * planner leaves Flink's own Kafka source in place (the fallback).
 */
public final class NativeKafkaSource
    implements Source<ArrowBatch, KafkaPartitionSplit, KafkaSourceEnumState> {

  private static final long serialVersionUID = 1L;

  private final KafkaSubscriber subscriber;
  private final OffsetsInitializer startingOffsets;
  private final OffsetsInitializer stoppingOffsets;
  private final Boundedness boundedness;
  private final Properties props;
  private final String[] configKeys;
  private final String[] configValues;
  private final int format;
  private final RowType outputType;
  private final String avroSchema;
  private final String readerAvroSchema;
  private final int schemaId;
  private final byte[] protoDescriptor;
  private final String protoMessageName;
  private final int maxRecords;
  private final long pollTimeoutMillis;
  private final int rowtimeIndex;

  public NativeKafkaSource(
      KafkaSubscriber subscriber,
      OffsetsInitializer startingOffsets,
      OffsetsInitializer stoppingOffsets,
      Boundedness boundedness,
      Properties props,
      String[] configKeys,
      String[] configValues,
      int format,
      RowType outputType,
      String avroSchema,
      String readerAvroSchema,
      int schemaId,
      byte[] protoDescriptor,
      String protoMessageName,
      int maxRecords,
      long pollTimeoutMillis,
      int rowtimeIndex) {
    this.subscriber = subscriber;
    this.startingOffsets = startingOffsets;
    this.stoppingOffsets = stoppingOffsets;
    this.boundedness = boundedness;
    this.props = props;
    // Match KafkaSourceBuilder: a bounded source must disable periodic partition discovery, otherwise
    // the enumerator never signals no-more-splits and the job never terminates. (Default is 5 minutes.)
    if (boundedness == Boundedness.BOUNDED) {
      props.setProperty("partition.discovery.interval.ms", "-1");
    }
    this.configKeys = configKeys;
    this.configValues = configValues;
    this.format = format;
    this.outputType = outputType;
    this.avroSchema = avroSchema;
    this.readerAvroSchema = readerAvroSchema;
    this.schemaId = schemaId;
    this.protoDescriptor = protoDescriptor;
    this.protoMessageName = protoMessageName;
    this.maxRecords = maxRecords;
    this.pollTimeoutMillis = pollTimeoutMillis;
    this.rowtimeIndex = rowtimeIndex;
  }

  @Override
  public Boundedness getBoundedness() {
    return boundedness;
  }

  @Override
  public SourceReader<ArrowBatch, KafkaPartitionSplit> createReader(SourceReaderContext context) {
    Supplier<SplitReader<NativeKafkaRecord, KafkaPartitionSplit>> splitReaderSupplier =
        () ->
            new NativeKafkaSplitReader(
                configKeys,
                configValues,
                format,
                outputType,
                avroSchema,
                readerAvroSchema,
                schemaId,
                protoDescriptor,
                protoMessageName,
                maxRecords,
                pollTimeoutMillis,
                rowtimeIndex);
    return new NativeKafkaSourceReader(
        splitReaderSupplier, new NativeKafkaRecordEmitter(), toConfiguration(props), context);
  }

  @Override
  public SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> createEnumerator(
      SplitEnumeratorContext<KafkaPartitionSplit> enumContext) {
    return new KafkaSourceEnumerator(
        subscriber, startingOffsets, stoppingOffsets, props, enumContext, boundedness);
  }

  @Override
  public SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> restoreEnumerator(
      SplitEnumeratorContext<KafkaPartitionSplit> enumContext, KafkaSourceEnumState checkpoint)
      throws IOException {
    return new KafkaSourceEnumerator(
        subscriber, startingOffsets, stoppingOffsets, props, enumContext, boundedness, checkpoint);
  }

  @Override
  public SimpleVersionedSerializer<KafkaPartitionSplit> getSplitSerializer() {
    return new KafkaPartitionSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<KafkaSourceEnumState> getEnumeratorCheckpointSerializer() {
    return new KafkaSourceEnumStateSerializer();
  }

  private static Configuration toConfiguration(Properties props) {
    Configuration config = new Configuration();
    props.stringPropertyNames().forEach(key -> config.setString(key, props.getProperty(key)));
    return config;
  }
}

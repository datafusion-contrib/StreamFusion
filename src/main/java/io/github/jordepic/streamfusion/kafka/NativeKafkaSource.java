package io.github.jordepic.streamfusion.kafka;

import io.github.jordepic.streamfusion.format.NativeMessageDecoderFactory;
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
 * FLIP-27 Kafka source that consumes with a native rdkafka reader and emits Arrow batches. The planner
 * supplies the table's format decoder through the format-provider SPI and the split reader runs it on
 * the fetch thread, so the emitted batches are already typed and the decode overlaps the task thread's
 * operators; connector and format DSOs share Arrow C Data rather than private Rust handles. Without a
 * decoder the source emits the raw binary {@code body} batches.
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
  private final int maxRecords;
  private final long pollTimeoutMillis;
  private final NativeMessageDecoderFactory decoderFactory;
  private final RowType decodedType;
  // Rowtime column in the decoded batch for a watermarked table, or -1: the split reader stamps each
  // batch's max rowtime as its record timestamp for the per-split source watermarks.
  private final int rowtimeIndex;

  public NativeKafkaSource(
      KafkaSubscriber subscriber,
      OffsetsInitializer startingOffsets,
      OffsetsInitializer stoppingOffsets,
      Boundedness boundedness,
      Properties props,
      String[] configKeys,
      String[] configValues,
      int maxRecords,
      long pollTimeoutMillis,
      NativeMessageDecoderFactory decoderFactory,
      RowType decodedType,
      int rowtimeIndex) {
    this.subscriber = subscriber;
    this.startingOffsets = startingOffsets;
    this.stoppingOffsets = stoppingOffsets;
    this.boundedness = boundedness;
    this.props = props;
    if (boundedness == Boundedness.BOUNDED) {
      props.setProperty("partition.discovery.interval.ms", "-1");
    }
    this.configKeys = configKeys;
    this.configValues = configValues;
    this.maxRecords = maxRecords;
    this.pollTimeoutMillis = pollTimeoutMillis;
    this.decoderFactory = decoderFactory;
    this.decodedType = decodedType;
    this.rowtimeIndex = rowtimeIndex;
  }

  @Override
  public Boundedness getBoundedness() {
    return boundedness;
  }

  @Override
  public SourceReader<ArrowBatch, KafkaPartitionSplit> createReader(SourceReaderContext context) {
    NativeKafkaSourceMetrics metrics = new NativeKafkaSourceMetrics(context.metricGroup());
    java.util.LinkedHashMap<String, String> readerConfig = new java.util.LinkedHashMap<>();
    for (int i = 0; i < configKeys.length; i++) {
      readerConfig.put(configKeys[i], configValues[i]);
    }
    readerConfig.put(
        "client.id",
        props.getProperty("client.id.prefix") + "-" + context.getIndexOfSubtask());
    String[] readerConfigKeys = readerConfig.keySet().toArray(new String[0]);
    String[] readerConfigValues =
        java.util.Arrays.stream(readerConfigKeys).map(readerConfig::get).toArray(String[]::new);
    Supplier<SplitReader<NativeKafkaRecord, KafkaPartitionSplit>> splitReaderSupplier =
        () ->
            new NativeKafkaSplitReader(
                readerConfigKeys,
                readerConfigValues,
                maxRecords,
                pollTimeoutMillis,
                decoderFactory,
                decodedType,
                rowtimeIndex,
                metrics);
    Configuration configuration = toConfiguration(props);
    return new NativeKafkaSourceReader(
        new NativeKafkaSourceFetcherManager(splitReaderSupplier, configuration),
        new NativeKafkaRecordEmitter(),
        configuration,
        context,
        metrics);
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

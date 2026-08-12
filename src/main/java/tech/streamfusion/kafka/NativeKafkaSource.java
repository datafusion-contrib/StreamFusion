package tech.streamfusion.kafka;

import tech.streamfusion.format.NativeMessageDecoderFactory;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeSourceRecord;
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
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.types.logical.RowType;

/**
 * Split-aware native-decoding Kafka source. Flink's Kafka source still owns enumeration,
 * assignment, offsets, authentication, and KafkaConsumer behavior; only the task-side byte decode
 * is replaced, before the partition-local {@code SourceOutput} watermark boundary.
 */
public final class NativeKafkaSource
    implements Source<ArrowBatch, KafkaPartitionSplit, KafkaSourceEnumState> {

  private static final long serialVersionUID = 1L;

  private final KafkaSource<byte[]> delegate;
  private final Properties properties;
  private final RowType outputType;
  private final NativeMessageDecoderFactory decoderFactory;
  private final boolean keyed;
  private final int rowtimeIndex;

  public NativeKafkaSource(
      KafkaSource<byte[]> delegate,
      Properties properties,
      RowType outputType,
      NativeMessageDecoderFactory decoderFactory,
      boolean keyed,
      int rowtimeIndex) {
    this.delegate = delegate;
    this.properties = properties;
    this.outputType = outputType;
    this.decoderFactory = decoderFactory;
    this.keyed = keyed;
    this.rowtimeIndex = rowtimeIndex;
  }

  @Override
  public Boundedness getBoundedness() {
    return delegate.getBoundedness();
  }

  @Override
  public SourceReader<ArrowBatch, KafkaPartitionSplit> createReader(SourceReaderContext context) {
    KafkaSourceReaderMetrics metrics = new KafkaSourceReaderMetrics(context.metricGroup());
    Supplier<SplitReader<NativeSourceRecord, KafkaPartitionSplit>> reader =
        () ->
            new NativeKafkaSplitReader(
                properties,
                context,
                metrics,
                outputType,
                decoderFactory,
                keyed,
                rowtimeIndex);
    return new NativeKafkaSourceReader(
        reader, new NativeKafkaRecordEmitter(), configuration(), context, metrics);
  }

  @Override
  public SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> createEnumerator(
      SplitEnumeratorContext<KafkaPartitionSplit> context) {
    return delegate.createEnumerator(context);
  }

  @Override
  public SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> restoreEnumerator(
      SplitEnumeratorContext<KafkaPartitionSplit> context, KafkaSourceEnumState checkpoint)
      throws IOException {
    return delegate.restoreEnumerator(context, checkpoint);
  }

  @Override
  public SimpleVersionedSerializer<KafkaPartitionSplit> getSplitSerializer() {
    return delegate.getSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<KafkaSourceEnumState> getEnumeratorCheckpointSerializer() {
    return delegate.getEnumeratorCheckpointSerializer();
  }

  private Configuration configuration() {
    Configuration config = new Configuration();
    properties.stringPropertyNames()
        .forEach(key -> config.setString(key, properties.getProperty(key)));
    return config;
  }
}

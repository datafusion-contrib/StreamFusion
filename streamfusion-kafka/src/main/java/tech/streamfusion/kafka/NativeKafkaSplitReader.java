package tech.streamfusion.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics;
import org.apache.flink.connector.kafka.source.reader.KafkaPartitionSplitReader;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import tech.streamfusion.format.NativeMessageDecoderFactory;
import tech.streamfusion.operator.NativeSourceRecord;
import tech.streamfusion.operator.WatermarkExpression;

/**
 * Kafka's stock partition reader with a split-local native decode boundary. Each poll is grouped by
 * Kafka partition before decoding, so the resulting Arrow batch remains attached to the same Flink
 * split that supplied its bytes. Flink can therefore run one watermark generator per partition.
 */
final class NativeKafkaSplitReader
    implements SplitReader<NativeSourceRecord, KafkaPartitionSplit> {

  private static final int BATCH_SIZE = 8192;

  private final KafkaPartitionSplitReader delegate;
  private final NativeKafkaBatchDecoder batches;
  private final NativeKafkaDecodeMetrics decodeMetrics;

  NativeKafkaSplitReader(
      java.util.Properties properties,
      SourceReaderContext context,
      KafkaSourceReaderMetrics metrics,
      NativeKafkaDecodeMetrics decodeMetrics,
      RowType outputType,
      NativeMessageDecoderFactory decoderFactory,
      boolean keyed,
      List<String> metadataKeys,
      int rowtimeIndex,
      WatermarkExpression watermarkExpression) {
    this.delegate = new KafkaPartitionSplitReader(properties, context, metrics);
    this.decodeMetrics = decodeMetrics;
    this.batches =
        new NativeKafkaBatchDecoder(
            outputType, decoderFactory, keyed, metadataKeys, rowtimeIndex, watermarkExpression);
  }

  @Override
  public RecordsWithSplitIds<NativeSourceRecord> fetch() throws IOException {
    return decodeFetched(delegate.fetch(), this::decodeCounted);
  }

  private NativeSourceRecord decodeCounted(List<ConsumerRecord<byte[], byte[]>> records)
      throws Exception {
    long start = System.nanoTime();
    NativeSourceRecord decoded = batches.decode(records);
    decodeMetrics.decoded(
        records.size(),
        decoded.batch() == null ? 0 : decoded.batch().rowCount(),
        System.nanoTime() - start);
    return decoded;
  }

  @FunctionalInterface
  interface BatchDecoder {
    NativeSourceRecord decode(List<ConsumerRecord<byte[], byte[]>> records) throws Exception;
  }

  static RecordsWithSplitIds<NativeSourceRecord> decodeFetched(
      RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> raw, BatchDecoder decoder)
      throws IOException {
    RecordsBySplits.Builder<NativeSourceRecord> decoded = new RecordsBySplits.Builder<>();
    try {
      String splitId;
      while ((splitId = raw.nextSplit()) != null) {
        List<ConsumerRecord<byte[], byte[]>> batch = new ArrayList<>(BATCH_SIZE);
        ConsumerRecord<byte[], byte[]> record;
        while ((record = raw.nextRecordFromSplit()) != null) {
          batch.add(record);
          if (batch.size() == BATCH_SIZE) {
            decoded.add(splitId, decoder.decode(batch));
            batch.clear();
          }
        }
        if (!batch.isEmpty()) {
          decoded.add(splitId, decoder.decode(batch));
        }
      }
      decoded.addFinishedSplits(raw.finishedSplits());
      return decoded.build();
    } catch (Exception | Error failure) {
      RecordsWithSplitIds<NativeSourceRecord> abandoned = decoded.build();
      while (abandoned.nextSplit() != null) {
        NativeSourceRecord record;
        while ((record = abandoned.nextRecordFromSplit()) != null) {
          if (record.batch() != null) {
            try {
              record.batch().root().close();
            } catch (Throwable cleanupFailure) {
              failure.addSuppressed(cleanupFailure);
            }
          }
        }
      }
      if (failure instanceof Error) {
        throw (Error) failure;
      }
      throw new IOException("native Kafka batch decode failed", failure);
    } finally {
      raw.recycle();
    }
  }

  @Override
  public void handleSplitsChanges(SplitsChange<KafkaPartitionSplit> splitsChanges) {
    delegate.handleSplitsChanges(splitsChanges);
  }

  @Override
  public void wakeUp() {
    delegate.wakeUp();
  }

  @Override
  public void close() throws Exception {
    org.apache.flink.util.IOUtils.closeAll(Arrays.asList(delegate, batches), Throwable.class);
  }

  void commitOffsets(
      Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
    delegate.notifyCheckpointComplete(offsets, callback);
  }
}

package tech.streamfusion.kafka;

import tech.streamfusion.format.NativeMessageDecoder;
import tech.streamfusion.format.NativeMessageDecoderFactory;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.NativeSourceRecord;
import tech.streamfusion.operator.NativeSourceWatermarks;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
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

/**
 * Kafka's stock partition reader with a split-local native decode boundary. Each poll is grouped by
 * Kafka partition before decoding, so the resulting Arrow batch remains attached to the same Flink
 * split that supplied its bytes. Flink can therefore run one watermark generator per partition.
 */
final class NativeKafkaSplitReader
    implements SplitReader<NativeSourceRecord, KafkaPartitionSplit> {

  private static final int BATCH_SIZE = 8192;

  private final KafkaPartitionSplitReader delegate;
  private final RowType outputType;
  private final NativeMessageDecoder decoder;
  private final BufferAllocator allocator = NativeAllocator.SHARED;
  private final boolean keyed;
  private final int rowtimeIndex;
  private ArrowBuf decodeSlab;

  NativeKafkaSplitReader(
      java.util.Properties properties,
      SourceReaderContext context,
      KafkaSourceReaderMetrics metrics,
      RowType outputType,
      NativeMessageDecoderFactory decoderFactory,
      boolean keyed,
      int rowtimeIndex) {
    this.delegate = new KafkaPartitionSplitReader(properties, context, metrics);
    this.outputType = outputType;
    this.keyed = keyed;
    this.rowtimeIndex = rowtimeIndex;
    try {
      this.decoder = decoderFactory.create();
      this.decoder.open(allocator, outputType);
    } catch (Exception e) {
      throw new IllegalStateException("native Kafka decoder initialization failed", e);
    }
  }

  @Override
  public RecordsWithSplitIds<NativeSourceRecord> fetch() throws IOException {
    RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> raw = delegate.fetch();
    RecordsBySplits.Builder<NativeSourceRecord> decoded = new RecordsBySplits.Builder<>();
    try {
      String splitId;
      while ((splitId = raw.nextSplit()) != null) {
        List<ConsumerRecord<byte[], byte[]>> batch = new ArrayList<>(BATCH_SIZE);
        ConsumerRecord<byte[], byte[]> record;
        while ((record = raw.nextRecordFromSplit()) != null) {
          batch.add(record);
          if (batch.size() == BATCH_SIZE) {
            decoded.add(splitId, decode(batch));
            batch.clear();
          }
        }
        if (!batch.isEmpty()) {
          decoded.add(splitId, decode(batch));
        }
      }
      decoded.addFinishedSplits(raw.finishedSplits());
      return decoded.build();
    } catch (Exception e) {
      throw new IOException("native Kafka batch decode failed", e);
    } finally {
      raw.recycle();
    }
  }

  private NativeSourceRecord decode(List<ConsumerRecord<byte[], byte[]>> records) throws Exception {
    int count = records.size();
    long bodyBytes = 0;
    long keyBytes = 0;
    for (ConsumerRecord<byte[], byte[]> record : records) {
      byte[] value = record.value();
      if (value != null) {
        bodyBytes += value.length;
      }
      if (keyed) {
        byte[] key = record.key();
        if (key != null) {
          keyBytes += key.length;
        }
      }
    }
    if (decoder.supportsContiguousBytes()) {
      return decodeContiguous(records, bodyBytes, keyBytes);
    }
    try (VarBinaryVector body = new VarBinaryVector("body", allocator);
        VarBinaryVector keys = keyed ? new VarBinaryVector("key", allocator) : null) {
      body.allocateNew(bodyBytes, count);
      if (keys != null) {
        keys.allocateNew(keyBytes, count);
      }
      for (int i = 0; i < count; i++) {
        ConsumerRecord<byte[], byte[]> record = records.get(i);
        set(body, i, record.value());
        if (keys != null) {
          set(keys, i, record.key());
        }
      }
      body.setValueCount(count);
      if (keys != null) {
        keys.setValueCount(count);
      }
      decoder.beforeDecode(body, count);
      try (VectorSchemaRoot input =
              new VectorSchemaRoot(keys == null ? List.of(body) : List.of(keys, body));
          ArrowArray inArray = ArrowArray.allocateNew(allocator);
          ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
          ArrowArray outArray = ArrowArray.allocateNew(allocator);
          ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
        input.setRowCount(count);
        Data.exportVectorSchemaRoot(
            allocator, input, NativeAllocator.DICTIONARIES, inArray, inSchema);
        decoder.decodeInto(
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            outArray.memoryAddress(),
            outSchema.memoryAddress());
        VectorSchemaRoot output =
            Data.importVectorSchemaRoot(
                allocator, outArray, outSchema, NativeAllocator.DICTIONARIES);
        long nextOffset = records.get(count - 1).offset() + 1;
        if (output.getRowCount() == 0) {
          output.close();
          return new NativeSourceRecord(null, nextOffset, Long.MIN_VALUE);
        }
        long maxRowtime =
            rowtimeIndex < 0
                ? Long.MIN_VALUE
                : NativeSourceWatermarks.maxRowtimeMillis(output, rowtimeIndex);
        return new NativeSourceRecord(new ArrowBatch(output), nextOffset, maxRowtime);
      }
    }
  }

  private NativeSourceRecord decodeContiguous(
      List<ConsumerRecord<byte[], byte[]>> records, long bodyBytes, long keyBytes) throws Exception {
    int count = records.size();
    long totalBytes = Math.addExact(bodyBytes, keyBytes);
    ensureDecodeSlab(totalBytes);
    int[] lengths = new int[Math.multiplyExact(count, 2)];
    long keyOffset = 0;
    long bodyOffset = keyBytes;
    for (int i = 0; i < count; i++) {
      ConsumerRecord<byte[], byte[]> record = records.get(i);
      byte[] key = keyed ? record.key() : null;
      byte[] value = record.value();
      lengths[i * 2] = key == null ? -1 : key.length;
      lengths[i * 2 + 1] = value == null ? -1 : value.length;
      if (key != null) {
        decodeSlab.setBytes(keyOffset, key);
        keyOffset += key.length;
      }
      if (value != null) {
        decodeSlab.setBytes(bodyOffset, value);
        bodyOffset += value.length;
      }
    }
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      decoder.decodeContiguousBytesInto(
          decodeSlab.memoryAddress(),
          totalBytes,
          keyBytes,
          lengths,
          count,
          keyed,
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      VectorSchemaRoot output =
          Data.importVectorSchemaRoot(
              allocator, outArray, outSchema, NativeAllocator.DICTIONARIES);
      return decodedRecord(records, output);
    }
  }

  private void ensureDecodeSlab(long required) {
    if (decodeSlab != null && decodeSlab.capacity() >= required) {
      return;
    }
    if (decodeSlab != null) {
      decodeSlab.close();
    }
    decodeSlab = allocator.buffer(Math.max(1L, required));
  }

  private NativeSourceRecord decodedRecord(
      List<ConsumerRecord<byte[], byte[]>> records, VectorSchemaRoot output) {
    long nextOffset = records.get(records.size() - 1).offset() + 1;
    if (output.getRowCount() == 0) {
      output.close();
      return new NativeSourceRecord(null, nextOffset, Long.MIN_VALUE);
    }
    long maxRowtime =
        rowtimeIndex < 0
            ? Long.MIN_VALUE
            : NativeSourceWatermarks.maxRowtimeMillis(output, rowtimeIndex);
    return new NativeSourceRecord(new ArrowBatch(output), nextOffset, maxRowtime);
  }

  private static void set(VarBinaryVector vector, int index, byte[] value) {
    if (value == null) {
      vector.setNull(index);
    } else {
      vector.setSafe(index, value);
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
    try {
      delegate.close();
    } finally {
      try {
        decoder.close();
      } finally {
        if (decodeSlab != null) {
          decodeSlab.close();
          decodeSlab = null;
        }
      }
    }
  }

  void commitOffsets(
      Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
    delegate.notifyCheckpointComplete(offsets, callback);
  }
}

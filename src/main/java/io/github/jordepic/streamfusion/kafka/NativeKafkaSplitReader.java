package io.github.jordepic.streamfusion.kafka;

import io.github.jordepic.streamfusion.Native;
import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.NativeAllocator;
import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.common.TopicPartition;

/**
 * The native side of one Flink subtask's Kafka reading: a single rdkafka consumer (multiplexing all the
 * subtask's partitions) wrapped behind the FLIP-27 {@link SplitReader} contract, so it slots into the
 * standard {@code SingleThreadMultiplexSourceReaderBase} machinery in place of Flink's
 * {@code KafkaPartitionSplitReader}. Splits handed over by the enumerator are assigned+seeked natively
 * ({@code assignKafkaSplits}); each {@link #fetch()} polls one cycle and turns the per-partition decoded
 * Arrow batches into per-split records so the reader updates each split's offset state independently.
 *
 * <p>Decode happens in Rust (the consumer feeds payloads straight into an Arrow builder and decodes to
 * {@code outputType}), so no {@code RowData}/{@code ConsumerRecord} is ever materialized on the JVM.
 */
final class NativeKafkaSplitReader implements SplitReader<NativeKafkaRecord, KafkaPartitionSplit> {

  private final long handle;
  private final int maxRecords;
  private final long pollTimeoutMillis;
  private final BufferAllocator allocator = NativeAllocator.SHARED;

  NativeKafkaSplitReader(
      String[] configKeys,
      String[] configValues,
      RowType outputType,
      int maxRecords,
      long pollTimeoutMillis) {
    this.maxRecords = maxRecords;
    this.pollTimeoutMillis = pollTimeoutMillis;
    // Export an empty batch of the decoder's output schema so the native side can build the JSON
    // decoder; the consumer is created here, partitions are assigned later via handleSplitsChanges.
    try (VectorSchemaRoot template = RowDataArrowConverter.write(List.of(), outputType, allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, template, NativeAllocator.DICTIONARIES, array, schema);
      this.handle =
          Native.openKafkaConsumer(
              configKeys, configValues, array.memoryAddress(), schema.memoryAddress());
    }
  }

  @Override
  public RecordsWithSplitIds<NativeKafkaRecord> fetch() {
    int pending = Native.pollKafkaBatch(handle, maxRecords, pollTimeoutMillis);
    RecordsBySplits.Builder<NativeKafkaRecord> builder = new RecordsBySplits.Builder<>();
    for (int i = 0; i < pending; i++) {
      try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
          ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
        long[] meta = new long[2];
        String[] topic = new String[1];
        Native.drainKafkaSplit(
            handle, meta, topic, outArray.memoryAddress(), outSchema.memoryAddress());
        VectorSchemaRoot root =
            Data.importVectorSchemaRoot(allocator, outArray, outSchema, NativeAllocator.DICTIONARIES);
        String splitId =
            KafkaPartitionSplit.toSplitId(new TopicPartition(topic[0], (int) meta[0]));
        builder.add(splitId, new NativeKafkaRecord(new ArrowBatch(root), meta[1]));
      }
    }
    return builder.build();
  }

  @Override
  public void handleSplitsChanges(SplitsChange<KafkaPartitionSplit> splitsChanges) {
    List<KafkaPartitionSplit> splits = splitsChanges.splits();
    String[] topics = new String[splits.size()];
    long[] partitions = new long[splits.size()];
    long[] offsets = new long[splits.size()];
    for (int i = 0; i < splits.size(); i++) {
      KafkaPartitionSplit split = splits.get(i);
      topics[i] = split.getTopic();
      partitions[i] = split.getPartition();
      offsets[i] = split.getStartingOffset();
    }
    Native.assignKafkaSplits(handle, topics, partitions, offsets);
  }

  @Override
  public void wakeUp() {
    // fetch() polls with a bounded timeout and returns promptly, so the fetcher loop is never blocked
    // for long; no interrupt of an in-flight native poll is needed.
  }

  @Override
  public void close() {
    Native.closeKafkaConsumer(handle);
  }
}

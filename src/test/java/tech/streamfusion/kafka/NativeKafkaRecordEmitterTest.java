package tech.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeSourceRecord;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitState;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Pins the native batch to Kafka's split-local output and checkpoint offset contract. */
@Tag("streamfusion-kafka")
class NativeKafkaRecordEmitterTest {

  @Test
  void emitsPartitionTimestampAndAdvancesOffset() throws Exception {
    NativeKafkaRecordEmitter emitter = new NativeKafkaRecordEmitter();
    KafkaPartitionSplitState splitState =
        new KafkaPartitionSplitState(
            new KafkaPartitionSplit(new TopicPartition("events", 3), 11L));
    CapturingOutput output = new CapturingOutput();

    try (BufferAllocator allocator = new RootAllocator()) {
      VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of()), allocator);
      ArrowBatch batch = new ArrowBatch(root);

      emitter.emitRecord(
          new NativeSourceRecord(batch, 42L, 1_700_000_000_123L), output, splitState);

      assertSame(batch, output.record);
      assertEquals(1_700_000_000_123L, output.timestamp);
      assertEquals(42L, splitState.getCurrentOffset());
      output.record.root().close();
    }
  }

  private static final class CapturingOutput implements SourceOutput<ArrowBatch> {

    private ArrowBatch record;
    private long timestamp = Long.MIN_VALUE;

    @Override
    public void collect(ArrowBatch record) {
      this.record = record;
    }

    @Override
    public void collect(ArrowBatch record, long timestamp) {
      this.record = record;
      this.timestamp = timestamp;
    }

    @Override
    public void emitWatermark(Watermark watermark) {}

    @Override
    public void markIdle() {}

    @Override
    public void markActive() {}
  }
}

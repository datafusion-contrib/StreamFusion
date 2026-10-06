package tech.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.NativeSourceRecord;

@Tag("streamfusion-kafka")
class NativeKafkaSplitReaderTest {
  @Test
  void failedPollClosesEarlierDecodedBatchesWithoutWaitingForGc() {
    var raw = new RecordsBySplits.Builder<ConsumerRecord<byte[], byte[]>>();
    raw.add("first", new ConsumerRecord<>("events", 0, 0L, null, new byte[] {1}));
    raw.add("second", new ConsumerRecord<>("events", 1, 0L, null, new byte[] {2}));
    IOException failure = new IOException("malformed second batch");
    AtomicInteger calls = new AtomicInteger();
    try (var allocator = new RootAllocator()) {
      IOException thrown =
          assertThrows(
              IOException.class,
              () ->
                  NativeKafkaSplitReader.decodeFetched(
                      raw.build(),
                      records -> {
                        if (calls.getAndIncrement() > 0) {
                          throw failure;
                        }
                        var vector = new BigIntVector("id", allocator);
                        vector.allocateNew(1);
                        vector.set(0, 42L);
                        vector.setValueCount(1);
                        var root = new VectorSchemaRoot(List.of(vector));
                        root.setRowCount(1);
                        return NativeSourceRecord.fromRoot(root, 1L, -1, null);
                      }));
      assertSame(failure, thrown.getCause());
      assertEquals(2, calls.get());
      assertEquals(0L, allocator.getAllocatedMemory());
    }
  }
}

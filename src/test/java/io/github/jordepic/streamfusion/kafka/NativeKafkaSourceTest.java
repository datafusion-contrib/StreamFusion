package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.Native;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end test of the production native Kafka split reader (assign+seek+poll → decoded Arrow with
 * checkpointable offsets), exercising the part the FLIP-27 source delegates to native code. It proves
 * the offset semantics that make the source correct: a reader assigned to a partition at an explicit
 * offset reads forward from exactly there, reports the next offset to resume from, and a *second*
 * reader opened at that reported offset continues with no gap and no overlap — i.e. exactly-once across
 * a simulated checkpoint/restore. Decode is the same native JSON path the shallow operator uses.
 *
 * <p>Opt-in via {@code SF_BENCHMARK=true} (Docker for Testcontainers Kafka, and a native build with
 * the {@code kafka} cargo feature, which statically links a bundled librdkafka). The default build
 * excludes rdkafka and skips this test.
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeKafkaSourceTest {

  private static final String TOPIC = "native-source-it";
  private static final int MESSAGES = 5_000;

  @Test
  void readsAssignedPartitionAndResumesFromCheckpointedOffset() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      produce(brokers, MESSAGES);

      Set<Long> ids = new HashSet<>();
      try (BufferAllocator allocator = new RootAllocator();
          CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {

        // Session 1: open at offset 0, read only the first ~half, then "checkpoint" the next offset.
        long[] checkpoint = {0};
        long handle = open(allocator, dictionaries, brokers, checkpoint[0]);
        try {
          while (ids.size() < MESSAGES / 2) {
            poll(handle, allocator, dictionaries, ids, checkpoint);
          }
        } finally {
          Native.closeKafkaConsumer(handle);
        }
        long resumeFrom = checkpoint[0];
        assertEquals(ids.size(), resumeFrom, "next offset must equal rows read on a single partition");

        // Session 2: a fresh reader restored at the checkpointed offset finishes the topic.
        handle = open(allocator, dictionaries, brokers, resumeFrom);
        try {
          long emptyPolls = 0;
          while (ids.size() < MESSAGES && emptyPolls < 3) {
            long before = ids.size();
            poll(handle, allocator, dictionaries, ids, checkpoint);
            emptyPolls = ids.size() == before ? emptyPolls + 1 : 0;
          }
        } finally {
          Native.closeKafkaConsumer(handle);
        }
      }

      // Exactly-once: every id 0..MESSAGES-1 seen exactly once across the two sessions (no gap/overlap).
      assertEquals(MESSAGES, ids.size(), "expected each message exactly once across checkpoint/restore");
      for (long i = 0; i < MESSAGES; i++) {
        assertTrue(ids.contains(i), "missing id " + i);
      }
    }
  }

  /** Opens a native reader and assigns it {@code (TOPIC, 0)} starting at {@code startOffset}. */
  private static long open(
      BufferAllocator allocator,
      CDataDictionaryProvider dictionaries,
      String brokers,
      long startOffset) {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", brokers);
    props.setProperty("group.id", "native-source-it");
    props.setProperty("enable.auto.commit", "false");
    KafkaConfigTranslator.Result config = KafkaConfigTranslator.translate(props);
    assertTrue(config.isTranslated(), () -> "config should translate: " + config.fallbackReason());
    String[] keys = config.config().keySet().toArray(new String[0]);
    String[] values = new String[keys.length];
    for (int i = 0; i < keys.length; i++) {
      values[i] = config.config().get(keys[i]);
    }
    try (BigIntVector id = new BigIntVector("id", allocator);
        VarCharVector name = new VarCharVector("name", allocator);
        Float8Vector score = new Float8Vector("score", allocator)) {
      for (FieldVector vector : List.of(id, name, score)) {
        vector.allocateNew();
        vector.setValueCount(0);
      }
      try (VectorSchemaRoot template = new VectorSchemaRoot(List.of(id, name, score));
          ArrowArray array = ArrowArray.allocateNew(allocator);
          ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
        template.setRowCount(0);
        Data.exportVectorSchemaRoot(allocator, template, dictionaries, array, schema);
        long handle =
            Native.openKafkaConsumer(
                keys, values, 0, array.memoryAddress(), schema.memoryAddress(), "", "", 0, null, "", -1);
        Native.assignKafkaSplits(handle, new String[] {TOPIC}, new long[] {0}, new long[] {startOffset});
        return handle;
      }
    }
  }

  /** Polls a cycle, draining each per-partition batch's ids and the (single) split's next offset. */
  private static void poll(
      long handle,
      BufferAllocator allocator,
      CDataDictionaryProvider dictionaries,
      Set<Long> ids,
      long[] checkpoint) {
    int pending = Native.pollKafkaBatch(handle, 1024, 2000);
    for (int p = 0; p < pending; p++) {
      try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
          ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
        long[] meta = new long[3];
        String[] topic = new String[1];
        int rows =
            Native.drainKafkaSplit(
                handle, meta, topic, outArray.memoryAddress(), outSchema.memoryAddress());
        checkpoint[0] = meta[1]; // single partition in this test
        try (VectorSchemaRoot out =
            Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries)) {
          assertEquals(rows, out.getRowCount());
          BigIntVector id = (BigIntVector) out.getVector("id");
          for (int i = 0; i < out.getRowCount(); i++) {
            ids.add(id.get(i));
          }
        }
      }
    }
  }

  private static void produce(String brokers, int messages) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1 << 20);
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (int i = 0; i < messages; i++) {
        byte[] value =
            String.format("{\"id\": %d, \"name\": \"row-%d\", \"score\": %d.5}", i, i, i % 100)
                .getBytes(StandardCharsets.UTF_8);
        producer.send(new ProducerRecord<>(TOPIC, 0, null, value));
      }
      producer.flush();
    }
  }
}

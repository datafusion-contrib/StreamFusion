package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end test of the production native Kafka split reader (assign+seek+poll → Arrow body batches with
 * checkpointable offsets), exercising the part the FLIP-27 source delegates to native code. It proves
 * the offset semantics that make the source correct: a reader assigned to a partition at an explicit
 * offset reads forward from exactly there, reports the next offset to resume from, and a *second*
 * reader opened at that reported offset continues with no gap and no overlap — i.e. exactly-once across
 * a simulated checkpoint/restore. The test deliberately observes raw bodies: JSON decoding belongs to
 * the separate format artifact.
 *
 * <p>Opt-in via {@code SF_BENCHMARK=true} (Docker for Testcontainers Kafka, and a native build with
 * the {@code kafka} cargo feature, which statically links a bundled librdkafka). The default build
 * excludes rdkafka and skips this test.
 */
@Tag("streamfusion-kafka")
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
        long handle = open(brokers, checkpoint[0]);
        try {
          while (ids.size() < MESSAGES / 2) {
            poll(handle, allocator, dictionaries, ids, checkpoint);
          }
        } finally {
          NativeKafka.closeKafkaConsumer(handle);
        }
        long resumeFrom = checkpoint[0];
        assertEquals(ids.size(), resumeFrom, "next offset must equal rows read on a single partition");

        // Session 2: a fresh reader restored at the checkpointed offset finishes the topic.
        handle = open(brokers, resumeFrom);
        try {
          long emptyPolls = 0;
          while (ids.size() < MESSAGES && emptyPolls < 3) {
            long before = ids.size();
            poll(handle, allocator, dictionaries, ids, checkpoint);
            emptyPolls = ids.size() == before ? emptyPolls + 1 : 0;
          }
        } finally {
          NativeKafka.closeKafkaConsumer(handle);
        }
      }

      // Exactly-once: every id 0..MESSAGES-1 seen exactly once across the two sessions (no gap/overlap).
      assertEquals(MESSAGES, ids.size(), "expected each message exactly once across checkpoint/restore");
      for (long i = 0; i < MESSAGES; i++) {
        assertTrue(ids.contains(i), "missing id " + i);
      }
    }
  }

  @Test
  void capsEachBatchAtTheBoundedStoppingOffset() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      produce(brokers, TOPIC, 0, 100);

      Set<Long> ids = new HashSet<>();
      long[] checkpoint = {0};
      long handle = open(brokers, new String[] {TOPIC}, new long[] {0}, new long[] {37});
      try (BufferAllocator allocator = new RootAllocator();
          CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
        for (int attempts = 0; checkpoint[0] < 37 && attempts < 10; attempts++) {
          poll(handle, allocator, dictionaries, ids, checkpoint);
        }
      } finally {
        NativeKafka.closeKafkaConsumer(handle);
      }

      assertEquals(37, checkpoint[0]);
      assertEquals(37, ids.size());
      for (long id = 0; id < 37; id++) {
        assertTrue(ids.contains(id), "missing bounded id " + id);
      }
    }
  }

  @Test
  void keepsTheSamePartitionNumberSeparateAcrossTopics() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      String first = "native-source-topic-a";
      String second = "native-source-topic-b";
      produce(brokers, first, 0, 10);
      produce(brokers, second, 100, 110);

      Map<String, Set<Long>> ids =
          Map.of(first, new HashSet<>(), second, new HashSet<>());
      Map<String, Long> checkpoints = new java.util.HashMap<>();
      long handle =
          open(
              brokers,
              new String[] {first, second},
              new long[] {0, 0},
              new long[] {10, 10});
      try (BufferAllocator allocator = new RootAllocator();
          CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
        for (int attempts = 0;
            ids.values().stream().mapToInt(Set::size).sum() < 20 && attempts < 10;
            attempts++) {
          pollByTopic(handle, allocator, dictionaries, ids, checkpoints);
        }
      } finally {
        NativeKafka.closeKafkaConsumer(handle);
      }

      assertEquals(Set.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L), ids.get(first));
      assertEquals(
          Set.of(100L, 101L, 102L, 103L, 104L, 105L, 106L, 107L, 108L, 109L),
          ids.get(second));
      assertEquals(10L, checkpoints.get(first));
      assertEquals(10L, checkpoints.get(second));
    }
  }

  @Test
  void preservesTombstonesAndAdvancesPastThem() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      produceWithTombstone(brokers);

      List<byte[]> bodies = new ArrayList<>();
      long[] checkpoint = {0};
      long handle = open(brokers, new String[] {TOPIC}, new long[] {0}, new long[] {3});
      try (BufferAllocator allocator = new RootAllocator();
          CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
        for (int attempts = 0; checkpoint[0] < 3 && attempts < 10; attempts++) {
          pollBodies(handle, allocator, dictionaries, bodies, checkpoint);
        }
      } finally {
        NativeKafka.closeKafkaConsumer(handle);
      }

      assertEquals(3, checkpoint[0]);
      assertEquals(3, bodies.size());
      assertEquals(0L, id(bodies.get(0)));
      assertNull(bodies.get(1));
      assertEquals(2L, id(bodies.get(2)));
    }
  }

  @Test
  void surfacesAnUnresettableOffsetAsAnIOException() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      produce(brokers, TOPIC, 0, 1);
      Properties props = consumerProperties(brokers);
      props.setProperty("auto.offset.reset", "none");
      long handle = open(props, new String[] {TOPIC}, new long[] {10_000}, new long[] {Long.MIN_VALUE});
      try {
        IOException error =
            assertThrows(
                IOException.class,
                () -> {
                  for (int attempts = 0; attempts < 10; attempts++) {
                    NativeKafka.pollKafkaBatch(handle, 1024, 1000);
                  }
                });
        assertTrue(error.getMessage().contains("_AUTO_OFFSET_RESET"), error::getMessage);
      } finally {
        NativeKafka.closeKafkaConsumer(handle);
      }
    }
  }

  @Test
  void commitsCompletedCheckpointOffsetsToKafka() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      String group = "native-source-checkpoint-it";
      Properties props = consumerProperties(brokers);
      props.setProperty("group.id", group);
      long handle =
          open(props, new String[] {TOPIC}, new long[] {0}, new long[] {Long.MIN_VALUE});
      try {
        NativeKafka.commitKafkaOffsets(
            handle, new String[] {TOPIC}, new long[] {0}, new long[] {37});
      } finally {
        NativeKafka.closeKafkaConsumer(handle);
      }

      Properties adminProps = new Properties();
      adminProps.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
      try (AdminClient admin = AdminClient.create(adminProps)) {
        assertEquals(
            37,
            admin
                .listConsumerGroupOffsets(group)
                .partitionsToOffsetAndMetadata()
                .get()
                .get(new TopicPartition(TOPIC, 0))
                .offset());
      }
    }
  }

  @Test
  void pausesAndResumesIndividualSplits() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      String pausedTopic = "native-source-paused";
      String runningTopic = "native-source-running";
      produce(brokers, pausedTopic, 0, 10);
      produce(brokers, runningTopic, 100, 110);

      Map<String, Set<Long>> ids =
          Map.of(pausedTopic, new HashSet<>(), runningTopic, new HashSet<>());
      Map<String, Long> checkpoints = new java.util.HashMap<>();
      long handle =
          open(
              brokers,
              new String[] {pausedTopic, runningTopic},
              new long[] {0, 0},
              new long[] {Long.MIN_VALUE, Long.MIN_VALUE});
      try (BufferAllocator allocator = new RootAllocator();
          CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
        for (int attempts = 0;
            ids.values().stream().mapToInt(Set::size).sum() < 20 && attempts < 10;
            attempts++) {
          pollByTopic(handle, allocator, dictionaries, ids, checkpoints);
        }
        assertEquals(20, ids.values().stream().mapToInt(Set::size).sum());

        NativeKafka.setKafkaSplitsPaused(
            handle, new String[] {pausedTopic}, new long[] {0}, true);
        produce(brokers, pausedTopic, 10, 20);
        produce(brokers, runningTopic, 110, 120);
        for (int attempts = 0;
            checkpoints.getOrDefault(runningTopic, 0L) < 20 && attempts < 10;
            attempts++) {
          pollByTopic(handle, allocator, dictionaries, ids, checkpoints);
        }
        assertEquals(10L, checkpoints.get(pausedTopic));
        assertEquals(20L, checkpoints.get(runningTopic));

        NativeKafka.setKafkaSplitsPaused(
            handle, new String[] {pausedTopic}, new long[] {0}, false);
        for (int attempts = 0;
            checkpoints.getOrDefault(pausedTopic, 0L) < 20 && attempts < 10;
            attempts++) {
          pollByTopic(handle, allocator, dictionaries, ids, checkpoints);
        }
        assertEquals(20L, checkpoints.get(pausedTopic));
      } finally {
        NativeKafka.closeKafkaConsumer(handle);
      }
    }
  }

  @Test
  void readCommittedFinishesAcrossAbortedTransactionsAndControlRecords() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      produceTransactions(brokers);

      TopicPartition partition = new TopicPartition(TOPIC, 0);
      Properties adminProps = new Properties();
      adminProps.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
      long stop;
      try (AdminClient admin = AdminClient.create(adminProps)) {
        stop =
            admin
                .listOffsets(Map.of(partition, OffsetSpec.latest()))
                .partitionResult(partition)
                .get()
                .offset();
      }

      Properties props = consumerProperties(brokers);
      props.setProperty("group.id", "native-source-read-committed-it");
      props.setProperty("isolation.level", "read_committed");
      List<byte[]> bodies = new ArrayList<>();
      long[] checkpoint = {0};
      long handle = open(props, new String[] {TOPIC}, new long[] {0}, new long[] {stop});
      try (BufferAllocator allocator = new RootAllocator();
          CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
        for (int attempts = 0; checkpoint[0] < stop && attempts < 10; attempts++) {
          pollBodies(handle, allocator, dictionaries, bodies, checkpoint);
        }
      } finally {
        NativeKafka.closeKafkaConsumer(handle);
      }

      assertEquals(stop, checkpoint[0], "control records must advance the bounded position");
      assertEquals(List.of(0L, 2L), bodies.stream().map(NativeKafkaSourceTest::id).toList());
    }
  }

  @Test
  void groupOffsetsHonorCommittedAndResetStrategies() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      produce(brokers, TOPIC, 0, 3);

      Properties none = consumerProperties(brokers);
      none.setProperty("group.id", "native-source-no-offset-none");
      none.setProperty("auto.offset.reset", "none");
      long handle = open(none, new String[] {TOPIC}, new long[] {-3}, new long[] {Long.MIN_VALUE});
      long noOffsetHandle = handle;
      try {
        assertThrows(
            IOException.class,
            () -> {
              for (int attempts = 0; attempts < 10; attempts++) {
                NativeKafka.pollKafkaBatch(noOffsetHandle, 1024, 1000);
              }
            });
      } finally {
        NativeKafka.closeKafkaConsumer(handle);
      }

      Properties earliest = consumerProperties(brokers);
      earliest.setProperty("group.id", "native-source-no-offset-earliest");
      earliest.setProperty("auto.offset.reset", "earliest");
      List<byte[]> earliestBodies = new ArrayList<>();
      long[] earliestPosition = {0};
      handle =
          open(earliest, new String[] {TOPIC}, new long[] {-3}, new long[] {Long.MIN_VALUE});
      try (BufferAllocator allocator = new RootAllocator();
          CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
        for (int attempts = 0; earliestBodies.size() < 3 && attempts < 10; attempts++) {
          pollBodies(handle, allocator, dictionaries, earliestBodies, earliestPosition);
        }
      } finally {
        NativeKafka.closeKafkaConsumer(handle);
      }
      assertEquals(List.of(0L, 1L, 2L), earliestBodies.stream().map(NativeKafkaSourceTest::id).toList());

      Properties latest = consumerProperties(brokers);
      latest.setProperty("group.id", "native-source-no-offset-latest");
      latest.setProperty("auto.offset.reset", "latest");
      List<byte[]> latestBodies = new ArrayList<>();
      long[] latestPosition = {0};
      handle = open(latest, new String[] {TOPIC}, new long[] {-3}, new long[] {Long.MIN_VALUE});
      try (BufferAllocator allocator = new RootAllocator();
          CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
        for (int attempts = 0; latestPosition[0] < 3 && attempts < 10; attempts++) {
          pollBodies(handle, allocator, dictionaries, latestBodies, latestPosition);
        }
        produce(brokers, TOPIC, 3, 4);
        for (int attempts = 0; latestBodies.isEmpty() && attempts < 10; attempts++) {
          pollBodies(handle, allocator, dictionaries, latestBodies, latestPosition);
        }
      } finally {
        NativeKafka.closeKafkaConsumer(handle);
      }
      assertEquals(List.of(3L), latestBodies.stream().map(NativeKafkaSourceTest::id).toList());

      Properties committed = consumerProperties(brokers);
      committed.setProperty("group.id", "native-source-stored-offset");
      committed.setProperty("auto.offset.reset", "none");
      handle = open(committed, new String[] {TOPIC}, new long[] {0}, new long[] {Long.MIN_VALUE});
      NativeKafka.commitKafkaOffsets(
          handle, new String[] {TOPIC}, new long[] {0}, new long[] {2});
      NativeKafka.closeKafkaConsumer(handle);
      List<byte[]> committedBodies = new ArrayList<>();
      long[] committedPosition = {0};
      handle = open(committed, new String[] {TOPIC}, new long[] {-3}, new long[] {Long.MIN_VALUE});
      try (BufferAllocator allocator = new RootAllocator();
          CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
        for (int attempts = 0; committedBodies.isEmpty() && attempts < 10; attempts++) {
          pollBodies(handle, allocator, dictionaries, committedBodies, committedPosition);
        }
      } finally {
        NativeKafka.closeKafkaConsumer(handle);
      }
      assertEquals(
          List.of(2L, 3L), committedBodies.stream().map(NativeKafkaSourceTest::id).toList());
    }
  }

  /** Opens a native reader and assigns it {@code (TOPIC, 0)} starting at {@code startOffset}. */
  private static long open(String brokers, long startOffset) {
    return open(
        brokers,
        new String[] {TOPIC},
        new long[] {startOffset},
        new long[] {Long.MIN_VALUE});
  }

  private static long open(
      String brokers, String[] topics, long[] startOffsets, long[] stoppingOffsets) {
    return open(consumerProperties(brokers), topics, startOffsets, stoppingOffsets);
  }

  private static Properties consumerProperties(String brokers) {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", brokers);
    props.setProperty("group.id", "native-source-it");
    props.setProperty("enable.auto.commit", "false");
    return props;
  }

  private static long open(
      Properties props, String[] topics, long[] startOffsets, long[] stoppingOffsets) {
    KafkaConfigTranslator.Result config = KafkaConfigTranslator.translate(props);
    assertTrue(config.isTranslated(), () -> "config should translate: " + config.fallbackReason());
    String[] keys = config.config().keySet().toArray(new String[0]);
    String[] values = new String[keys.length];
    for (int i = 0; i < keys.length; i++) {
      values[i] = config.config().get(keys[i]);
    }
    long handle = NativeKafka.openKafkaConsumer(keys, values);
    NativeKafka.assignKafkaSplits(
        handle, topics, new long[topics.length], startOffsets, stoppingOffsets);
    return handle;
  }

  private static void pollBodies(
      long handle,
      BufferAllocator allocator,
      CDataDictionaryProvider dictionaries,
      List<byte[]> bodies,
      long[] checkpoint) {
    int pending = NativeKafka.pollKafkaBatch(handle, 1024, 2000);
    for (int p = 0; p < pending; p++) {
      try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
          ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
        long[] meta = new long[5];
        String[] topic = new String[1];
        NativeKafka.drainKafkaSplit(
            handle, meta, topic, outArray.memoryAddress(), outSchema.memoryAddress());
        checkpoint[0] = meta[1];
        try (VectorSchemaRoot out =
            Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries)) {
          VarBinaryVector body = (VarBinaryVector) out.getVector("body");
          for (int i = 0; i < out.getRowCount(); i++) {
            bodies.add(body.isNull(i) ? null : body.get(i));
          }
        }
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
    int pending = NativeKafka.pollKafkaBatch(handle, 1024, 2000);
    for (int p = 0; p < pending; p++) {
      try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
          ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
        long[] meta = new long[5];
        String[] topic = new String[1];
        int rows =
            NativeKafka.drainKafkaSplit(
                handle, meta, topic, outArray.memoryAddress(), outSchema.memoryAddress());
        checkpoint[0] = meta[1]; // single partition in this test
        assertEquals(rows, meta[3], "native metadata must count consumed records");
        if (rows > 0) {
          assertTrue(meta[2] > 0, "non-empty payload batches must report consumed bytes");
        }
        assertTrue(
            meta[4] == -1 || meta[4] >= meta[1],
            "cached high watermark must not trail the consumer position");
        try (VectorSchemaRoot out =
            Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries)) {
          assertEquals(rows, out.getRowCount());
          VarBinaryVector body = (VarBinaryVector) out.getVector("body");
          for (int i = 0; i < out.getRowCount(); i++) {
            String message = new String(body.get(i), StandardCharsets.UTF_8);
            int start = message.indexOf(":") + 1;
            int end = message.indexOf(",", start);
            ids.add(Long.parseLong(message.substring(start, end).trim()));
          }
        }
      }
    }
  }

  private static void pollByTopic(
      long handle,
      BufferAllocator allocator,
      CDataDictionaryProvider dictionaries,
      Map<String, Set<Long>> ids,
      Map<String, Long> checkpoints) {
    int pending = NativeKafka.pollKafkaBatch(handle, 1024, 2000);
    for (int p = 0; p < pending; p++) {
      try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
          ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
        long[] meta = new long[2];
        String[] topic = new String[1];
        NativeKafka.drainKafkaSplit(
            handle, meta, topic, outArray.memoryAddress(), outSchema.memoryAddress());
        checkpoints.put(topic[0], meta[1]);
        try (VectorSchemaRoot out =
            Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries)) {
          VarBinaryVector body = (VarBinaryVector) out.getVector("body");
          for (int i = 0; i < out.getRowCount(); i++) {
            ids.get(topic[0]).add(id(body.get(i)));
          }
        }
      }
    }
  }

  private static long id(byte[] body) {
    String message = new String(body, StandardCharsets.UTF_8);
    int start = message.indexOf(":") + 1;
    int end = message.indexOf(",", start);
    return Long.parseLong(message.substring(start, end).trim());
  }

  private static void produce(String brokers, int messages) {
    produce(brokers, TOPIC, 0, messages);
  }

  private static void produce(String brokers, String topic, int from, int to) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1 << 20);
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (int i = from; i < to; i++) {
        byte[] value =
            String.format("{\"id\": %d, \"name\": \"row-%d\", \"score\": %d.5}", i, i, i % 100)
                .getBytes(StandardCharsets.UTF_8);
        producer.send(new ProducerRecord<>(topic, 0, null, value));
      }
      producer.flush();
    }
  }

  private static void produceWithTombstone(String brokers) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (int id : new int[] {0, 2}) {
        byte[] value =
            String.format("{\"id\": %d, \"name\": \"row-%d\", \"score\": 1.5}", id, id)
                .getBytes(StandardCharsets.UTF_8);
        producer.send(new ProducerRecord<>(TOPIC, 0, null, value));
        if (id == 0) {
          producer.send(new ProducerRecord<>(TOPIC, 0, null, null));
        }
      }
      producer.flush();
    }
  }

  private static void produceTransactions(String brokers) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "native-source-control-records-it");
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      producer.initTransactions();
      producer.beginTransaction();
      producer.send(new ProducerRecord<>(TOPIC, 0, null, message(0)));
      producer.commitTransaction();

      producer.beginTransaction();
      producer.send(new ProducerRecord<>(TOPIC, 0, null, message(1)));
      producer.abortTransaction();

      producer.beginTransaction();
      producer.send(new ProducerRecord<>(TOPIC, 0, null, message(2)));
      producer.commitTransaction();
    }
  }

  private static byte[] message(int id) {
    return String.format("{\"id\": %d, \"name\": \"row-%d\", \"score\": 1.5}", id, id)
        .getBytes(StandardCharsets.UTF_8);
  }
}

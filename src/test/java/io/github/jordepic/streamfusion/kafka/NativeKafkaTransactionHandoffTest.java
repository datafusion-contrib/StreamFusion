package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Broker-level proof of the exactly-once hand-off contract the native Kafka producer rests on: a
 * transaction is identified by (transactional.id, producer id, epoch) alone, so a transaction
 * written and flushed by librdkafka — whose session is then destroyed — can be committed by Flink's
 * Java committer impersonating that identity, fenced by a Java initTransactions on the same id, or
 * reaped by the broker's transaction timeout. Each is pinned here, along with the statistics-based
 * identity capture being validated against the transaction coordinator's own view.
 */
@Tag("streamfusion-kafka")
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeKafkaTransactionHandoffTest {

  private static final long TIMEOUT_MS = 30_000;

  @Test
  void javaCommitterCommitsNativeTransaction() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String bootstrap = kafka.getBootstrapServers();
      String topic = "handoff-commit-" + UUID.randomUUID();
      String transactionalId = "handoff-txn-" + UUID.randomUUID();
      createTopic(bootstrap, topic);

      long handle = openNativeProducer(bootstrap, transactionalId, 60_000);
      long[] identity = new long[2];
      NativeKafka.initKafkaTransactions(handle, TIMEOUT_MS, identity);
      assertTrue(identity[0] >= 0, "producer id assigned");
      assertTrue(identity[1] >= 0, "epoch assigned");

      NativeKafka.beginKafkaTransaction(handle);
      for (int i = 0; i < 5; i++) {
        NativeKafka.produceKafkaRecord(handle, topic, bytes("key-" + i), bytes("value-" + i));
      }
      long[] flushedIdentity = new long[2];
      NativeKafka.flushKafkaProducer(handle, TIMEOUT_MS, flushedIdentity);
      assertArrayEquals(identity, flushedIdentity, "identity stable across the epoch");

      TransactionDescription described = describeTransaction(bootstrap, transactionalId);
      assertEquals(TransactionState.ONGOING, described.state());
      assertEquals(identity[0], described.producerId(), "statistics producer id matches broker");
      assertEquals(identity[1], described.producerEpoch(), "statistics epoch matches broker");

      assertEquals(5, consume(bootstrap, topic, "read_uncommitted", 5).size());
      assertTrue(
          consume(bootstrap, topic, "read_committed", 1).isEmpty(),
          "no records visible before commit");

      NativeKafka.closeKafkaProducer(handle);
      assertEquals(
          TransactionState.ONGOING,
          describeTransaction(bootstrap, transactionalId).state(),
          "destroying the native producer neither commits nor aborts");

      try (FlinkKafkaInternalProducer<byte[], byte[]> committer =
          committer(bootstrap, transactionalId)) {
        committer.resumeTransaction(identity[0], (short) identity[1]);
        committer.commitTransaction();
      }

      List<ConsumerRecord<byte[], byte[]>> committed =
          consume(bootstrap, topic, "read_committed", 5);
      assertEquals(5, committed.size(), "committed records visible exactly once");
      for (int i = 0; i < 5; i++) {
        assertEquals("key-" + i, new String(committed.get(i).key(), StandardCharsets.UTF_8));
        assertEquals("value-" + i, new String(committed.get(i).value(), StandardCharsets.UTF_8));
      }

      // A committer retry after a crash replays EndTxn with the same identity; the broker treats
      // it as a duplicate of the completed commit, and no records are duplicated.
      try (FlinkKafkaInternalProducer<byte[], byte[]> retry =
          committer(bootstrap, transactionalId)) {
        retry.resumeTransaction(identity[0], (short) identity[1]);
        retry.commitTransaction();
      }
      assertEquals(5, consume(bootstrap, topic, "read_committed", 6).size());
    }
  }

  @Test
  void javaInitTransactionsFencesNativeTransaction() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String bootstrap = kafka.getBootstrapServers();
      String topic = "handoff-fence-" + UUID.randomUUID();
      String transactionalId = "handoff-txn-" + UUID.randomUUID();
      createTopic(bootstrap, topic);

      long[] identity = writeFlushedTransaction(bootstrap, topic, transactionalId, 60_000);

      // Restore-time abort probing is exactly this call: initTransactions on the id fences the
      // native transaction's epoch and aborts it.
      Properties props = producerProperties(bootstrap);
      props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
      try (KafkaProducer<byte[], byte[]> prober = new KafkaProducer<>(props)) {
        prober.initTransactions();
      }
      assertNotEquals(
          TransactionState.ONGOING, describeTransaction(bootstrap, transactionalId).state());

      try (FlinkKafkaInternalProducer<byte[], byte[]> committer =
          committer(bootstrap, transactionalId)) {
        committer.resumeTransaction(identity[0], (short) identity[1]);
        assertThrows(KafkaException.class, committer::commitTransaction);
      }
      assertTrue(
          consume(bootstrap, topic, "read_committed", 1).isEmpty(),
          "fenced transaction's records never become visible");
    }
  }

  @Test
  void brokerAbortsNativeTransactionAfterTimeout() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
            .withEnv("KAFKA_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS", "1000")) {
      kafka.start();
      String bootstrap = kafka.getBootstrapServers();
      String topic = "handoff-timeout-" + UUID.randomUUID();
      String transactionalId = "handoff-txn-" + UUID.randomUUID();
      createTopic(bootstrap, topic);

      long[] identity = writeFlushedTransaction(bootstrap, topic, transactionalId, 5_000);

      long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
      TransactionState state = TransactionState.ONGOING;
      while (state == TransactionState.ONGOING && System.nanoTime() < deadline) {
        Thread.sleep(500);
        state = describeTransaction(bootstrap, transactionalId).state();
      }
      assertNotEquals(TransactionState.ONGOING, state, "broker reaps the abandoned transaction");

      try (FlinkKafkaInternalProducer<byte[], byte[]> committer =
          committer(bootstrap, transactionalId)) {
        committer.resumeTransaction(identity[0], (short) identity[1]);
        assertThrows(KafkaException.class, committer::commitTransaction);
      }
      assertTrue(
          consume(bootstrap, topic, "read_committed", 1).isEmpty(),
          "timed-out transaction's records never become visible");
    }
  }

  /** Opens a native producer, writes one flushed-but-uncommitted transaction, and destroys it. */
  private static long[] writeFlushedTransaction(
      String bootstrap, String topic, String transactionalId, int transactionTimeoutMs) {
    long handle = openNativeProducer(bootstrap, transactionalId, transactionTimeoutMs);
    long[] identity = new long[2];
    NativeKafka.initKafkaTransactions(handle, TIMEOUT_MS, identity);
    NativeKafka.beginKafkaTransaction(handle);
    for (int i = 0; i < 3; i++) {
      NativeKafka.produceKafkaRecord(handle, topic, null, bytes("orphan-" + i));
    }
    NativeKafka.flushKafkaProducer(handle, TIMEOUT_MS, identity);
    NativeKafka.closeKafkaProducer(handle);
    return identity;
  }

  private static long openNativeProducer(
      String bootstrap, String transactionalId, int transactionTimeoutMs) {
    Map<String, String> config = new LinkedHashMap<>();
    config.put("bootstrap.servers", bootstrap);
    config.put("transactional.id", transactionalId);
    config.put("transaction.timeout.ms", String.valueOf(transactionTimeoutMs));
    config.put("statistics.interval.ms", "50");
    return NativeKafka.openTransactionalKafkaProducer(
        config.keySet().toArray(new String[0]), config.values().toArray(new String[0]));
  }

  private static FlinkKafkaInternalProducer<byte[], byte[]> committer(
      String bootstrap, String transactionalId) {
    return new FlinkKafkaInternalProducer<>(producerProperties(bootstrap), transactionalId);
  }

  private static Properties producerProperties(String bootstrap) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    return props;
  }

  private static TransactionDescription describeTransaction(String bootstrap, String transactionalId)
      throws Exception {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    try (AdminClient admin = AdminClient.create(props)) {
      return admin
          .describeTransactions(List.of(transactionalId))
          .description(transactionalId)
          .get();
    }
  }

  private static void createTopic(String bootstrap, String topic) throws Exception {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    try (AdminClient admin = AdminClient.create(props)) {
      admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
    }
  }

  /** Polls until {@code expected} records arrive or ~5s of patience runs out. */
  private static List<ConsumerRecord<byte[], byte[]>> consume(
      String bootstrap, String topic, String isolation, int expected) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "handoff-consumer-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);
    props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(List.of(topic));
      long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
      while (records.size() < expected && System.nanoTime() < deadline) {
        consumer.poll(Duration.ofMillis(200)).forEach(records::add);
      }
    }
    return records;
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}

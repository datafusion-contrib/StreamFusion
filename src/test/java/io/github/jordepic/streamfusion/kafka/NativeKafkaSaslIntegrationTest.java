package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
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
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end SASL authentication against the native reader: the client listener is switched to
 * SASL_PLAINTEXT (the listener keeps its {@code PLAINTEXT} name so the container's advertised
 * address stays valid, only its protocol changes) and the same translated table properties a SQL
 * job would carry drive librdkafka through a real authenticated read. PLAIN exercises the
 * always-built mechanism; SCRAM-SHA-512 exercises the OpenSSL-gated one that exists only because
 * the native build links OpenSSL statically — if the build lost it, this test fails at consumer
 * creation with librdkafka's "No provider for SASL mechanism" error.
 *
 * <p>The TLS handshake itself is pinned by the Rust capability test (a SASL_SSL+SCRAM consumer
 * must construct); a full SASL_SSL read here would additionally need a broker certificate
 * generated into the container and is left as a follow-up.
 *
 * <p>Opt-in via {@code SF_BENCHMARK=true} like the sibling container tests.
 */
@Tag("streamfusion-kafka")
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeKafkaSaslIntegrationTest {

  private static final String TOPIC = "native-sasl-it";
  private static final int MESSAGES = 100;
  private static final String ADMIN_JAAS =
      "org.apache.kafka.common.security.plain.PlainLoginModule required"
          + " username=\"admin\" password=\"admin-secret\";";

  @Test
  void readsThroughSaslPlainWithTranslatedTableProperties() throws Exception {
    try (KafkaContainer kafka = saslContainer()) {
      kafka.start();
      String bootstrap = bootstrap(kafka);
      produceAsAdmin(bootstrap, MESSAGES);

      Properties props = tableProperties(bootstrap, "PLAIN", ADMIN_JAAS);
      assertEquals(MESSAGES, readAll(props));
    }
  }

  @Test
  void readsThroughSaslScramSha512WithTranslatedTableProperties() throws Exception {
    try (KafkaContainer kafka = saslContainer()) {
      kafka.start();
      String bootstrap = bootstrap(kafka);
      createScramUser(bootstrap, "alice", "alice-secret");
      produceAsAdmin(bootstrap, MESSAGES);

      Properties props =
          tableProperties(
              bootstrap,
              "SCRAM-SHA-512",
              "org.apache.kafka.common.security.scram.ScramLoginModule required"
                  + " username=\"alice\" password=\"alice-secret\";");
      assertEquals(MESSAGES, readAll(props));
    }
  }

  /**
   * The stock container hardcodes every listener to PLAINTEXT, but merges rather than overwrites
   * the env it derives, so the client listener's protocol can be swapped while keeping its name.
   */
  private static KafkaContainer saslContainer() {
    return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
        .withEnv(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT")
        .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN,SCRAM-SHA-512")
        // listener.name.plaintext.plain.sasl.jaas.config: the PLAIN server credentials, including
        // the admin user the tests authenticate as (cp-kafka env encoding: _ becomes a dot).
        .withEnv(
            "KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
            "org.apache.kafka.common.security.plain.PlainLoginModule required"
                + " username=\"admin\" password=\"admin-secret\" user_admin=\"admin-secret\";")
        // listener.name.plaintext.scram-sha-512.sasl.jaas.config (___ encodes a dash): SCRAM
        // needs only the login module server-side; credentials live in cluster metadata.
        .withEnv(
            "KAFKA_LISTENER_NAME_PLAINTEXT_SCRAM___SHA___512_SASL_JAAS_CONFIG",
            "org.apache.kafka.common.security.scram.ScramLoginModule required;");
  }

  /**
   * {@code getBootstrapServers()} prefixes {@code PLAINTEXT://}, which librdkafka would read as a
   * per-broker security protocol contradicting {@code SASL_PLAINTEXT} — so build host:port bare.
   */
  private static String bootstrap(KafkaContainer kafka) {
    return kafka.getHost() + ":" + kafka.getMappedPort(9093);
  }

  /** The table's {@code properties.*} exactly as a secured SQL job would declare them. */
  private static Properties tableProperties(String bootstrap, String mechanism, String jaas) {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", bootstrap);
    props.setProperty("group.id", "native-sasl-it");
    props.setProperty("security.protocol", "SASL_PLAINTEXT");
    props.setProperty("sasl.mechanism", mechanism);
    props.setProperty("sasl.jaas.config", jaas);
    return props;
  }

  /** Translates the table properties and reads the whole topic through the native reader. */
  private static int readAll(Properties props) throws IOException {
    KafkaConfigTranslator.Result config = KafkaConfigTranslator.translate(props);
    assertTrue(
        config.fallbackReason == null, () -> "config should translate: " + config.fallbackReason);
    String[] keys = config.config().keySet().toArray(new String[0]);
    String[] values = new String[keys.length];
    for (int i = 0; i < keys.length; i++) {
      values[i] = config.config().get(keys[i]);
    }
    Set<Long> ids = new HashSet<>();
    long handle = NativeKafka.openKafkaConsumer(keys, values);
    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
      NativeKafka.assignKafkaSplits(
          handle,
          new String[] {TOPIC},
          new long[] {0},
          new long[] {0},
          new long[] {MESSAGES});
      for (int attempts = 0; ids.size() < MESSAGES && attempts < 20; attempts++) {
        int pending = NativeKafka.pollKafkaBatch(handle, 1024, 2000);
        for (int p = 0; p < pending; p++) {
          try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
              ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
            long[] meta = new long[5];
            String[] topic = new String[1];
            NativeKafka.drainKafkaSplit(
                handle, meta, topic, outArray.memoryAddress(), outSchema.memoryAddress());
            try (VectorSchemaRoot out =
                Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries)) {
              VarBinaryVector body = (VarBinaryVector) out.getVector("body");
              for (int i = 0; i < out.getRowCount(); i++) {
                String message = new String(body.get(i), StandardCharsets.UTF_8);
                int start = message.indexOf(":") + 1;
                ids.add(Long.parseLong(message.substring(start, message.indexOf(",", start)).trim()));
              }
            }
          }
        }
      }
    } finally {
      NativeKafka.closeKafkaConsumer(handle);
    }
    return ids.size();
  }

  /** Registers SCRAM credentials through the broker's admin API, authenticated as the PLAIN admin. */
  private static void createScramUser(String bootstrap, String user, String password)
      throws Exception {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    saslPlainAdmin(props);
    try (Admin admin = Admin.create(props)) {
      admin
          .alterUserScramCredentials(
              List.of(
                  new UserScramCredentialUpsertion(
                      user, new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 4096), password)))
          .all()
          .get();
    }
  }

  private static void produceAsAdmin(String bootstrap, int messages) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    saslPlainAdmin(props);
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (int i = 0; i < messages; i++) {
        byte[] value =
            String.format("{\"id\": %d, \"name\": \"row-%d\"}", i, i)
                .getBytes(StandardCharsets.UTF_8);
        producer.send(new ProducerRecord<>(TOPIC, 0, null, value));
      }
      producer.flush();
    }
  }

  private static void saslPlainAdmin(Properties props) {
    props.put("security.protocol", "SASL_PLAINTEXT");
    props.put("sasl.mechanism", "PLAIN");
    props.put("sasl.jaas.config", ADMIN_JAAS);
  }
}

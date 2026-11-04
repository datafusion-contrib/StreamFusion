package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;
import org.apache.flink.table.types.logical.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.example.canonical.CanonicalFixtureFactory;
import org.example.canonical.CanonicalKafkaJobWorker;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@EnabledIfEnvironmentVariable(named = "SF_KAFKA_INTEGRATION", matches = "true")
class CanonicalKafkaSourceJobTest {
  private static KafkaContainer kafka;

  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  @BeforeAll
  static void startBroker() {
    kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
    kafka.start();
  }

  @AfterAll
  static void closeBroker() {
    if (kafka != null) kafka.close();
  }

  static Stream<Arguments> cases() {
    String selected = System.getProperty("sf.canonical.kafka.mode", "all");
    return Stream.of("continue", "no-input", "checkpoint", "rescale")
        .filter(mode -> selected.equals("all") || selected.equals(mode))
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        ordinary ->
                            Stream.of(false, true)
                                .filter(first -> !mode.equals("checkpoint") || first)
                                .map(first -> Arguments.of(mode, ordinary, first))));
  }

  @ParameterizedTest(name = "{0},ordinary={1},nativeFirst={2}")
  @MethodSource("cases")
  @Timeout(value = 12, unit = TimeUnit.MINUTES)
  void sourceOffsetsAndCoordinatorSurviveRepeatedCanonicalEngineSwitches(
      String mode, boolean ordinary, boolean nativeFirst) throws Exception {
    String topic = "canonical-source-" + UUID.randomUUID();
    var properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    try (var admin = Admin.create(properties)) {
      admin
          .createTopics(List.of(new NewTopic(topic, 3, (short) 1)))
          .all()
          .get(30, TimeUnit.SECONDS);
    }
    Path root = directory.resolve("switched");
    Files.createDirectories(root);
    if (ordinary) Files.createFile(root.resolve("ordinary-sql"));
    Files.writeString(
        root.resolve("input.sql"),
        "CREATE TABLE input_table(k BIGINT,n BIGINT,total BIGINT) WITH"
            + " ('connector'='kafka','topic'='"
            + topic
            + "','properties.bootstrap.servers'='"
            + kafka.getBootstrapServers()
            + "','properties.group.id'='"
            + topic
            + "','scan.startup.mode'='earliest-offset','format'='json')");
    if (mode.equals("rescale")) {
      Files.createFile(root.resolve("disable-chaining"));
      Files.writeString(root.resolve("prefix-parallelism"), "1");
      Files.writeString(root.resolve("middle-parallelism"), "3");
      Files.writeString(root.resolve("suffix-parallelism"), "2");
    }
    if (mode.equals("checkpoint")) Files.createFile(root.resolve("prefix-native-checkpoint"));
    var runner = new CanonicalJobRunner(directory, CanonicalKafkaJobWorker.class);
    Path fixture = runner.fixtureJar();
    produce(topic, 0, 5);
    runner.run(fixture, nativeFirst, root, 5, "none", "prefix", 3);
    assertRows(root, 5);
    String restore = "prefix";
    if (mode.equals("no-input")) {
      runner.run(fixture, !nativeFirst, root, 0, restore, "no-input", 3);
      runner.run(fixture, nativeFirst, root, 0, "no-input", "return", 3);
      assertRows(root, 5);
      restore = "return";
    } else if (mode.equals("checkpoint")) {
      runner.run(fixture, true, root, 0, restore, "canonical", 3);
      assertRows(root, 5);
      restore = "canonical";
    }
    produce(topic, 5, 9);
    runner.run(fixture, !nativeFirst, root, 4, restore, "middle", 3);
    assertRows(root, 9);
    produce(topic, 9, 13);
    runner.run(fixture, nativeFirst, root, 4, "middle", "suffix", 3);
    assertRows(root, 13);
    List<String> expectedIds =
        Files.readAllLines(
                root.resolve(
                    (mode.equals("checkpoint") ? "canonical" : "prefix") + "-inventory.tsv"))
            .stream()
            .map(line -> line.split("\\t", -1)[0])
            .sorted()
            .collect(Collectors.toList());
    for (String stage : List.of("prefix", "middle", "suffix")) {
      List<String> inventory = Files.readAllLines(root.resolve(stage + "-inventory.tsv"));
      boolean nativeCheckpoint = mode.equals("checkpoint") && stage.equals("prefix");
      if (!nativeCheckpoint)
        assertEquals(
            expectedIds,
            inventory.stream()
                .map(line -> line.split("\\t", -1)[0])
                .sorted()
                .collect(Collectors.toList()),
            "All connector/helper state identities must stay stable");
      var source =
          inventory.stream()
              .map(line -> line.split("\\t", -1))
              .filter(cells -> Long.parseLong(cells[4]) > 0)
              .collect(Collectors.toList());
      assertEquals(1, source.size(), "Actual Kafka enumerator coordinator state must be retained");
      assertEquals(
          3,
          Long.parseLong(source.get(0)[5]),
          "All three Kafka partition split positions must survive");
      int emitted = stage.equals("prefix") ? 5 : stage.equals("middle") ? 9 : 13;
      var offsets = new java.util.TreeMap<Integer, Long>();
      for (int partition = 0; partition < 3; partition++) offsets.put(partition, 0L);
      for (int id = 0; id < emitted; id++) offsets.merge(id % 3, 1L, Long::sum);
      String exactOffsets =
          offsets.entrySet().stream()
              .map(entry -> entry.getKey() + "=" + entry.getValue())
              .collect(Collectors.joining(","));
      assertEquals(
          exactOffsets,
          source.get(0)[6],
          "Saved split positions must equal actual broker record offsets");
      if (mode.equals("rescale"))
        assertEquals(
            stage.equals("prefix") ? 1 : stage.equals("middle") ? 3 : 2,
            Integer.parseInt(source.get(0)[2]));
      String location = Files.readString(root.resolve(stage + "-savepoint")).trim();
      String metadata =
          new String(
              Files.readAllBytes(Path.of(java.net.URI.create(location)).resolve("_metadata")),
              java.nio.charset.StandardCharsets.ISO_8859_1);
      if (!nativeCheckpoint)
        assertFalse(
            metadata.contains("tech.streamfusion"),
            "Stock metadata includes StreamFusion state classes");
      boolean nativeStage = stage.equals("middle") ? !nativeFirst : nativeFirst;
      if (nativeStage)
        assertTrue(
            Long.parseLong(Files.readString(root.resolve(stage + "-native-output"))) > 0,
            "Native Kafka source must actually emit decoded batches");
      else assertTrue(Files.exists(root.resolve(stage + "-stock-classpath-verified")));
    }
    if (ordinary) assertFalse(Files.exists(root.resolve("plan.json")));
    else assertFalse(Files.readString(root.resolve("plan.json")).contains("tech.streamfusion"));
  }

  private static void produce(String topic, int start, int end) throws Exception {
    var properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    try (var producer = new KafkaProducer<String, String>(properties)) {
      for (int id = start; id < end; id++)
        producer
            .send(
                new ProducerRecord<>(
                    topic,
                    id % 3,
                    null,
                    "{\"k\":"
                        + id
                        + ",\"n\":"
                        + (id * 10)
                        + ",\"total\":"
                        + (id % 4 == 0 ? "null" : Integer.toString(-id))
                        + "}"))
            .get(30, TimeUnit.SECONDS);
    }
  }

  private static void assertRows(Path root, int end) throws Exception {
    var type = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
    var actual =
        CanonicalFixtureFactory.committedRows(root.resolve("committed"), type).stream()
            .sorted()
            .collect(Collectors.toList());
    var expected =
        IntStream.range(0, end)
            .mapToObj(
                id ->
                    "+I|"
                        + id
                        + "|"
                        + (id * 10)
                        + "|"
                        + (id % 4 == 0 ? "null" : Integer.toString(-id)))
            .sorted()
            .collect(Collectors.toList());
    assertEquals(
        expected,
        actual,
        "Committed source data must have no loss, replay or duplicates after each switch");
  }
}

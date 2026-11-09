package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.canonical.CanonicalKafkaSinkJobWorker;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@EnabledIfEnvironmentVariable(named = "SF_KAFKA_INTEGRATION", matches = "true")
class CanonicalKafkaSinkJobTest {
  private static KafkaContainer kafka;

  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  boolean kafkaInput() {
    return false;
  }

  boolean strictBatchBoundaries() {
    return false;
  }

  boolean rescaleReaders() {
    return false;
  }

  @BeforeAll
  static void startBroker() {
    kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
            .withEnv("KAFKA_TRANSACTION_MAX_TIMEOUT_MS", "7200000");
    kafka.start();
  }

  @AfterAll
  static void closeBroker() {
    if (kafka != null) kafka.close();
  }

  static Stream<Arguments> cases() {
    String selected = System.getProperty("sf.canonical.kafka.sink.mode", "all");
    return fixtureCases(
            Stream.of("continue", "no-input", "checkpoint", "rescale", "rescale-uids")
                .filter(mode -> selected.equals("all") || selected.equals(mode)))
        .filter(test -> !test.get()[0].equals("checkpoint") || (boolean) test.get()[2]);
  }

  static Stream<Arguments> fixtureCases(Stream<String> modes) {
    boolean smoke = Boolean.getBoolean("sf.canonical.kafka.fixture.smoke");
    return modes.flatMap(
        mode ->
            Stream.of(false, true)
                .filter(ordinary -> !smoke || !ordinary)
                .flatMap(
                    ordinary ->
                        Stream.of(false, true)
                            .filter(
                                first ->
                                    !smoke
                                        || first
                                            == (mode.equals("full-batch")
                                                || mode.equals("checkpoint")))
                            .flatMap(
                                first ->
                                    CanonicalKafkaSinkJobWorker.transactionNamingStrategies()
                                        .filter(naming -> !smoke || naming.equals("incrementing"))
                                        .map(
                                            naming ->
                                                Arguments.of(mode, ordinary, first, naming)))));
  }

  @ParameterizedTest(name = "{0},ordinary={1},nativeFirst={2},naming={3}")
  @MethodSource("cases")
  @Timeout(value = 12, unit = TimeUnit.MINUTES)
  void preservesExactlyOnceSinkEffectsAcrossRepeatedCanonicalSwitches(
      String mode, boolean ordinary, boolean nativeFirst, String naming) throws Exception {
    int batchRows = mode.equals("full-batch") ? 5 : 3;
    boolean completedRecovery = mode.equals("completion-failure");
    String topic = "canonical-sink-" + UUID.randomUUID();
    String inputTopic = topic + "-input";
    var adminProperties = new Properties();
    adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    try (var admin = Admin.create(adminProperties)) {
      var topics = new ArrayList<NewTopic>();
      topics.add(new NewTopic(topic, 3, (short) 1));
      if (kafkaInput()) topics.add(new NewTopic(inputTopic, 3, (short) 1));
      admin.createTopics(topics).all().get(30, TimeUnit.SECONDS);
    }
    Path root = directory.resolve("switched");
    Files.createDirectories(root);
    Files.writeString(root.resolve("brokers"), kafka.getBootstrapServers());
    Files.writeString(root.resolve("output-topic"), topic);
    if (strictBatchBoundaries() || mode.equals("process-failure") || completedRecovery)
      Files.createFile(root.resolve("strict-batch-boundaries"));
    if (ordinary) Files.createFile(root.resolve("ordinary-sql"));
    if (rescaleReaders()) {
      assertTrue(kafkaInput());
      assertEquals("rescale-uids", mode);
      Files.createFile(root.resolve("rescale-readers"));
    }
    if (mode.equals("checkpoint")) Files.createFile(root.resolve("prefix-native-checkpoint"));
    if (mode.startsWith("rescale")) {
      if (mode.equals("rescale-uids")) Files.createFile(root.resolve("stable-uids"));
      else Files.createFile(root.resolve("disable-chaining"));
      Files.writeString(root.resolve("prefix-parallelism"), "1");
      Files.writeString(root.resolve("middle-parallelism"), "3");
      Files.writeString(root.resolve("suffix-parallelism"), "2");
    }
    if (kafkaInput()) {
      Files.createFile(root.resolve("kafka-input"));
      Files.writeString(root.resolve("brokers"), kafka.getBootstrapServers());
      Files.writeString(root.resolve("output-topic"), topic);
      Files.writeString(
          root.resolve("input.sql"),
          "CREATE TABLE input_table(k BIGINT,n BIGINT,total BIGINT) WITH"
              + " ('connector'='kafka','topic'='"
              + inputTopic
              + "','properties.bootstrap.servers'='"
              + kafka.getBootstrapServers()
              + "','properties.group.id'='"
              + inputTopic
              + "','scan.startup.mode'='earliest-offset','format'='json')");
      produce(inputTopic, 0, 5);
    } else
      Files.write(
          root.resolve("input.csv"),
          IntStream.range(0, 20)
              .mapToObj(id -> "INSERT," + id + "," + id * 10 + "," + (id % 4 == 0 ? "null" : -id))
              .collect(Collectors.toList()));
    Files.writeString(
        root.resolve("output.sql"),
        "CREATE TABLE output_table(k BIGINT,n BIGINT,total BIGINT) WITH"
            + " ('connector'='kafka','topic'='"
            + topic
            + "','properties.bootstrap.servers'='"
            + kafka.getBootstrapServers()
            + "','format'='json','sink.delivery-guarantee'='exactly-once','sink.parallelism'='%SINK_PARALLELISM%'"
            + CanonicalKafkaSinkJobWorker.transactionNamingOption(naming)
            + ",'sink.transactional-id-prefix'='"
            + topic
            + "')");
    var runner = new CanonicalJobRunner(directory, CanonicalKafkaSinkJobWorker.class);
    Path fixture = runner.fixtureJar();
    runner.run(fixture, nativeFirst, root, 5, "none", "prefix", batchRows);
    String restore = "prefix";
    if (mode.equals("checkpoint")) {
      runner.run(fixture, true, root, 5, restore, "canonical", batchRows);
      assertEquals(
          "0",
          Files.readString(root.resolve("canonical-native-rows")),
          "Checkpoint recovery must not reserialize source records");
      restore = "canonical";
    }
    assertRows(topic, 5);
    if (mode.equals("no-input")) {
      runner.run(fixture, !nativeFirst, root, 5, restore, "no-input", batchRows);
      assertRows(topic, 5);
      runner.run(fixture, nativeFirst, root, 5, "no-input", "return", batchRows);
      assertRows(topic, 5);
      restore = "return";
    }
    if (kafkaInput()) produce(inputTopic, 5, 9);
    var failures =
        java.util.Map.of(
            "fence", "fence-transactions",
            "task-failure", "terminate-task-manager",
            "process-failure", "halt-process",
            "completion-failure", "halt-after-savepoint");
    if (failures.containsKey(mode)) {
      Files.createFile(root.resolve("failed-" + failures.get(mode)));
      runner.run(
          fixture,
          !nativeFirst,
          root,
          9,
          restore,
          "failed",
          batchRows,
          "hashmap",
          mode.equals("process-failure") || completedRecovery ? 42 : 0);
      assertFalse(Files.exists(root.resolve("failed-savepoint")));
      if (mode.equals("fence")) {
        assertEquals(
            CanonicalKafkaSinkJobWorker.failureEvidence(),
            Files.readString(root.resolve("failed-failure-evidence")));
        assertTrue(Files.exists(root.resolve("failed-rejected-savepoint")));
        assertTrue(
            Long.parseLong(Files.readString(root.resolve("failed-failed-committables"))) > 0);
        assertFalse(Files.readAllLines(root.resolve("failed-fenced-transactions")).isEmpty());
        assertTrue(
            Files.readString(root.resolve("failed-inventory.tsv"))
                .contains(kafkaInput() ? "[0=3, 1=3, 2=3]" : "[9]"),
            "Fenced savepoint advances source positions despite missing external effects");
      } else if (mode.equals("process-failure")) {
        assertEquals(
            !nativeFirst && !kafkaInput() ? "8" : "9",
            Files.readString(root.resolve("failed-halt-ready")),
            "The process must die after records reached the broker, retaining its partial batch");
        if (!nativeFirst)
          assertEquals(
              kafkaInput() ? "4" : "3",
              Files.readString(root.resolve("failed-native-rows")),
              "The row source must retain one pending row; the Kafka pipeline has no transpose");
      } else if (completedRecovery) {
        assertEquals("FINISHED", Files.readString(root.resolve("failed-completion-reached")));
        assertRows(topic, 9);
        try (var files = Files.list(root.resolve("failed-completion-savepoints"))) {
          var completed =
              files
                  .filter(path -> Files.isRegularFile(path.resolve("_metadata")))
                  .collect(Collectors.toList());
          assertEquals(
              1, completed.size(), "Find the completed artifact after losing its response");
          Files.writeString(root.resolve("failed-savepoint"), completed.get(0).toUri().toString());
        }
        restore = "failed";
      } else assertTrue(Files.exists(root.resolve("failed-expected-failure")));
      if (!completedRecovery) assertRows(topic, 5);
      if (!nativeFirst)
        assertTrue(Long.parseLong(Files.readString(root.resolve("failed-native-rows"))) > 0);
      else assertTrue(Files.exists(root.resolve("failed-stock-classpath-verified")));
      if (!nativeFirst && kafkaInput())
        assertTrue(
            Long.parseLong(Files.readString(root.resolve("failed-native-source-output"))) > 0);
    }
    runner.run(
        fixture,
        completedRecovery ? nativeFirst : !nativeFirst,
        root,
        9,
        restore,
        "middle",
        batchRows);
    assertRows(topic, 9);
    if (kafkaInput()) produce(inputTopic, 9, 13);
    runner.run(
        fixture,
        completedRecovery ? !nativeFirst : nativeFirst,
        root,
        13,
        "middle",
        "suffix",
        batchRows);
    assertRows(topic, 13);
    List<String> expectedIds = null;
    for (String stage :
        completedRecovery
            ? List.of("prefix", "failed", "middle", "suffix")
            : mode.equals("checkpoint")
                ? List.of("canonical", "middle", "suffix")
                : mode.equals("no-input")
                    ? List.of("prefix", "no-input", "return", "middle", "suffix")
                    : List.of("prefix", "middle", "suffix")) {
      var inventory =
          Files.readAllLines(root.resolve(stage + "-inventory.tsv")).stream()
              .map(line -> line.split("\\t", -1))
              .collect(Collectors.toList());
      var stateIds =
          inventory.stream()
              .filter(cells -> !cells[1].isEmpty())
              .map(cells -> cells[0])
              .sorted()
              .collect(Collectors.toList());
      if (expectedIds == null) expectedIds = stateIds;
      else assertEquals(expectedIds, stateIds, "Source/writer/committer IDs must survive switches");
      int limit =
          stage.equals("middle") || stage.equals("failed") ? 9 : stage.equals("suffix") ? 13 : 5;
      int incoming = 0;
      if (stage.equals("prefix")) incoming = 5;
      else if (stage.equals("failed")
          || stage.equals("suffix")
          || stage.equals("middle") && !completedRecovery) incoming = 4;
      assertEquals(
          mode.equals("rescale-uids") ? "ALWAYS" : "DISABLED",
          Files.readString(root.resolve(stage + "-uid-policy")));
      assertEquals(
          !mode.equals("rescale"),
          Boolean.parseBoolean(Files.readString(root.resolve(stage + "-chaining"))));
      String expectedSource = "[" + limit + "]";
      if (kafkaInput()) {
        var positions = new java.util.TreeMap<Integer, Integer>();
        for (int partition = 0; partition < 3; partition++) positions.put(partition, 0);
        for (int id = 0; id < limit; id++) positions.merge(id % 3, 1, Integer::sum);
        expectedSource =
            positions.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.toList())
                .toString();
        assertEquals(
            1,
            inventory.stream().filter(cells -> Long.parseLong(cells[8]) > 0).count(),
            "Kafka source coordinator state must survive the sink handoff");
      }
      assertEquals(
          List.of(expectedSource),
          inventory.stream()
              .filter(cells -> cells[1].contains("SourceReaderState"))
              .map(cells -> cells[2])
              .collect(Collectors.toList()),
          "Custom source saved offsets must be exact");
      int writers =
          mode.startsWith("rescale")
              ? stage.equals("middle") ? 3 : stage.equals("suffix") ? 2 : 1
              : 1;
      if (rescaleReaders()) {
        var sources =
            inventory.stream()
                .filter(cells -> cells[1].contains("SourceReaderState"))
                .collect(Collectors.toList());
        assertEquals(1, sources.size());
        assertEquals(
            writers,
            Integer.parseInt(sources.get(0)[4]),
            "Reader parallelism must change with the writers");
        assertEquals(
            128, Integer.parseInt(sources.get(0)[5]), "Source maximum parallelism must stay fixed");
      }
      int owned =
          naming.equals("pooling") && mode.startsWith("rescale") && stage.equals("suffix")
              ? 3
              : writers;
      var writerState =
          inventory.stream()
              .filter(cells -> cells[1].contains("writer_raw_states"))
              .collect(Collectors.toList());
      assertEquals(1, writerState.size());
      assertEquals(
          "[" + String.join(", ", java.util.Collections.nCopies(owned, topic)) + "]",
          writerState.get(0)[3],
          "Kafka writer transaction identity must be retained");
      assertEquals(
          writers,
          Integer.parseInt(writerState.get(0)[4]),
          "Writer parallelism must actually change");
      assertEquals(
          IntStream.range(0, owned)
              .mapToObj(
                  id ->
                      id
                          + "/"
                          + owned
                          + "/"
                          + CanonicalKafkaSinkJobWorker.transactionOwnership(naming))
              .collect(Collectors.toList())
              .toString(),
          writerState.get(0)[6],
          "Saved transaction ownership must match the released rescaling contract");
      if (incoming > 0)
        assertTrue(
            inventory.stream().mapToInt(cells -> Integer.parseInt(cells[7])).sum() > 0,
            "The handoff must retain actual precommitted transaction state: stage="
                + stage
                + ", mode="
                + mode
                + ", ordinary="
                + ordinary
                + ", nativeFirst="
                + nativeFirst
                + ", naming="
                + naming
                + ", committedBeforeSavepoint="
                + Files.readString(root.resolve(stage + "-committed-before-savepoint"))
                + ", inventory="
                + Files.readString(root.resolve(stage + "-inventory.tsv")));
      String saved = Files.readString(root.resolve(stage + "-savepoint")).trim();
      String metadata =
          new String(
              Files.readAllBytes(Path.of(java.net.URI.create(saved)).resolve("_metadata")),
              java.nio.charset.StandardCharsets.ISO_8859_1);
      assertFalse(metadata.contains("tech.streamfusion"), "Non-stock state class in metadata");
      boolean nativeStage = nativeFirst;
      if (stage.equals("failed") || stage.equals("no-input")) nativeStage = !nativeFirst;
      else if (stage.equals("middle")) nativeStage = completedRecovery ? nativeFirst : !nativeFirst;
      else if (stage.equals("suffix") && completedRecovery) nativeStage = !nativeFirst;
      if (nativeStage)
        assertEquals(
            incoming,
            Long.parseLong(Files.readString(root.resolve(stage + "-native-rows"))),
            "Native encoder must process every row, including the partial barrier batch");
      else assertTrue(Files.exists(root.resolve(stage + "-stock-classpath-verified")));
      if (nativeStage && !kafkaInput() && Files.exists(root.resolve("strict-batch-boundaries"))) {
        long full = incoming - incoming % batchRows;
        assertEquals(
            incoming + "/" + full + "/" + (incoming - full),
            Files.readString(root.resolve(stage + "-batch-boundary")),
            "Actual transpose input and encoded rows must prove the batch boundary before saving");
      }
      if (nativeStage && kafkaInput() && incoming > 0)
        assertTrue(
            Long.parseLong(Files.readString(root.resolve(stage + "-native-source-output"))) > 0,
            "Both native endpoints must actually process data");
    }
  }

  private static void produce(String topic, int start, int end) throws Exception {
    var properties = new Properties();
    properties.setProperty("bootstrap.servers", kafka.getBootstrapServers());
    properties.setProperty(
        "key.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());
    properties.setProperty(
        "value.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());
    try (var producer =
        new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties)) {
      for (int id = start; id < end; id++)
        producer
            .send(
                new org.apache.kafka.clients.producer.ProducerRecord<>(
                    topic,
                    id % 3,
                    null,
                    "{\"k\":"
                        + id
                        + ",\"n\":"
                        + id * 10
                        + ",\"total\":"
                        + (id % 4 == 0 ? "null" : -id)
                        + "}"))
            .get(30, TimeUnit.SECONDS);
    }
  }

  private static void assertRows(String topic, int end) throws Exception {
    var properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    var actual = new ArrayList<String>();
    try (var consumer = new KafkaConsumer<String, String>(properties)) {
      var partitions =
          consumer.partitionsFor(topic).stream()
              .map(info -> new TopicPartition(topic, info.partition()))
              .collect(Collectors.toList());
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      var ends = consumer.endOffsets(partitions);
      long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
      while (true) {
        for (var record : consumer.poll(Duration.ofMillis(100))) {
          var json =
              new org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper()
                  .readTree(record.value());
          actual.add(
              json.get("k").asText()
                  + "|"
                  + json.get("n").asText()
                  + "|"
                  + json.get("total").asText());
        }
        if (partitions.stream().allMatch(p -> consumer.position(p) >= ends.get(p))) break;
        if (System.nanoTime() > deadline)
          fail("Committed consumer did not reach the saved end offsets");
      }
    }
    var expected =
        IntStream.range(0, end)
            .mapToObj(id -> id + "|" + id * 10 + "|" + (id % 4 == 0 ? "null" : -id))
            .sorted()
            .collect(Collectors.toList());
    java.util.Collections.sort(actual);
    assertEquals(expected, actual, "Committed Kafka effects must have no loss or duplication");
  }
}

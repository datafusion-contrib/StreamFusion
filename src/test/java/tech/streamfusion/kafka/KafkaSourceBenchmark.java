package tech.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import tech.streamfusion.planner.NativePlanner;

/**
 * Complete-job comparison for Kafka source shapes: stock Flink, the previous StreamFusion route
 * (Flink's Kafka source and deserializer, a RowData→Arrow transpose, then native operators) and the
 * native decode. Each workload's corpus is produced once into a real Kafka topic, then every trial
 * runs the three engines in rotated order over the same bounded offsets with identical
 * parallelism, resources and mini-batch aggregation settings. Timing spans planning and submission
 * through the last result row; production, plan inspection and output comparison are untimed.
 * Before a size's runs, each route's plan is checked once for its intended route; every run then
 * checks that all three outputs collapse to the same multiset, keeping both outputs beside the
 * observations when they don't.
 *
 * <p>Opt-in via {@code SF_BENCHMARK=true}; run with the release profile ({@code -Pbench}). Select
 * workloads, sizes, warmups and trials with the {@code kafka.bench.*} system properties; each run
 * appends one JSON line to {@code observations.jsonl} under {@code kafka.bench.output}, and each
 * route's plan is written beside it. Set {@code kafka.bench.brokers} to measure against an existing
 * broker instead of a Testcontainers one, whose port forwarding can bound throughput (Docker
 * Desktop on macOS does); each run creates and deletes its own topics there.
 */
@Tag("streamfusion-kafka")
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class KafkaSourceBenchmark {

  private static final String DECODE_SWITCH = "streamfusion.operator.kafkaDecode.enabled";
  private static final int PARTITIONS = Integer.getInteger("kafka.bench.partitions", 4);
  private static final int PARALLELISM = Integer.getInteger("kafka.bench.parallelism", 4);
  private static final int WARMUPS = Integer.getInteger("kafka.bench.warmups", 2);
  private static final int TRIALS = Integer.getInteger("kafka.bench.trials", 6);

  enum Route {
    STOCK,
    PREVIOUS,
    NATIVE
  }

  /** A measured job: its table DDL, query and deterministic corpus. */
  interface Workload {
    String id();

    String ddl(String brokers, String topic);

    String query();

    /** The {@code index}-th of {@code records} messages, already assigned to its partition. */
    ProducerRecord<byte[], byte[]> record(String topic, long index, long records);
  }

  private static final Map<String, Workload> WORKLOADS = new LinkedHashMap<>();

  static {
    register(new NestedCdc("nested-maxwell", "maxwell-json"));
    register(new NestedCdc("nested-canal", "canal-json"));
    register(new CanalTables("canal-one-of-ten-tables", 10, "t0"));
    register(new CanalTables("canal-all-ten-tables", 10, "t\\d"));
    register(new CanalTables("canal-sharded-tables", 1_000_000, "t\\d*0"));
    register(new JsonMetadata());
  }

  private static void register(Workload workload) {
    WORKLOADS.put(workload.id(), workload);
  }

  @Test
  void compareRoutes() throws Exception {
    List<Workload> selected = new ArrayList<>();
    for (String id :
        System.getProperty("kafka.bench.workloads", String.join(",", WORKLOADS.keySet()))
            .split(",")) {
      Workload workload = WORKLOADS.get(id.trim());
      assertTrue(workload != null, "unknown workload " + id);
      selected.add(workload);
    }
    long[] sizes =
        Arrays.stream(System.getProperty("kafka.bench.records", "1000000").split(","))
            .mapToLong(size -> Long.parseLong(size.trim()))
            .toArray();
    Path output = Path.of(System.getProperty("kafka.bench.output", "target/kafka-source-bench"));
    Files.createDirectories(output);
    String external = System.getProperty("kafka.bench.brokers");
    String nonce = Long.toString(System.currentTimeMillis(), 36);
    try (KafkaContainer kafka =
        external != null
            ? null
            : new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      String brokers = external;
      if (kafka != null) {
        kafka.start();
        brokers = kafka.getBootstrapServers();
      }
      for (Workload workload : selected) {
        for (long records : sizes) {
          String name = workload.id() + "-" + records;
          String topic = name + "-" + nonce;
          produce(brokers, topic, workload, records);
          for (Route route : Route.values()) {
            Files.writeString(
                output.resolve(name + "-" + route.name().toLowerCase(Locale.ROOT) + ".plan"),
                plan(workload, route, brokers, topic));
          }
          for (int run = 0; run < WARMUPS + TRIALS; run++) {
            boolean warmup = run < WARMUPS;
            Measurement expected = null;
            for (int position = 0; position < 3; position++) {
              Route route = Route.values()[(run + position) % 3];
              Measurement measured = measure(workload, route, brokers, topic);
              if (expected == null) {
                expected = measured;
              }
              if (!expected.digest.equals(measured.digest)) {
                // Keep both outputs beside the observations so the difference can be inspected.
                Files.write(
                    output.resolve(name + "-run" + run + "-expected.rows"), expected.lines());
                Files.write(
                    output.resolve(name + "-run" + run + "-" + route + ".rows"), measured.lines());
              }
              assertEquals(
                  expected.digest,
                  measured.digest,
                  workload.id() + " " + route + " output differs in run " + run);
              Files.writeString(
                  output.resolve("observations.jsonl"),
                  String.format(
                      Locale.ROOT,
                      "{\"workload\":\"%s\",\"records\":%d,\"partitions\":%d,"
                          + "\"parallelism\":%d,\"route\":\"%s\",\"run\":%d,"
                          + "\"warmup\":%s,\"position\":%d,\"seconds\":%.9f,"
                          + "\"rows\":%d,\"digest\":\"%s\"}%n",
                      workload.id(),
                      records,
                      PARTITIONS,
                      PARALLELISM,
                      route.name().toLowerCase(Locale.ROOT),
                      run,
                      warmup,
                      position,
                      measured.seconds,
                      measured.rows,
                      measured.digest),
                  StandardOpenOption.CREATE,
                  StandardOpenOption.APPEND);
              System.out.printf(
                  Locale.ROOT,
                  "[kafka-source-bench] %s records=%d route=%s run=%d%s seconds=%.3f%n",
                  workload.id(),
                  records,
                  route,
                  run,
                  warmup ? " (warmup)" : "",
                  measured.seconds);
            }
          }
          deleteTopic(brokers, topic);
        }
      }
    }
  }

  /** Plans the query on {@code route} and checks it took exactly that route. Untimed. */
  private static String plan(Workload workload, Route route, String brokers, String topic) {
    String previous = System.getProperty(DECODE_SWITCH);
    try {
      routeSwitch(route);
      StreamTableEnvironment tEnv = environment(workload, brokers, topic);
      String plan =
          route == Route.STOCK
              ? tEnv.explainSql(workload.query())
              : NativePlanner.explain(tEnv, workload.query());
      if (route == Route.NATIVE) {
        assertTrue(plan.contains("NativeKafkaDecode"), "native route lacks the decode:\n" + plan);
      } else {
        assertFalse(
            plan.contains("NativeKafkaDecode"), route + " route decodes natively:\n" + plan);
      }
      if (route == Route.PREVIOUS) {
        assertTrue(plan.contains("Native"), "previous route runs no native operator:\n" + plan);
      }
      return plan;
    } finally {
      restore(previous);
    }
  }

  private static Measurement measure(Workload workload, Route route, String brokers, String topic)
      throws Exception {
    String previous = System.getProperty(DECODE_SWITCH);
    try {
      routeSwitch(route);
      System.gc();
      Thread.sleep(500);
      long start = System.nanoTime();
      StreamTableEnvironment tEnv = environment(workload, brokers, topic);
      if (route != Route.STOCK) {
        NativePlanner.install(tEnv);
      }
      Map<String, Long> collapsed = new HashMap<>();
      long rows = 0;
      try (CloseableIterator<Row> iterator = tEnv.executeSql(workload.query()).collect()) {
        while (iterator.hasNext()) {
          Row row = iterator.next();
          RowKind kind = row.getKind();
          row.setKind(RowKind.INSERT);
          long delta = kind == RowKind.DELETE || kind == RowKind.UPDATE_BEFORE ? -1 : 1;
          collapsed.merge(row.toString(), delta, Long::sum);
          rows++;
        }
      }
      double seconds = (System.nanoTime() - start) / 1e9;
      collapsed.values().removeIf(count -> count == 0);
      MessageDigest sha = MessageDigest.getInstance("SHA-256");
      for (Map.Entry<String, Long> entry : new TreeMap<>(collapsed).entrySet()) {
        sha.update(
            (entry.getKey() + "=" + entry.getValue() + "\n").getBytes(StandardCharsets.UTF_8));
      }
      StringBuilder digest = new StringBuilder();
      for (byte b : sha.digest()) {
        digest.append(String.format("%02x", b));
      }
      return new Measurement(seconds, rows, digest.toString(), new TreeMap<>(collapsed));
    } finally {
      restore(previous);
    }
  }

  private static StreamTableEnvironment environment(
      Workload workload, String brokers, String topic) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(PARALLELISM);
    StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(
            env, EnvironmentSettings.newInstance().inStreamingMode().build());
    tEnv.getConfig().set("table.local-time-zone", "UTC");
    // Without mini-batching an aggregate over a changelog emits an update per input row, and the
    // result fetch then bounds every route alike instead of the source being measured.
    tEnv.getConfig().set("table.exec.mini-batch.enabled", "true");
    tEnv.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
    tEnv.getConfig().set("table.exec.mini-batch.size", "50000");
    tEnv.executeSql(workload.ddl(brokers, topic));
    return tEnv;
  }

  private static void routeSwitch(Route route) {
    if (route == Route.PREVIOUS) {
      System.setProperty(DECODE_SWITCH, "false");
    } else {
      System.clearProperty(DECODE_SWITCH);
    }
  }

  private static void restore(String previous) {
    if (previous == null) {
      System.clearProperty(DECODE_SWITCH);
    } else {
      System.setProperty(DECODE_SWITCH, previous);
    }
  }

  private static void produce(String brokers, String topic, Workload workload, long records)
      throws Exception {
    Properties admin = new Properties();
    admin.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    try (AdminClient client = AdminClient.create(admin)) {
      // A workload may stamp its records with fixed historic timestamps; time retention must not
      // delete them while the routes are still reading.
      client
          .createTopics(
              List.of(
                  new NewTopic(topic, PARTITIONS, (short) 1)
                      .configs(Map.of("retention.ms", "-1"))))
          .all()
          .get();
    }
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "1048576");
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (long index = 0; index < records; index++) {
        producer.send(workload.record(topic, index, records));
      }
      producer.flush();
    }
  }

  private static void deleteTopic(String brokers, String topic) throws Exception {
    Properties admin = new Properties();
    admin.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    try (AdminClient client = AdminClient.create(admin)) {
      client.deleteTopics(List.of(topic)).all().get();
    }
  }

  private static final class Measurement {
    final double seconds;
    final long rows;
    final String digest;
    final Map<String, Long> collapsed;

    Measurement(double seconds, long rows, String digest, Map<String, Long> collapsed) {
      this.seconds = seconds;
      this.rows = rows;
      this.digest = digest;
      this.collapsed = collapsed;
    }

    List<String> lines() {
      List<String> lines = new ArrayList<>();
      collapsed.forEach((row, count) -> lines.add(row + "=" + count));
      return lines;
    }
  }

  private static String bounded(
      String brokers, String topic, String format, String extraOptions) {
    return " WITH ('connector' = 'kafka', 'topic' = '"
        + topic
        + "', 'properties.bootstrap.servers' = '"
        + brokers
        + "', 'properties.max.poll.records' = '8192', 'scan.startup.mode' = 'earliest-offset',"
        + " 'scan.bounded.mode' = 'latest-offset', 'format' = '"
        + format
        + "'"
        + extraOptions
        + ")";
  }

  /**
   * Maxwell or Canal changes for nested rows: every key is inserted, updated twice through `old`
   * images that carry nested pre-images (so `findValue` meets column names inside them), and then
   * either updated again or deleted. A key's changes stay in one partition, in order; Canal
   * messages carry two keys of the same partition. Integer aggregates keep the final result exact
   * however partitions interleave.
   */
  private static final class NestedCdc implements Workload {
    private final String id;
    private final String format;

    NestedCdc(String id, String format) {
      this.id = id;
      this.format = format;
    }

    @Override
    public String id() {
      return id;
    }

    @Override
    public String ddl(String brokers, String topic) {
      return "CREATE TABLE cdc (id BIGINT, tenant STRING, name STRING, amount BIGINT,"
          + " info ROW<name STRING, score BIGINT, tags ARRAY<STRING>>,"
          + " attrs MAP<STRING, BIGINT>, items ARRAY<ROW<sku STRING, qty INT>>)"
          + bounded(brokers, topic, format, "");
    }

    @Override
    public String query() {
      return "SELECT tenant, COUNT(*) AS changes, SUM(amount) AS amount,"
          + " SUM(info.score) AS score, COUNT(name) AS named FROM cdc GROUP BY tenant";
    }

    @Override
    public ProducerRecord<byte[], byte[]> record(String topic, long index, long records) {
      boolean canal = "canal-json".equals(format);
      long rounds = 4;
      long perRound = Math.max(1, records / rounds);
      long round = Math.min(rounds - 1, index / perRound);
      long slot = index % perRound;
      long[] keys = canal ? new long[] {2 * slot, 2 * slot + 1} : new long[] {slot};
      int partition = (int) (slot % PARTITIONS);
      boolean delete = round == 3 && slot % 4 == 0;
      StringBuilder data = new StringBuilder();
      StringBuilder old = new StringBuilder();
      for (int i = 0; i < keys.length; i++) {
        data.append(i == 0 ? "" : ",").append(row(keys[i], delete ? round - 1 : round));
        old.append(i == 0 ? "" : ",").append(changed(keys[i], round - 1));
      }
      String type;
      String body;
      if (round == 0) {
        type = canal ? "INSERT" : "insert";
        body = "";
      } else if (delete) {
        type = canal ? "DELETE" : "delete";
        body = "";
      } else {
        type = canal ? "UPDATE" : "update";
        body = canal ? ",\"old\":[" + old + "]" : ",\"old\":" + old;
      }
      String value =
          canal
              ? "{\"data\":[" + data + "]" + body + ",\"database\":\"shop\",\"table\":\"orders\","
                  + "\"es\":1589373560000,\"ts\":1589373560798,\"isDdl\":false,\"type\":\""
                  + type + "\"}"
              : "{\"database\":\"shop\",\"table\":\"orders\",\"type\":\"" + type
                  + "\",\"ts\":1589373560,\"data\":" + data + body + "}";
      return new ProducerRecord<>(
          topic,
          partition,
          Long.toString(keys[0]).getBytes(StandardCharsets.UTF_8),
          value.getBytes(StandardCharsets.UTF_8));
    }

    private static String row(long key, long round) {
      return "{\"id\":" + key + ",\"tenant\":\"t" + (key % 64) + "\",\"name\":\"n" + key
          + "\",\"amount\":" + (key % 1000 + 25 * round) + ","
          + changedColumns(key, round) + "}";
    }

    private static String changed(long key, long round) {
      return "{\"amount\":" + (key % 1000 + 25 * round) + "," + changedColumns(key, round) + "}";
    }

    private static String changedColumns(long key, long round) {
      return "\"info\":{\"name\":\"i" + (key % 97) + "\",\"score\":" + (key % 13 + round)
          + ",\"tags\":[\"a\",\"b" + round + "\"]},\"attrs\":{\"a\":" + (key % 5) + ",\"b\":"
          + round + "},\"items\":[{\"sku\":\"s" + (key % 11) + "\",\"qty\":" + (round + 1)
          + "}]";
    }
  }

  /**
   * A Canal topic multiplexing one database's tables, the shape Canal's include filters exist for:
   * messages rotate through {@code tables} tables with Canal's own envelope fields and string
   * values, and the measured table admits {@code pattern}'s share, spread over every partition.
   * A million sharded tables make nearly every value distinct, so the native verdict cache misses.
   */
  private static final class CanalTables implements Workload {
    private final String id;
    private final long tables;
    private final String pattern;

    CanalTables(String id, long tables, String pattern) {
      this.id = id;
      this.tables = tables;
      this.pattern = pattern;
    }

    @Override
    public String id() {
      return id;
    }

    @Override
    public String ddl(String brokers, String topic) {
      return "CREATE TABLE cdc (id BIGINT, tenant STRING, name STRING, amount BIGINT)"
          + bounded(
              brokers,
              topic,
              "canal-json",
              ", 'canal-json.database.include' = 'shop', 'canal-json.table.include' = '"
                  + pattern
                  + "'");
    }

    @Override
    public String query() {
      return "SELECT tenant, COUNT(*) AS changes, SUM(amount) AS amount FROM cdc GROUP BY tenant";
    }

    @Override
    public ProducerRecord<byte[], byte[]> record(String topic, long index, long records) {
      String value =
          "{\"data\":[{\"id\":\"" + index + "\",\"tenant\":\"t" + (index % 64)
              + "\",\"name\":\"n" + index + "\",\"amount\":\"" + (index % 1000) + "\"}],"
              + "\"database\":\"shop\",\"es\":1589373560000,\"id\":" + index
              + ",\"isDdl\":false,\"mysqlType\":{\"id\":\"bigint(20)\","
              + "\"tenant\":\"varchar(16)\",\"name\":\"varchar(64)\",\"amount\":\"bigint(20)\"},"
              + "\"old\":null,\"pkNames\":[\"id\"],\"sql\":\"\",\"sqlType\":{\"id\":-5,"
              + "\"tenant\":12,\"name\":12,\"amount\":-5},\"table\":\"t" + (index % tables)
              + "\",\"ts\":1589373560798,\"type\":\"INSERT\"}";
      return new ProducerRecord<>(
          topic,
          (int) ((index / 10) % PARTITIONS),
          Long.toString(index).getBytes(StandardCharsets.UTF_8),
          value.getBytes(StandardCharsets.UTF_8));
    }
  }

  /**
   * JSON records read with connector metadata: each carries three headers and its own timestamp,
   * and the query aggregates the partition, offset, timestamp and a header lookup with the values.
   */
  private static final class JsonMetadata implements Workload {
    @Override
    public String id() {
      return "json-metadata";
    }

    @Override
    public String ddl(String brokers, String topic) {
      return "CREATE TABLE cdc (id BIGINT, tenant STRING, name STRING, amount BIGINT,"
          + " part INT METADATA FROM 'partition', off BIGINT METADATA FROM 'offset',"
          + " ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',"
          + " hdrs MAP<STRING, BYTES> METADATA FROM 'headers')"
          + bounded(brokers, topic, "json", "");
    }

    @Override
    public String query() {
      return "SELECT tenant, COUNT(*) AS records, SUM(amount) AS amount, MAX(part) AS part,"
          + " MAX(off) AS last_offset, MAX(ts) AS latest, COUNT(hdrs['trace']) AS traced"
          + " FROM cdc GROUP BY tenant";
    }

    @Override
    public ProducerRecord<byte[], byte[]> record(String topic, long index, long records) {
      String value =
          "{\"id\":" + index + ",\"tenant\":\"t" + (index % 64) + "\",\"name\":\"n" + index
              + "\",\"amount\":" + (index % 1000) + "}";
      return new ProducerRecord<>(
          topic,
          (int) (index % PARTITIONS),
          1_600_000_000_000L + index,
          null,
          value.getBytes(StandardCharsets.UTF_8),
          List.<Header>of(
              new RecordHeader("trace", Long.toString(index).getBytes(StandardCharsets.UTF_8)),
              new RecordHeader("source", "orders".getBytes(StandardCharsets.UTF_8)),
              new RecordHeader(
                  "tenant", ("t" + (index % 64)).getBytes(StandardCharsets.UTF_8))));
    }
  }
}

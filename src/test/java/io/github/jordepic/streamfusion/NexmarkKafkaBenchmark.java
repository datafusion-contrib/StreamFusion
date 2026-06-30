package io.github.jordepic.streamfusion;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import io.github.jordepic.streamfusion.proto.NexmarkAuction;
import io.github.jordepic.streamfusion.proto.NexmarkBid;
import io.github.jordepic.streamfusion.proto.NexmarkEvent;
import io.github.jordepic.streamfusion.proto.NexmarkPerson;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Kafka-sourced Nexmark q0–q2: the wide event row arrives as JSON on a topic, read through a {@code
 * 'format'='json'} table. Native decode (Flink consumes raw bytes, a native operator decodes them to
 * Arrow, with the query's projection pushed into the decode) is compared to Flink's own {@code json}
 * format, over the same bytes and the same query. Flink does not push projection into the Kafka scan,
 * so it decodes the whole record; the native decoder builds only the columns/nested fields the query
 * reads — the win this benchmark isolates. Complements {@link NexmarkBenchmark} (the generator source).
 *
 * <p>Opt-in: {@code SF_BENCHMARK=true} (Docker for Testcontainers Kafka). {@code dateTime}/{@code
 * expires} are epoch-millis BIGINT here (the decode cost is a long either way; avoids JSON timestamp
 * parsing noise). {@code SF_ROWS} overrides the event count (default 1,000,000).
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NexmarkKafkaBenchmark {

  static final long ROWS =
      System.getenv("SF_ROWS") != null ? Long.parseLong(System.getenv("SF_ROWS")) : 1_000_000L;
  private static final int WARMUP = 1;
  private static final int RUNS = 2;
  private static final int BLOCK = 50;
  private static final String[] STATES = {"OR", "ID", "CA", "WA", "NY", "TX"};

  static final String SCHEMA =
      "event_type INT,"
          + " person ROW<id BIGINT, name STRING, emailAddress STRING, creditCard STRING, city STRING,"
          + " state STRING, `dateTime` BIGINT, extra STRING>,"
          + " auction ROW<id BIGINT, itemName STRING, description STRING, initialBid BIGINT,"
          + " reserve BIGINT, `dateTime` BIGINT, expires BIGINT, seller BIGINT, category BIGINT,"
          + " extra STRING>,"
          + " bid ROW<auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING,"
          + " `dateTime` BIGINT, extra STRING>,"
          + " `dateTime` BIGINT";

  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PROFILE", matches = "true")
  void q0NativeProfileLoop() throws Exception {
    String format = System.getProperty("profile.format", "json");
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      produce(brokers, "nexmark", format);
      String sinkDdl =
          "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price BIGINT, `dateTime` BIGINT,"
              + " extra STRING) WITH ('connector' = 'blackhole')";
      String insertSql =
          "INSERT INTO sink SELECT bid.auction, bid.bidder, bid.price, bid.`dateTime`, bid.extra"
              + " FROM src WHERE event_type = 2";
      long deadline = System.currentTimeMillis() + Long.getLong("profile.seconds", 60L) * 1000L;
      long iterations = 0;
      while (System.currentTimeMillis() < deadline) {
        runOnce(brokers, format, true, sinkDdl, insertSql);
        iterations++;
      }
      System.out.println("[profile] native Kafka/" + format + " q0 iterations: " + iterations);
    }
  }

  @Test
  void nexmarkKafkaJson() throws Exception {
    runFormat("json");
  }

  @Test
  void nexmarkKafkaAvro() throws Exception {
    runFormat("avro");
  }

  @Test
  void nexmarkKafkaProtobuf() throws Exception {
    runFormat("protobuf");
  }

  private void runFormat(String format) throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      produce(brokers, "nexmark", format);

      compare(
          brokers,
          format,
          "q0 pass-through (project bid fields)",
          false,
          "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price BIGINT, `dateTime` BIGINT,"
              + " extra STRING) WITH ('connector' = 'blackhole')",
          "INSERT INTO sink SELECT bid.auction, bid.bidder, bid.price, bid.`dateTime`, bid.extra"
              + " FROM src WHERE event_type = 2");
      compare(
          brokers,
          format,
          "q1 currency conversion (0.908 * price)",
          true,
          "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price DECIMAL(23, 3), `dateTime` BIGINT,"
              + " extra STRING) WITH ('connector' = 'blackhole')",
          "INSERT INTO sink SELECT bid.auction, bid.bidder, 0.908 * bid.price, bid.`dateTime`,"
              + " bid.extra FROM src WHERE event_type = 2");
      compare(
          brokers,
          format,
          "q2 filter (MOD(auction, 123) = 0)",
          false,
          "CREATE TABLE sink (auction BIGINT, price BIGINT) WITH ('connector' = 'blackhole')",
          "INSERT INTO sink SELECT bid.auction, bid.price FROM src WHERE event_type = 2"
              + " AND MOD(bid.auction, 123) = 0");
    }
  }

  private static void compare(
      String brokers,
      String format,
      String label,
      boolean approximateDecimal,
      String sinkDdl,
      String insertSql)
      throws Exception {
    double flink = bestOf(brokers, format, false, approximateDecimal, sinkDdl, insertSql);
    double nativeRun = bestOf(brokers, format, true, approximateDecimal, sinkDdl, insertSql);
    System.out.printf(
        "%n[benchmark] Kafka/%s %s over %,d events (best of %d)%n",
        format.toUpperCase(java.util.Locale.ROOT), label, ROWS, RUNS);
    System.out.printf("[benchmark]   Flink : %6.3f s  (%,.0f events/s)%n", flink, ROWS / flink);
    System.out.printf(
        "[benchmark]   Native: %6.3f s  (%,.0f events/s)  %.2fx vs Flink%n",
        nativeRun, ROWS / nativeRun, flink / nativeRun);
  }

  private static double bestOf(
      String brokers,
      String format,
      boolean useNative,
      boolean approximateDecimal,
      String sinkDdl,
      String insertSql)
      throws Exception {
    String property = "streamfusion.expression.decimalArithmetic.approximate";
    String previous = System.getProperty(property);
    if (useNative && approximateDecimal) {
      System.setProperty(property, "true");
    }
    try {
      double best = Double.MAX_VALUE;
      for (int run = 0; run < WARMUP + RUNS; run++) {
        double seconds = runOnce(brokers, format, useNative, sinkDdl, insertSql);
        if (run >= WARMUP) {
          best = Math.min(best, seconds);
        }
      }
      return best;
    } finally {
      if (previous == null) {
        System.clearProperty(property);
      } else {
        System.setProperty(property, previous);
      }
    }
  }

  private static double runOnce(
      String brokers, String format, boolean useNative, String sinkDdl, String insertSql)
      throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.getConfig().enableObjectReuse();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(
        "CREATE TABLE src ("
            + SCHEMA
            + ") WITH ('connector' = 'kafka', 'topic' = 'nexmark', 'properties.bootstrap.servers' = '"
            + brokers
            + "', 'properties.group.id' = 'nexmark', 'scan.startup.mode' = 'earliest-offset',"
            + " 'scan.bounded.mode' = 'latest-offset', 'format' = '"
            + format
            + "'"
            + ("protobuf".equals(format)
                ? ", 'protobuf.message-class-name' = 'io.github.jordepic.streamfusion.proto.NexmarkEvent'"
                : "")
            + ")");
    PhysicalPlanScan scan = useNative ? NativePlanner.install(tEnv) : null;
    tEnv.executeSql(sinkDdl);
    long start = System.nanoTime();
    tEnv.executeSql(insertSql).await();
    double seconds = (System.nanoTime() - start) / 1e9;
    if (useNative && scan.substitutions() == 0) {
      throw new IllegalStateException(
          "native decode did not engage; comparison is moot. " + scan.fallbackReasons());
    }
    return seconds;
  }

  static void produce(String brokers, String topic, String format) throws Exception {
    produce(brokers, topic, format, ROWS);
  }

  static void produce(String brokers, String topic, String format, long rows) throws Exception {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1 << 20);
    boolean avro = "avro".equals(format);
    boolean protobuf = "protobuf".equals(format);
    Schema schema = avro ? AvroSchemaConverter.convertToSchema(nexmarkRowType().copy(false)) : null;
    GenericDatumWriter<GenericRecord> writer = avro ? new GenericDatumWriter<>(schema) : null;
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (long i = 0; i < rows; i++) {
        byte[] value;
        if (protobuf) {
          value = protobufEvent(i).toByteArray();
        } else if (avro) {
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
          writer.write(avroEvent(i, schema), encoder);
          encoder.flush();
          value = out.toByteArray();
        } else {
          value = event(i).getBytes(StandardCharsets.UTF_8);
        }
        producer.send(new ProducerRecord<>(topic, 0, null, value));
      }
      producer.flush();
    }
  }

  /** One wide event as JSON, the inactive structs null — the Nexmark person/auction/bid mix. */
  private static String event(long i) {
    long block = i / BLOCK;
    int pos = (int) (i % BLOCK);
    long ts = block * 1000L + pos * 10L;
    if (pos == 0) {
      return String.format(
          "{\"event_type\":0,\"person\":{\"id\":%d,\"name\":\"p-%d\",\"emailAddress\":\"e-%d\","
              + "\"creditCard\":\"1234\",\"city\":\"c-%d\",\"state\":\"%s\",\"dateTime\":%d,"
              + "\"extra\":\"x\"},\"auction\":null,\"bid\":null,\"dateTime\":%d}",
          block, block, block, block % 1000, STATES[(int) (block % STATES.length)], ts, ts);
    }
    if (pos <= 3) {
      long auctionId = block * 3 + (pos - 1);
      return String.format(
          "{\"event_type\":1,\"person\":null,\"auction\":{\"id\":%d,\"itemName\":\"i-%d\","
              + "\"description\":\"d-%d\",\"initialBid\":10,\"reserve\":50,\"dateTime\":%d,"
              + "\"expires\":%d,\"seller\":%d,\"category\":%d,\"extra\":\"x\"},\"bid\":null,"
              + "\"dateTime\":%d}",
          auctionId, auctionId, auctionId, ts, ts + 20000, block, block % 100, ts);
    }
    long auctionId = block * 3 + (pos % 3);
    return String.format(
        "{\"event_type\":2,\"person\":null,\"auction\":null,\"bid\":{\"auction\":%d,\"bidder\":%d,"
            + "\"price\":%d,\"channel\":\"ch-%d\",\"url\":\"https://n.test/%d\",\"dateTime\":%d,"
            + "\"extra\":\"x\"},\"dateTime\":%d}",
        auctionId, pos, (i % 1000) + 1, pos % 8, auctionId, ts, ts);
  }

  /** The same wide event as a protobuf {@link NexmarkEvent}; the inactive nested messages stay unset. */
  private static NexmarkEvent protobufEvent(long i) {
    long block = i / BLOCK;
    int pos = (int) (i % BLOCK);
    long ts = block * 1000L + pos * 10L;
    NexmarkEvent.Builder row = NexmarkEvent.newBuilder().setDateTime(ts);
    if (pos == 0) {
      return row.setEventType(0)
          .setPerson(
              NexmarkPerson.newBuilder()
                  .setId(block)
                  .setName("p-" + block)
                  .setEmailAddress("e-" + block)
                  .setCreditCard("1234")
                  .setCity("c-" + (block % 1000))
                  .setState(STATES[(int) (block % STATES.length)])
                  .setDateTime(ts)
                  .setExtra("x"))
          .build();
    }
    if (pos <= 3) {
      long auctionId = block * 3 + (pos - 1);
      return row.setEventType(1)
          .setAuction(
              NexmarkAuction.newBuilder()
                  .setId(auctionId)
                  .setItemName("i-" + auctionId)
                  .setDescription("d-" + auctionId)
                  .setInitialBid(10)
                  .setReserve(50)
                  .setDateTime(ts)
                  .setExpires(ts + 20000)
                  .setSeller(block)
                  .setCategory(block % 100)
                  .setExtra("x"))
          .build();
    }
    long auctionId = block * 3 + (pos % 3);
    return row.setEventType(2)
        .setBid(
            NexmarkBid.newBuilder()
                .setAuction(auctionId)
                .setBidder(pos)
                .setPrice((i % 1000) + 1)
                .setChannel("ch-" + (pos % 8))
                .setUrl("https://n.test/" + auctionId)
                .setDateTime(ts)
                .setExtra("x"))
        .build();
  }

  /** The same wide event as an Avro {@link GenericRecord}, inactive structs left null. */
  private static GenericRecord avroEvent(long i, Schema schema) {
    long block = i / BLOCK;
    int pos = (int) (i % BLOCK);
    long ts = block * 1000L + pos * 10L;
    GenericRecord row = new GenericData.Record(schema);
    row.put("event_type", pos == 0 ? 0 : pos <= 3 ? 1 : 2);
    row.put("dateTime", ts);
    if (pos == 0) {
      GenericRecord person = new GenericData.Record(branch(schema, "person"));
      person.put("id", block);
      person.put("name", "p-" + block);
      person.put("emailAddress", "e-" + block);
      person.put("creditCard", "1234");
      person.put("city", "c-" + (block % 1000));
      person.put("state", STATES[(int) (block % STATES.length)]);
      person.put("dateTime", ts);
      person.put("extra", "x");
      row.put("person", person);
    } else if (pos <= 3) {
      long auctionId = block * 3 + (pos - 1);
      GenericRecord auction = new GenericData.Record(branch(schema, "auction"));
      auction.put("id", auctionId);
      auction.put("itemName", "i-" + auctionId);
      auction.put("description", "d-" + auctionId);
      auction.put("initialBid", 10L);
      auction.put("reserve", 50L);
      auction.put("dateTime", ts);
      auction.put("expires", ts + 20000);
      auction.put("seller", block);
      auction.put("category", block % 100);
      auction.put("extra", "x");
      row.put("auction", auction);
    } else {
      long auctionId = block * 3 + (pos % 3);
      GenericRecord bid = new GenericData.Record(branch(schema, "bid"));
      bid.put("auction", auctionId);
      bid.put("bidder", (long) pos);
      bid.put("price", (i % 1000) + 1);
      bid.put("channel", "ch-" + (pos % 8));
      bid.put("url", "https://n.test/" + auctionId);
      bid.put("dateTime", ts);
      bid.put("extra", "x");
      row.put("bid", bid);
    }
    return row;
  }

  /** The record branch of a nullable (null-union) struct field's Avro schema. */
  private static Schema branch(Schema schema, String field) {
    Schema fieldSchema = schema.getField(field).schema();
    if (fieldSchema.getType() == Schema.Type.RECORD) {
      return fieldSchema;
    }
    return fieldSchema.getTypes().stream()
        .filter(s -> s.getType() == Schema.Type.RECORD)
        .findFirst()
        .orElseThrow();
  }

  /** The wide event row type, mirroring {@link #SCHEMA}, for deriving the Avro schema. */
  private static RowType nexmarkRowType() {
    return (RowType)
        DataTypes.ROW(
                DataTypes.FIELD("event_type", DataTypes.INT()),
                DataTypes.FIELD(
                    "person",
                    DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("emailAddress", DataTypes.STRING()),
                        DataTypes.FIELD("creditCard", DataTypes.STRING()),
                        DataTypes.FIELD("city", DataTypes.STRING()),
                        DataTypes.FIELD("state", DataTypes.STRING()),
                        DataTypes.FIELD("dateTime", DataTypes.BIGINT()),
                        DataTypes.FIELD("extra", DataTypes.STRING()))),
                DataTypes.FIELD(
                    "auction",
                    DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("itemName", DataTypes.STRING()),
                        DataTypes.FIELD("description", DataTypes.STRING()),
                        DataTypes.FIELD("initialBid", DataTypes.BIGINT()),
                        DataTypes.FIELD("reserve", DataTypes.BIGINT()),
                        DataTypes.FIELD("dateTime", DataTypes.BIGINT()),
                        DataTypes.FIELD("expires", DataTypes.BIGINT()),
                        DataTypes.FIELD("seller", DataTypes.BIGINT()),
                        DataTypes.FIELD("category", DataTypes.BIGINT()),
                        DataTypes.FIELD("extra", DataTypes.STRING()))),
                DataTypes.FIELD(
                    "bid",
                    DataTypes.ROW(
                        DataTypes.FIELD("auction", DataTypes.BIGINT()),
                        DataTypes.FIELD("bidder", DataTypes.BIGINT()),
                        DataTypes.FIELD("price", DataTypes.BIGINT()),
                        DataTypes.FIELD("channel", DataTypes.STRING()),
                        DataTypes.FIELD("url", DataTypes.STRING()),
                        DataTypes.FIELD("dateTime", DataTypes.BIGINT()),
                        DataTypes.FIELD("extra", DataTypes.STRING()))),
                DataTypes.FIELD("dateTime", DataTypes.BIGINT()))
            .getLogicalType();
  }
}

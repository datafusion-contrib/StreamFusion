package tech.streamfusion;

import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The full Nexmark matrix: every query StreamFusion currently accelerates end-to-end, each run against
 * stock Flink and against native execution from every source it can be fed by — the generator (rowwise
 * RowData), a local Parquet file read by Flink's stock source,
 * and Kafka json/avro/protobuf, the Kafka formats climbing the source→columnar ladder (JVM transpose,
 * Rust decode with a JVM poll, fully native Rust poll+decode). One table per query, a native cell per
 * source, each a speedup over the Flink baseline for that same source.
 *
 * <p>The Parquet case writes the same wide event row once to a local directory, then reads it through
 * Flink's {@code filesystem}/{@code parquet} source; native operators pay the normal entry transpose.
 * Its rowtime is a plain {@code TIMESTAMP(3)} (unlike the Kafka {@code TIMESTAMP_LTZ}), so the {@code
 * DATE_FORMAT}/{@code HOUR} queries that are generator-only on Kafka run here too.
 *
 * <p>The query set is every query StreamFusion accelerates: q0–q5, q7–q23 (q1's and q14's decimal are
 * exact and native by default; q21's REGEXP_EXTRACT/LOWER and q14's HOUR route through the host
 * implementation via the columnar JVM upcall; q13 is a synchronous lookup join against a bounded
 * {@code test-lookup} dimension). Only q6 is out — Flink itself cannot run it (wontdos/39). q10/q14/q15/
 * q16/q17 report on the generator only, since their {@code DATE_FORMAT}/{@code HOUR} need a plain
 * {@code TIMESTAMP} and would partially fall back over the Kafka {@code TIMESTAMP_LTZ} rowtime.
 * Each query runs over the same logical {@code person}/{@code auction}/{@code bid} views the published
 * Nexmark SQL uses, off a watermarked event-time {@code dateTime}; the only thing that changes between
 * cells is how those rows are produced and transposed into the columnar island. The perimeter transposes
 * (source and sink) stay in the measured path — the steelman per CLAUDE.md.
 *
 * <p>Opt-in (millions of rows, Docker for Testcontainers Kafka, the {@code kafka} cargo feature for the
 * native source): {@code SF_BENCHMARK=true mvn test -Pbench -Dnative.cargo.args="build --release
 * --features kafka" -Dtest=NexmarkMatrixBenchmark}. {@code SF_ROWS} overrides the event count (default
 * 500,000), {@code SF_PARALLELISM} the Kafka-fed runs' job parallelism (default 4; both engines),
 * {@code SF_KAFKA_PARTITIONS} an optional corpus partition count (default: job parallelism; useful
 * for multi-split source tests), {@code SF_MATRIX_QUERIES} a comma-separated query subset (e.g. {@code q0,q7,q15}), {@code
 * SF_LADDER_FORMATS} the Kafka formats (default {@code json,avro,protobuf}), {@code SF_MATRIX_GENERATOR}
 * ({@code false} to skip the generator column), {@code SF_MATRIX_PARQUET} ({@code false} to skip the
 * Parquet column), and {@code SF_MATRIX_KAFKA} ({@code false} to skip Kafka).
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NexmarkMatrixBenchmark {

  private static final long ROWS =
      System.getenv("SF_ROWS") != null ? Long.parseLong(System.getenv("SF_ROWS")) : 500_000L;
  // The Kafka-fed comparisons run both engines at a representative multi-subtask parallelism; the
  // corpus topic is created with one partition per subtask so every source instance has a split.
  private static final int PARALLELISM =
      System.getenv("SF_PARALLELISM") != null ? Integer.parseInt(System.getenv("SF_PARALLELISM")) : 4;
  private static final int KAFKA_PARTITIONS =
      System.getenv("SF_KAFKA_PARTITIONS") != null
          ? Integer.parseInt(System.getenv("SF_KAFKA_PARTITIONS"))
          : PARALLELISM;
  // Front-page sink tuning, shared byte-for-byte by the Flink and StreamFusion cells. Kafka 4.2's
  // 16 KiB / 5 ms defaults force the task and sender threads to contend over tiny accumulator
  // batches; one partition's share of an 8192-row source batch fits comfortably in 512 KiB.
  private static final int KAFKA_PRODUCER_BATCH_BYTES = 512 * 1024;
  private static final int KAFKA_PRODUCER_LINGER_MS = 20;
  // Headline discipline: one warmup, best of two. SF_WARMUP/SF_RUNS override for quick
  // iteration passes (SF_RUNS=1 SF_WARMUP=0 measures every cell once, ~3x faster and noisier).
  private static final int WARMUP =
      System.getenv("SF_WARMUP") != null ? Integer.parseInt(System.getenv("SF_WARMUP")) : 1;
  private static final int RUNS =
      System.getenv("SF_RUNS") != null ? Integer.parseInt(System.getenv("SF_RUNS")) : 2;

  // Extra TableConfig entries applied to EVERY measured environment (both engines) — the tuned
  // matrix sets the mini-batch keys here so Flink and the native island run the same tuning,
  // per the steelman rule. Empty for the default matrix.
  private static Map<String, String> tableConfigExtras = Map.of();

  private static final Map<String, String> UPSERT_KEYS =
      Map.of(
          "q4", "id",
          "q9", "id",
          "q15", "`day`",
          "q16", "channel, `day`",
          "q17", "auction, `day`",
          "q18", "bidder, auction",
          "q19", "auction, rank_number");

  // The opt-in native path for DATE_FORMAT/EXTRACT over TIMESTAMP_LTZ (chrono-tz in Rust instead of the
  // byte-parity JVM upcall) — reported as a second "incompatible" row for the datetime queries, exactly
  // as q21 reports its native-regex path. Divergence surface: tzdb-version skew, DST beyond ~2100, deep
  // history, and legacy zone forms (which the encoder rejects → fall back).
  private static final String DATETIME_VARIANT = "native datetime (incompatible)";
  private static final Map<String, String> ALLOW_INCOMPATIBLE =
      Map.of("streamfusion.expression.allowIncompatible", "true");

  /** A rung of the Kafka source-to-columnar boundary. */
  private enum Rung {
    FLINK("Flink", Map.of("streamfusion.native.enabled", "false")),
    JVM_TRANSPOSE(
        "JVM transpose",
        Map.of(
            "streamfusion.native.enabled", "true",
            "streamfusion.operator.kafkaDecode.enabled", "false")),
    RUST_DECODE(
        "Rust decode (JVM poll)",
        Map.of(
            "streamfusion.native.enabled", "true",
            "streamfusion.operator.kafkaDecode.enabled", "true"));

    final String label;
    final Map<String, String> properties;

    Rung(String label, Map<String, String> properties) {
      this.label = label;
      this.properties = properties;
    }
  }

  private static final class Query {
    final String label;
    final boolean approximateDecimal;
    final String[] setup; // extra SQL run before the insert (q12 proctime view, q13 dim+proctime); else null
    final String sinkDdl; // %TS% substituted with the harness's event-time type
    final String insertSql;
    // A second native measurement run with these extra properties set — used to report a query both on
    // its byte-identical default path and on a faster opt-in path that diverges from Flink at an edge.
    // q21's REGEXP_EXTRACT/LOWER default to a Flink-parity JVM upcall; allowIncompatible switches them to
    // the pure-native Rust regex/case path. Null for queries with no such variant.
    final String nativeVariantLabel;
    final Map<String, String> nativeVariantProps;

    Query(String label, boolean approximateDecimal, String[] setup, String sinkDdl, String insertSql) {
      this(label, approximateDecimal, setup, sinkDdl, insertSql, null, null);
    }

    Query(
        String label,
        boolean approximateDecimal,
        String[] setup,
        String sinkDdl,
        String insertSql,
        String nativeVariantLabel,
        Map<String, String> nativeVariantProps) {
      this.label = label;
      this.approximateDecimal = approximateDecimal;
      this.setup = setup;
      this.sinkDdl = sinkDdl;
      this.insertSql = insertSql;
      this.nativeVariantLabel = nativeVariantLabel;
      this.nativeVariantProps = nativeVariantProps;
    }
  }

  /** Nexmark q14's UDF ({@code count_char(extra, 'c')}); registered in every matrix environment. */
  public static class CountChar extends org.apache.flink.table.functions.ScalarFunction {
    public long eval(String s, String c) {
      if (s == null || c == null || c.isEmpty()) {
        return 0L;
      }
      long count = 0;
      char target = c.charAt(0);
      for (int i = 0; i < s.length(); i++) {
        if (s.charAt(i) == target) {
          count++;
        }
      }
      return count;
    }
  }

  private static final Query[] ALL_QUERIES = {
    new Query(
        "q0",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price BIGINT, `dateTime` %TS%,"
            + " extra STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT auction, bidder, price, `dateTime`, extra FROM bid"),
    new Query(
        "q1",
        false, // exact Decimal128 * + HALF_UP cast is native + byte-parity by default (the reported cell)
        null,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price DECIMAL(23, 3), `dateTime` %TS%,"
            + " extra STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT auction, bidder, 0.908 * price AS price, `dateTime`, extra FROM bid",
        // …and a second cell on the faster approximate-decimal path (double math, diverges from Flink's
        // exact rounding at an edge) — same parity-vs-non-parity split as q21's regex/case.
        "approximate decimal (incompatible)",
        Map.of("streamfusion.expression.decimalArithmetic.approximate", "true")),
    new Query(
        "q2",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, price BIGINT) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT auction, price FROM bid WHERE MOD(auction, 123) = 0"),
    new Query(
        "q3",
        false,
        null,
        "CREATE TABLE sink (name STRING, city STRING, state STRING, id BIGINT) WITH"
            + " ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT P.name, P.city, P.state, A.id FROM auction AS A INNER JOIN"
            + " person AS P ON A.seller = P.id WHERE A.category = 10 AND (P.state = 'OR' OR P.state"
            + " = 'ID' OR P.state = 'CA')"),
    new Query(
        "q4",
        false,
        null,
        "CREATE TABLE sink (id BIGINT, final BIGINT) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT Q.category, AVG(Q.final) FROM (SELECT MAX(B.price) AS final,"
            + " A.category FROM auction A, bid B WHERE A.id = B.auction AND B.`dateTime` BETWEEN"
            + " A.`dateTime` AND A.expires GROUP BY A.id, A.category) Q GROUP BY Q.category"),
    new Query(
        "q7",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, price BIGINT, bidder BIGINT, `dateTime` %TS%,"
            + " extra STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT B.auction, B.price, B.bidder, B.`dateTime`, B.extra FROM bid B JOIN"
            + " (SELECT MAX(price) AS maxprice, window_end AS `dateTime` FROM"
            + " TABLE(TUMBLE(TABLE bid, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND))"
            + " GROUP BY window_start, window_end) B1 ON B.price = B1.maxprice"
            + " WHERE B.`dateTime` BETWEEN B1.`dateTime` - INTERVAL '10' SECOND AND B1.`dateTime`"),
    new Query(
        "q8",
        false,
        null,
        "CREATE TABLE sink (id BIGINT, name STRING, stime %WTS%) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT P.id, P.name, P.starttime FROM (SELECT id, name,"
            + " window_start AS starttime, window_end AS endtime FROM"
            + " TABLE(TUMBLE(TABLE person, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND))"
            + " GROUP BY id, name, window_start, window_end) P JOIN (SELECT seller,"
            + " window_start AS starttime, window_end AS endtime FROM"
            + " TABLE(TUMBLE(TABLE auction, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND))"
            + " GROUP BY seller, window_start, window_end) A"
            + " ON P.id = A.seller AND P.starttime = A.starttime AND P.endtime = A.endtime"),
    new Query(
        "q9",
        false,
        null,
        "CREATE TABLE sink (id BIGINT, itemName STRING, description STRING, initialBid BIGINT,"
            + " reserve BIGINT, `dateTime` %TS%, expires %TS%, seller BIGINT, category BIGINT,"
            + " extra STRING, auction BIGINT, bidder BIGINT, price BIGINT, bid_dateTime %TS%,"
            + " bid_extra STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT id, itemName, description, initialBid, reserve, `dateTime`, expires,"
            + " seller, category, extra, auction, bidder, price, bid_dateTime, bid_extra FROM (SELECT"
            + " A.*, B.auction, B.bidder, B.price, B.`dateTime` AS bid_dateTime, B.extra AS bid_extra,"
            + " ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.`dateTime` ASC) AS rownum"
            + " FROM auction A, bid B WHERE A.id = B.auction AND B.`dateTime` BETWEEN A.`dateTime` AND"
            + " A.expires) WHERE rownum <= 1"),
    new Query(
        "q10",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price BIGINT, `dateTime` %TS%,"
            + " extra STRING, dt STRING, hm STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT auction, bidder, price, `dateTime`, extra,"
            + " DATE_FORMAT(`dateTime`, 'yyyy-MM-dd'), DATE_FORMAT(`dateTime`, 'HH:mm') FROM bid",
        DATETIME_VARIANT,
        ALLOW_INCOMPATIBLE),
    new Query(
        "q11",
        false,
        null,
        "CREATE TABLE sink (bidder BIGINT, bid_count BIGINT, starttime %WTS%, endtime %WTS%) WITH"
            + " ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT B.bidder, count(*) AS bid_count,"
            + " SESSION_START(B.`dateTime`, INTERVAL '10' SECOND) AS starttime,"
            + " SESSION_END(B.`dateTime`, INTERVAL '10' SECOND) AS endtime FROM bid B"
            + " GROUP BY B.bidder, SESSION(B.`dateTime`, INTERVAL '10' SECOND)"),
    new Query(
        "q12",
        false,
        new String[] {"CREATE TEMPORARY VIEW bid_proc AS SELECT *, PROCTIME() AS p_time FROM bid"},
        "CREATE TABLE sink (bidder BIGINT, bid_count BIGINT, starttime %WTS%, endtime %WTS%) WITH"
            + " ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT bidder, count(*) AS bid_count, window_start AS starttime,"
            + " window_end AS endtime FROM TABLE(TUMBLE(TABLE bid_proc, DESCRIPTOR(p_time),"
            + " INTERVAL '10' SECOND)) GROUP BY bidder, window_start, window_end"),
    new Query(
        "q15",
        false,
        null,
        "CREATE TABLE sink (`day` STRING, total_bids BIGINT, rank1_bids BIGINT, rank2_bids BIGINT,"
            + " rank3_bids BIGINT, total_bidders BIGINT, rank1_bidders BIGINT, rank2_bidders BIGINT,"
            + " rank3_bidders BIGINT, total_auctions BIGINT, rank1_auctions BIGINT,"
            + " rank2_auctions BIGINT, rank3_auctions BIGINT) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT DATE_FORMAT(`dateTime`, 'yyyy-MM-dd') AS `day`, count(*) AS"
            + " total_bids, count(*) filter (where price < 10000) AS rank1_bids, count(*) filter"
            + " (where price >= 10000 and price < 1000000) AS rank2_bids, count(*) filter (where price"
            + " >= 1000000) AS rank3_bids, count(distinct bidder) AS total_bidders, count(distinct"
            + " bidder) filter (where price < 10000) AS rank1_bidders, count(distinct bidder) filter"
            + " (where price >= 10000 and price < 1000000) AS rank2_bidders, count(distinct bidder)"
            + " filter (where price >= 1000000) AS rank3_bidders, count(distinct auction) AS"
            + " total_auctions, count(distinct auction) filter (where price < 10000) AS rank1_auctions,"
            + " count(distinct auction) filter (where price >= 10000 and price < 1000000) AS"
            + " rank2_auctions, count(distinct auction) filter (where price >= 1000000) AS"
            + " rank3_auctions FROM bid GROUP BY DATE_FORMAT(`dateTime`, 'yyyy-MM-dd')",
        DATETIME_VARIANT,
        ALLOW_INCOMPATIBLE),
    new Query(
        "q16",
        false,
        null,
        "CREATE TABLE sink (channel STRING, `day` STRING, `minute` STRING, total_bids BIGINT,"
            + " rank1_bids BIGINT, rank2_bids BIGINT, rank3_bids BIGINT, total_bidders BIGINT,"
            + " rank1_bidders BIGINT, rank2_bidders BIGINT, rank3_bidders BIGINT, total_auctions"
            + " BIGINT, rank1_auctions BIGINT, rank2_auctions BIGINT, rank3_auctions BIGINT) WITH"
            + " ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT channel, DATE_FORMAT(`dateTime`, 'yyyy-MM-dd') AS `day`,"
            + " max(DATE_FORMAT(`dateTime`, 'HH:mm')) AS `minute`, count(*) AS total_bids, count(*)"
            + " filter (where price < 10000) AS rank1_bids, count(*) filter (where price >= 10000 and"
            + " price < 1000000) AS rank2_bids, count(*) filter (where price >= 1000000) AS rank3_bids,"
            + " count(distinct bidder) AS total_bidders, count(distinct bidder) filter (where price <"
            + " 10000) AS rank1_bidders, count(distinct bidder) filter (where price >= 10000 and price"
            + " < 1000000) AS rank2_bidders, count(distinct bidder) filter (where price >= 1000000) AS"
            + " rank3_bidders, count(distinct auction) AS total_auctions, count(distinct auction)"
            + " filter (where price < 10000) AS rank1_auctions, count(distinct auction) filter (where"
            + " price >= 10000 and price < 1000000) AS rank2_auctions, count(distinct auction) filter"
            + " (where price >= 1000000) AS rank3_auctions FROM bid GROUP BY channel,"
            + " DATE_FORMAT(`dateTime`, 'yyyy-MM-dd')",
        DATETIME_VARIANT,
        ALLOW_INCOMPATIBLE),
    new Query(
        "q17",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, `day` STRING, total_bids BIGINT, rank1_bids BIGINT,"
            + " rank2_bids BIGINT, rank3_bids BIGINT, min_price BIGINT, max_price BIGINT, avg_price"
            + " BIGINT, sum_price BIGINT) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT auction, DATE_FORMAT(`dateTime`, 'yyyy-MM-dd') AS `day`, count(*) AS"
            + " total_bids, count(*) filter (where price < 10000) AS rank1_bids, count(*) filter (where"
            + " price >= 10000 and price < 1000000) AS rank2_bids, count(*) filter (where price >="
            + " 1000000) AS rank3_bids, min(price) AS min_price, max(price) AS max_price, avg(price) AS"
            + " avg_price, sum(price) AS sum_price FROM bid GROUP BY auction, DATE_FORMAT(`dateTime`,"
            + " 'yyyy-MM-dd')",
        DATETIME_VARIANT,
        ALLOW_INCOMPATIBLE),
    new Query(
        "q18",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING,"
            + " `dateTime` %TS%, extra STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT auction, bidder, price, channel, url, `dateTime`, extra FROM (SELECT"
            + " *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY `dateTime` DESC) AS"
            + " rank_number FROM bid) WHERE rank_number <= 1"),
    new Query(
        "q19",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING,"
            + " `dateTime` %TS%, extra STRING, rank_number BIGINT) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY"
            + " price DESC) AS rank_number FROM bid) WHERE rank_number <= 10"),
    new Query(
        "q20",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING,"
            + " bid_dateTime %TS%, bid_extra STRING, itemName STRING, description STRING, initialBid"
            + " BIGINT, reserve BIGINT, auction_dateTime %TS%, expires %TS%, seller BIGINT, category"
            + " BIGINT, auction_extra STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT auction, bidder, price, channel, url, B.`dateTime`, B.extra, itemName,"
            + " description, initialBid, reserve, A.`dateTime`, expires, seller, category, A.extra FROM"
            + " bid AS B INNER JOIN auction AS A ON B.auction = A.id WHERE A.category = 10"),
    new Query(
        "q22",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, dir1 STRING,"
            + " dir2 STRING, dir3 STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT auction, bidder, price, channel, SPLIT_INDEX(url, '/', 3) AS dir1,"
            + " SPLIT_INDEX(url, '/', 4) AS dir2, SPLIT_INDEX(url, '/', 5) AS dir3 FROM bid"),
    new Query(
        "q5",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, num BIGINT) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT AuctionBids.auction, AuctionBids.num FROM (SELECT auction, count(*) AS"
            + " num, window_start AS starttime, window_end AS endtime FROM TABLE(HOP(TABLE bid,"
            + " DESCRIPTOR(`dateTime`), INTERVAL '2' SECOND, INTERVAL '10' SECOND)) GROUP BY auction,"
            + " window_start, window_end) AS AuctionBids JOIN (SELECT max(CountBids.num) AS maxn,"
            + " CountBids.starttime, CountBids.endtime FROM (SELECT count(*) AS num, window_start AS"
            + " starttime, window_end AS endtime FROM TABLE(HOP(TABLE bid, DESCRIPTOR(`dateTime`),"
            + " INTERVAL '2' SECOND, INTERVAL '10' SECOND)) GROUP BY auction, window_start, window_end)"
            + " AS CountBids GROUP BY CountBids.starttime, CountBids.endtime) AS MaxBids ON"
            + " AuctionBids.starttime = MaxBids.starttime AND AuctionBids.endtime = MaxBids.endtime AND"
            + " AuctionBids.num >= MaxBids.maxn"),
    new Query(
        "q13",
        false,
        new String[] {
          "CREATE TEMPORARY VIEW bid_lookup AS SELECT *, PROCTIME() AS p_time FROM bid",
          "CREATE TABLE dim (k BIGINT, val STRING) WITH ('connector' = 'test-lookup')",
        },
        "CREATE TABLE sink (auction BIGINT, price BIGINT, val STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT B.auction, B.price, D.val FROM bid_lookup AS B JOIN dim"
            + " FOR SYSTEM_TIME AS OF B.p_time AS D ON MOD(B.auction, 5) = D.k"),
    new Query(
        "q14",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price DECIMAL(23, 3), bidTimeType STRING,"
            + " `dateTime` %TS%, extra STRING, c_counts BIGINT) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT auction, bidder, 0.908 * price AS price, CASE WHEN HOUR(`dateTime`) >="
            + " 8 AND HOUR(`dateTime`) <= 18 THEN 'dayTime' WHEN HOUR(`dateTime`) <= 6 OR"
            + " HOUR(`dateTime`) >= 20 THEN 'nightTime' ELSE 'otherTime' END AS bidTimeType,"
            + " `dateTime`, extra, count_char(extra, 'c') AS c_counts FROM bid",
        DATETIME_VARIANT,
        ALLOW_INCOMPATIBLE),
    new Query(
        "q21",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price BIGINT, channel STRING,"
            + " channel_id STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT auction, bidder, price, channel, CASE WHEN lower(channel) = 'apple'"
            + " THEN '0' WHEN lower(channel) = 'google' THEN '1' WHEN lower(channel) = 'facebook' THEN"
            + " '2' WHEN lower(channel) = 'baidu' THEN '3' ELSE REGEXP_EXTRACT(url,"
            + " '(&|^)channel_id=([^&]*)', 2) END AS channel_id FROM bid WHERE REGEXP_EXTRACT(url,"
            + " '(&|^)channel_id=([^&]*)', 2) IS NOT NULL OR lower(channel) IN ('apple', 'google',"
            + " 'facebook', 'baidu')",
        "native regex/case (incompatible)",
        Map.of("streamfusion.expression.allowIncompatible", "true")),
    new Query(
        "q23",
        false,
        null,
        "CREATE TABLE sink (auction BIGINT, bidder BIGINT, price BIGINT, itemName STRING,"
            + " auction_dateTime %TS%, seller BIGINT) WITH ('connector' = 'blackhole')",
        "INSERT INTO sink SELECT B.auction, B.bidder, B.price, A.itemName, A.`dateTime`, A.seller"
            + " FROM bid B JOIN person P ON P.id = B.bidder JOIN auction A ON A.seller = B.bidder"),
  };

  // The wide nested event row written to / read from Parquet. Same shape as the generator's event row,
  // with a plain TIMESTAMP(3) rowtime (so DATE_FORMAT/HOUR stay native, unlike the Kafka LTZ rowtime).
  private static final String PERSON_TYPE =
      "ROW<id BIGINT, name STRING, emailAddress STRING, creditCard STRING, city STRING,"
          + " state STRING, `dateTime` TIMESTAMP(3), extra STRING>";
  private static final String AUCTION_TYPE =
      "ROW<id BIGINT, itemName STRING, description STRING, initialBid BIGINT,"
          + " reserve BIGINT, `dateTime` TIMESTAMP(3), expires TIMESTAMP(3), seller BIGINT,"
          + " category BIGINT, extra STRING>";
  private static final String BID_TYPE =
      "ROW<auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING,"
          + " `dateTime` TIMESTAMP(3), extra STRING>";
  private static final String PARQUET_SCHEMA =
      "event_type INT,"
          + " person " + PERSON_TYPE + ","
          + " auction " + AUCTION_TYPE + ","
          + " bid " + BID_TYPE + ","
          + " `dateTime` TIMESTAMP(3)";
  @Test
  void matrix() throws Exception {
    Query[] queries = selectQueries();
    boolean runGenerator = !"false".equals(System.getenv("SF_MATRIX_GENERATOR"));
    boolean runParquet = !"false".equals(System.getenv("SF_MATRIX_PARQUET"));
    boolean runKafka = !"false".equals(System.getenv("SF_MATRIX_KAFKA"));
    String formatsEnv = System.getenv("SF_LADDER_FORMATS");
    String[] formats =
        formatsEnv != null ? formatsEnv.split(",") : new String[] {"json", "avro", "protobuf"};
    // result[label] -> ordered cells (rendered at the end as one table).
    Map<String, List<String>> report = new LinkedHashMap<>();
    for (Query q : queries) {
      report.put(q.label, new ArrayList<>());
    }

    if (runGenerator) {
      for (Query q : queries) {
        double flink = generatorBest(q, false, null);
        double nativeRun = generatorBest(q, true, null);
        report.get(q.label).add(cell("generator", flink, nativeRun));
        // A query with a faster opt-in path that diverges from Flink at an edge (q21's native
        // regex/case) reports it too, against the same Flink baseline — parity and non-parity side by
        // side, so the cost of staying byte-identical is visible.
        if (q.nativeVariantProps != null) {
          double variant = generatorBest(q, true, q.nativeVariantProps);
          report.get(q.label).add(variantCell("generator", q.nativeVariantLabel, flink, variant));
        }
      }
    }

    if (runParquet) {
      Path dir = writeParquetSource();
      for (Query q : queries) {
        double flink = parquetBest(dir, q, false, null);
        double nativeRun = parquetBest(dir, q, true, null);
        report.get(q.label).add(cell("parquet", flink, nativeRun));
        if (q.nativeVariantProps != null) {
          double variant = parquetBest(dir, q, true, q.nativeVariantProps);
          report.get(q.label).add(variantCell("parquet", q.nativeVariantLabel, flink, variant));
        }
      }
    }

    if (runKafka) {
      for (String format : formats) {
        try (KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
          kafka.start();
          String brokers = kafka.getBootstrapServers();
          NexmarkKafkaBenchmark.produce(brokers, "nexmark", format, ROWS, KAFKA_PARTITIONS);
          for (Query q : queries) {
            double flink = kafkaBest(brokers, format, Rung.FLINK, q, null);
            report.get(q.label).add(kafkaCell(brokers, format, q, flink, null, null));
            // The opt-in path (q21 native regex/case; q10/q14/q15/q16/q17 native chrono-tz datetime) is
            // measured on Kafka too, so the incompatible row has per-format numbers like the default.
            if (q.nativeVariantProps != null) {
              report
                  .get(q.label)
                  .add(kafkaCell(brokers, format, q, flink, q.nativeVariantLabel, q.nativeVariantProps));
            }
          }
        }
      }
    }

    StringBuilder out = new StringBuilder("\n##### NEXMARK MATRIX (" + ROWS + " events, best of " + RUNS + ") #####\n");
    for (Query q : queries) {
      out.append("\n===== ").append(q.label).append(" =====\n");
      for (String line : report.get(q.label)) {
        out.append("  ").append(line).append('\n');
      }
    }
    System.out.println(out);
  }

  /**
   * The "tuned Flink" column: the same queries with {@code table.exec.mini-batch.*} enabled on BOTH
   * engines — the standard production tuning for the stateful changelog queries, and the config the
   * only public per-query Alibaba comparison used. Generator source only (the tuned question is
   * engine-vs-engine, not the source perimeter), changelog-family queries by default.
   * {@code table.optimizer.distinct-agg.split.enabled} stays at its default (off): it is a skew
   * mitigation for parallel deployments — these runs are parallelism 1 — and its incremental plan
   * chain has no native path yet (ticket 41). A query whose mini-batch plan shape does not route
   * reports the fallback instead of failing the run, so the column doubles as the mini-batch
   * coverage check. Gated by {@code SF_MATRIX_TUNED=true} on top of {@code SF_BENCHMARK}.
   */
  @Test
  @EnabledIfEnvironmentVariable(named = "SF_MATRIX_TUNED", matches = "true")
  void tunedMiniBatchMatrix() throws Exception {
    Map<String, String> miniBatch =
        Map.of(
            "table.exec.mini-batch.enabled", "true",
            "table.exec.mini-batch.allow-latency", "2 s",
            "table.exec.mini-batch.size", "50000");
    Query[] queries = selectTunedQueries();
    StringBuilder out =
        new StringBuilder(
            "\n##### NEXMARK TUNED (mini-batch on both engines; "
                + ROWS
                + " events, best of "
                + RUNS
                + ") #####\n");
    for (Query q : queries) {
      tableConfigExtras = miniBatch;
      try {
        double flink = generatorBest(q, false, null);
        String result;
        try {
          double nativeRun = generatorBest(q, true, null);
          result = cell("tuned", flink, nativeRun);
        } catch (IllegalStateException fallback) {
          result = String.format("tuned      Flink %6.3fs  |  %s", flink, fallback.getMessage());
        }
        out.append(String.format("%-4s  %s%n", q.label, result));
      } finally {
        tableConfigExtras = Map.of();
      }
    }
    System.out.println(out);
  }

  /**
   * Direct enabled-versus-disabled comparison for both engines on the same 5M-style generator
   * workload. Unlike running {@link #matrix()} and {@link #tunedMiniBatchMatrix()} back to back,
   * this keeps the two modes adjacent for each query and alternates which mode runs first. That
   * balances long-lived JVM/GC order effects while retaining the normal warmup and best-of-two rule
   * inside every cell. Gated by {@code SF_MATRIX_COMPARE_MODES=true}.
   */
  @Test
  @EnabledIfEnvironmentVariable(named = "SF_MATRIX_COMPARE_MODES", matches = "true")
  void miniBatchModeComparison() throws Exception {
    Map<String, String> miniBatch =
        Map.of(
            "table.exec.mini-batch.enabled", "true",
            "table.exec.mini-batch.allow-latency", "2 s",
            "table.exec.mini-batch.size", "50000");
    Query[] queries = selectQueries();
    StringBuilder out =
        new StringBuilder(
            "\n##### NEXMARK MINI-BATCH MODE COMPARISON ("
                + ROWS
                + " events, best of "
                + RUNS
                + ") #####\n");
    out.append(
        "query  Flink off  Native off  SF/Flink off  Flink on  Native on  SF/Flink on  Flink on/off  SF on/off\n");

    for (int i = 0; i < queries.length; i++) {
      Query q = queries[i];
      double flinkOff;
      double nativeOff;
      double flinkOn;
      double nativeOn;
      if ((i & 1) == 0) {
        flinkOff = generatorBestWithConfig(q, false, Map.of());
        nativeOff = generatorBestWithConfig(q, true, Map.of());
        flinkOn = generatorBestWithConfig(q, false, miniBatch);
        nativeOn = generatorBestWithConfig(q, true, miniBatch);
      } else {
        flinkOn = generatorBestWithConfig(q, false, miniBatch);
        nativeOn = generatorBestWithConfig(q, true, miniBatch);
        flinkOff = generatorBestWithConfig(q, false, Map.of());
        nativeOff = generatorBestWithConfig(q, true, Map.of());
      }
      out.append(
          String.format(
              "%4s  %9.3f  %10.3f  %12.2fx  %8.3f  %9.3f  %11.2fx  %12.2fx  %9.2fx%n",
              q.label,
              ROWS / flinkOff / 1_000_000.0,
              ROWS / nativeOff / 1_000_000.0,
              flinkOff / nativeOff,
              ROWS / flinkOn / 1_000_000.0,
              ROWS / nativeOn / 1_000_000.0,
              flinkOn / nativeOn,
              flinkOff / flinkOn,
              nativeOff / nativeOn));
    }
    System.out.println(out);
  }

  /**
   * The headline production-shaped comparison: Kafka JSON input and exactly-once Kafka JSON output
   * on the same broker, with mini-batching disabled and enabled on both engines. Append-only queries
   * use the regular Kafka connector; updating queries use their Nexmark result key through
   * upsert-kafka. Gated by {@code SF_MATRIX_KAFKA_SINK=true}.
   */
  @Test
  @EnabledIfEnvironmentVariable(named = "SF_MATRIX_KAFKA_SINK", matches = "true")
  void exactlyOnceKafkaSinkModeComparison() throws Exception {
    String requestedModes =
        System.getenv("SF_MATRIX_KAFKA_SINK_MODES") == null
            ? "both"
            : System.getenv("SF_MATRIX_KAFKA_SINK_MODES");
    if (!Set.of("off", "on", "both").contains(requestedModes)) {
      throw new IllegalArgumentException(
          "SF_MATRIX_KAFKA_SINK_MODES must be off, on, or both: " + requestedModes);
    }
    boolean runOff = !"on".equals(requestedModes);
    boolean runOn = !"off".equals(requestedModes);
    Map<String, String> miniBatch =
        Map.of(
            "table.exec.mini-batch.enabled", "true",
            "table.exec.mini-batch.allow-latency", "2 s",
            "table.exec.mini-batch.size", "50000");
    Query[] queries = selectQueries();
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
            .withEnv("KAFKA_TRANSACTION_MAX_TIMEOUT_MS", "7200000")
            // Deleted output topics free their segments immediately: the suites create and
            // delete one sizeable topic per run, and the default 60s delete delay lets pending
            // segments accumulate faster than they purge — enough to fill the Docker VM's disk
            // mid-suite and kill the broker.
            .withEnv("KAFKA_LOG_SEGMENT_DELETE_DELAY_MS", "0")) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      NexmarkKafkaBenchmark.produce(brokers, "nexmark", "json", ROWS, KAFKA_PARTITIONS);
      StringBuilder out =
          new StringBuilder(
              "\n##### NEXMARK EXACTLY-ONCE KAFKA "
                  + (runOff && runOn
                      ? "MODE COMPARISON"
                      : "MINI-BATCH " + requestedModes.toUpperCase())
                  + " ("
                  + ROWS
                  + " events, best of "
                  + RUNS
                  + ") #####\n");
      if (runOff && runOn) {
        out.append(
            "query  Flink off  Native off  SF/Flink off  Flink on  Native on  SF/Flink on  Flink on/off  SF on/off\n");
      } else {
        out.append("query  Flink s      ev/s  StreamFusion s      ev/s  SF/Flink\n");
      }

      for (int i = 0; i < queries.length; i++) {
        Query q = queries[i];
        String row;
        // One query lost to a transient environment stall (a broker timeout, a TaskExecutor
        // heartbeat loss) must not discard the whole suite: mark it failed, keep measuring, and
        // re-run the marked queries alone via SF_MATRIX_QUERIES. Rows also print as they land so
        // a later fatal cannot erase what completed.
        try {
          if (runOff && runOn) {
            double flinkOff;
            double nativeOff;
            double flinkOn;
            double nativeOn;
            if ((i & 1) == 0) {
              flinkOff = kafkaSinkBest(brokers, q, false, Map.of());
              nativeOff = kafkaSinkBest(brokers, q, true, Map.of());
              flinkOn = kafkaSinkBest(brokers, q, false, miniBatch);
              nativeOn = kafkaSinkBest(brokers, q, true, miniBatch);
            } else {
              flinkOn = kafkaSinkBest(brokers, q, false, miniBatch);
              nativeOn = kafkaSinkBest(brokers, q, true, miniBatch);
              flinkOff = kafkaSinkBest(brokers, q, false, Map.of());
              nativeOff = kafkaSinkBest(brokers, q, true, Map.of());
            }
            row =
                String.format(
                    "%4s  %9.3f  %10.3f  %12.2fx  %8.3f  %9.3f  %11.2fx  %12.2fx  %9.2fx%n",
                    q.label,
                    ROWS / flinkOff / 1_000_000.0,
                    ROWS / nativeOff / 1_000_000.0,
                    flinkOff / nativeOff,
                    ROWS / flinkOn / 1_000_000.0,
                    ROWS / nativeOn / 1_000_000.0,
                    flinkOn / nativeOn,
                    flinkOff / flinkOn,
                    nativeOff / nativeOn);
          } else {
            Map<String, String> config = runOn ? miniBatch : Map.of();
            double flink = kafkaSinkBest(brokers, q, false, config);
            double nativeRun = kafkaSinkBest(brokers, q, true, config);
            row =
                String.format(
                    "%4s  %7.3f  %8.0f  %14.3f  %8.0f  %8.2fx%n",
                    q.label,
                    flink,
                    ROWS / flink,
                    nativeRun,
                    ROWS / nativeRun,
                    flink / nativeRun);
          }
        } catch (Exception failure) {
          if ("true".equals(System.getenv("SF_BENCHMARK_STACKTRACE"))) {
            failure.printStackTrace(System.out);
          }
          row = String.format("%4s  FAILED: %s%n", q.label, rootCause(failure));
          if (rootCause(failure).contains("createTopics")) {
            // The broker itself is gone — every further cell would burn the same timeout.
            out.append(row);
            System.out.println(out);
            throw failure;
          }
        }
        out.append(row);
        System.out.print(row);
      }
      System.out.println(out);
    }
  }

  /**
   * The readme's memory-state, Kafka-input comparison with the output boundary changed to Parquet.
   * Every physical change is appended with its RowKind, so updating queries remain auditable rather
   * than being collapsed or rejected by the normal append-only filesystem sink. Mini-batching is
   * explicitly disabled. Each sink subtask writes independent part files below the printed root.
   */
  @Test
  @EnabledIfEnvironmentVariable(named = "SF_MATRIX_PARQUET_SINK", matches = "true")
  void changelogParquetSinkComparison() throws Exception {
    Query[] queries = selectQueries();
    Path outputRoot =
        System.getenv("SF_PARQUET_OUTPUT") == null
            ? Files.createTempDirectory("nexmark-changelog-parquet")
            : Path.of(System.getenv("SF_PARQUET_OUTPUT"));
    Files.createDirectories(outputRoot);
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      NexmarkKafkaBenchmark.produce(brokers, "nexmark", "json", ROWS, KAFKA_PARTITIONS);
      StringBuilder out =
          new StringBuilder(
              "\n##### NEXMARK CHANGELOG PARQUET (memory state, mini-batch off; "
                  + ROWS
                  + " events, best of "
                  + RUNS
                  + ") #####\n"
                  + "output: "
                  + outputRoot.toAbsolutePath()
                  + "\nquery  Flink s      ev/s  StreamFusion s      ev/s  SF/Flink\n");
      for (Query q : queries) {
        String row;
        try {
          double flink = parquetSinkBest(brokers, outputRoot, q, false);
          double nativeRun = parquetSinkBest(brokers, outputRoot, q, true);
          row =
              String.format(
                  "%4s  %7.3f  %8.0f  %14.3f  %8.0f  %8.2fx%n",
                  q.label, flink, ROWS / flink, nativeRun, ROWS / nativeRun, flink / nativeRun);
        } catch (Exception failure) {
          if ("true".equals(System.getenv("SF_BENCHMARK_STACKTRACE"))) {
            failure.printStackTrace(System.out);
          }
          row = String.format("%4s  FAILED: %s%n", q.label, rootCause(failure));
        }
        out.append(row);
        System.out.print(row);
      }
      System.out.println(out);
    }
  }

  /**
   * Runs the readme's Kafka JSON, memory-state, mini-batch-off matrix against fresh Delta tables.
   * Updating queries use Delta 4.4 merge-on-read upserts; append-only queries retain their natural
   * changelog mode. Invoked from the optional Delta module so the default runtime test artifact
   * remains connector-neutral.
   */
  static void runDeltaMergeOnReadSinkComparison(DeltaTableInitializer tableInitializer)
      throws Exception {
    Query[] queries = selectQueries();
    boolean retainOutput = System.getenv("SF_DELTA_OUTPUT") != null;
    Path outputRoot =
        retainOutput
            ? Path.of(System.getenv("SF_DELTA_OUTPUT"))
            : Files.createTempDirectory("nexmark-delta-mor");
    Files.createDirectories(outputRoot);
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      NexmarkKafkaBenchmark.produce(brokers, "nexmark", "json", ROWS, KAFKA_PARTITIONS);
      StringBuilder out =
          new StringBuilder(
              "\n##### NEXMARK DELTA SINK "
                  + "(Kafka JSON, memory state, mini-batch off; MOR upsert for updating queries; "
                  + ROWS
                  + " events, best of "
                  + RUNS
                  + ") #####\n"
                  + "output: "
                  + outputRoot.toAbsolutePath()
                  + (retainOutput ? " (retained)\n" : " (temporary)\n")
                  + "query  Flink s      ev/s  StreamFusion s      ev/s  SF/Flink\n");
      double logSpeedupSum = 0.0;
      int completed = 0;
      for (Query q : queries) {
        String row;
        try {
          double flink =
              deltaSinkBest(brokers, outputRoot, q, false, retainOutput, tableInitializer);
          double nativeRun =
              deltaSinkBest(brokers, outputRoot, q, true, retainOutput, tableInitializer);
          double speedup = flink / nativeRun;
          logSpeedupSum += Math.log(speedup);
          completed++;
          row =
              String.format(
                  "%4s  %7.3f  %8.0f  %14.3f  %8.0f  %8.2fx%n",
                  q.label, flink, ROWS / flink, nativeRun, ROWS / nativeRun, speedup);
        } catch (Exception failure) {
          if ("true".equals(System.getenv("SF_BENCHMARK_STACKTRACE"))) {
            failure.printStackTrace(System.out);
          }
          row = String.format("%4s  FAILED: %s%n", q.label, rootCause(failure));
        }
        out.append(row);
        System.out.print(row);
      }
      if (completed > 0) {
        out.append(
            String.format(
                "geomean (%d/%d completed): %.2fx%n",
                completed, queries.length, Math.exp(logSpeedupSum / completed)));
      }
      System.out.println(out);
    } finally {
      if (!retainOutput) {
        deleteTree(outputRoot);
      }
    }
  }

  private static double deltaSinkBest(
      String brokers,
      Path outputRoot,
      Query q,
      boolean nativeRun,
      boolean retainOutput,
      DeltaTableInitializer tableInitializer)
      throws Exception {
    return deltaSinkBest(
        brokers, outputRoot, q, nativeRun, retainOutput, WARMUP, RUNS, tableInitializer);
  }

  private static double deltaSinkBest(
      String brokers,
      Path outputRoot,
      Query q,
      boolean nativeRun,
      boolean retainOutput,
      int warmups,
      int runs,
      DeltaTableInitializer tableInitializer)
      throws Exception {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("streamfusion.native.enabled", Boolean.toString(nativeRun));
    if (nativeRun && q.nativeVariantProps != null) {
      properties.putAll(q.nativeVariantProps);
    }
    Map<String, String> previous = new LinkedHashMap<>();
    properties.forEach((key, value) -> previous.put(key, System.getProperty(key)));
    properties.forEach(System::setProperty);
    try {
      double best = Double.MAX_VALUE;
      for (int run = 0; run < warmups + runs; run++) {
        Path output =
            outputRoot.resolve(
                q.label + "/" + (nativeRun ? "streamfusion" : "flink") + "/run-" + run);
        System.out.printf(
            "  %s %s run %d/%d%s%n",
            q.label,
            nativeRun ? "StreamFusion" : "Flink",
            run + 1,
            warmups + runs,
            run < warmups ? " (warmup)" : "");
        double seconds = runDeltaSinkOnce(brokers, output, q, nativeRun, tableInitializer);
        System.out.printf("    completed in %.3f s%n", seconds);
        if (run >= warmups) {
          best = Math.min(best, seconds);
        }
        if (!retainOutput) {
          deleteTree(output);
        }
        // Every cell is a fully closed bounded Flink job with a fresh Delta table. Force the test
        // JVM to reclaim the previous MiniCluster/job graph before starting the next repetition;
        // otherwise several q10-sized cells can leave enough unreachable heap committed for macOS
        // to kill the fork before G1's next pressure-triggered collection.
        System.gc();
        System.runFinalization();
      }
      return best;
    } finally {
      previous.forEach(
          (key, value) -> {
            if (value == null) {
              System.clearProperty(key);
            } else {
              System.setProperty(key, value);
            }
          });
    }
  }

  /** Captures matched CPU and wall-clock profiles of one Kafka JSON to Delta MOR query. */
  static void runDeltaMergeOnReadSinkProfile(DeltaTableInitializer tableInitializer)
      throws Exception {
    String label = System.getProperty("profile.query", "q19");
    Query q =
        Arrays.stream(ALL_QUERIES)
            .filter(candidate -> candidate.label.equals(label))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("unknown profile.query: " + label));
    Path outputDir =
        Path.of(System.getProperty("profile.outputDir", "target/profiles/nexmark-delta"))
            .toAbsolutePath();
    Files.createDirectories(outputDir);
    String asprof = System.getProperty("profile.asprof", "asprof");
    String pid = Long.toString(ProcessHandle.current().pid());

    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      NexmarkKafkaBenchmark.produce(brokers, "nexmark", "json", ROWS, KAFKA_PARTITIONS);
      for (boolean nativeRun : new boolean[] {false, true}) {
        String engine = nativeRun ? "streamfusion" : "flink";
        Path engineOutput = outputDir.resolve(engine + "-output");
        deltaSinkBest(brokers, engineOutput, q, nativeRun, false, 1, 1, tableInitializer);
        String[] profileEvents =
            System.getProperty("profile.events", "cpu,wall").split(",");
        for (String event : profileEvents) {
          if (!Set.of("cpu", "wall", "alloc").contains(event)) {
            throw new IllegalArgumentException("unsupported profile event: " + event);
          }
          Path recording = outputDir.resolve(engine + "-" + q.label + "-" + event + ".jfr");
          List<String> startArgs =
              new ArrayList<>(
                  List.of(
                      "start",
                      "-e",
                      event,
                      "-i",
                      "1ms",
                      "-f",
                      recording.toString()));
          if (event.equals("wall")) {
            startArgs.add("-t");
          }
          startArgs.add(pid);
          runProfiler(asprof, startArgs.toArray(String[]::new));
          double seconds;
          try {
            seconds =
                deltaSinkBest(
                    brokers, engineOutput, q, nativeRun, false, 0, 1, tableInitializer);
          } finally {
            runProfiler(asprof, "stop", pid);
          }
          System.out.printf(
              "[profile-delta] %-12s %-4s %-4s %.3f s -> %s%n",
              engine, q.label, event, seconds, recording);
        }
      }
    }
  }

  private static double runDeltaSinkOnce(
      String brokers,
      Path output,
      Query q,
      boolean nativeRun,
      DeltaTableInitializer tableInitializer)
      throws Exception {
    StreamTableEnvironment tEnv = kafkaEnvironment(brokers, "json");
    tEnv.getConfig().getConfiguration().setString("execution.checkpointing.interval", "1 s");
    tEnv.getConfig().getConfiguration().setString("table.exec.mini-batch.enabled", "false");
    runSetup(tEnv, q);
    PhysicalPlanScan scan = nativeRun ? NativePlanner.install(tEnv) : null;
    tEnv.executeSql(deltaSinkDdl(q, output));
    org.apache.flink.table.types.logical.RowType sinkType =
        (org.apache.flink.table.types.logical.RowType)
            tEnv.from("sink").getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    tableInitializer.initialize(output, sinkType);
    String plan =
        tEnv.explainSql(q.insertSql, org.apache.flink.table.api.ExplainDetail.JSON_EXECUTION_PLAN);
    long start = System.nanoTime();
    tEnv.executeSql(q.insertSql).await();
    double seconds = (System.nanoTime() - start) / 1e9;
    if (nativeRun
        && (!plan.contains("NativeKafkaDecode")
            || !plan.contains("native-kafka-source")
            || plan.contains("RowDataToArrow")
            || plan.contains("ArrowToRowData")
            || scan.substitutions() < 3)) {
      throw new IllegalStateException(
          q.label
              + ": the native Kafka-to-Delta path did not engage (decode="
              + plan.contains("NativeKafkaDecode")
              + ", source="
              + plan.contains("native-kafka-source")
              + ", row-to-arrow="
              + plan.contains("RowDataToArrow")
              + ", arrow-to-row="
              + plan.contains("ArrowToRowData")
              + ", substitutions="
              + scan.substitutions()
              + "). "
              + scan.explainSummary());
    }
    return seconds;
  }

  private static String deltaSinkDdl(Query q, Path output) {
    // Delta timestamps have microsecond physical precision. The published 4.4 connector also reads
    // buffered BinaryRowData with precision 6 unconditionally; declaring that same precision here
    // prevents it from interpreting Flink's compact precision-3 timestamp layout as a 16-byte value.
    String ddl = q.sinkDdl.replace("%TS%", "TIMESTAMP_LTZ(6)").replace("%WTS%", "TIMESTAMP(6)");
    String key = UPSERT_KEYS.get(q.label);
    if (key != null) {
      ddl = ddl.replace(") WITH", ", PRIMARY KEY (" + key + ") NOT ENFORCED) WITH");
    }
    String options =
        "WITH ('connector' = 'delta', 'table_path' = '"
            + output.toUri()
            + "', 'write.mode' = '"
            + (key == null ? "append" : "upsert")
            + "', "
            + "'file_rolling.strategy' = 'count', 'file_rolling.count' = '-1')";
    return ddl.replace("WITH ('connector' = 'blackhole')", options);
  }

  @FunctionalInterface
  interface DeltaTableInitializer {
    void initialize(Path path, org.apache.flink.table.types.logical.RowType rowType) throws Exception;
  }

  private static void deleteTree(Path root) throws Exception {
    for (int attempt = 0; attempt < 10 && Files.exists(root); attempt++) {
      try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
        for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
          try {
            Files.deleteIfExists(path);
          } catch (java.nio.file.DirectoryNotEmptyException concurrentDeltaMaintenance) {
            // Delta's post-commit maintenance may finish creating a log artifact just after the
            // bounded job terminates. Re-walk below so benchmark cleanup does not fail a valid run.
          }
        }
      }
      if (Files.exists(root)) {
        Thread.sleep(100L);
      }
    }
    if (Files.exists(root)) {
      throw new java.nio.file.DirectoryNotEmptyException(root.toString());
    }
  }

  private static double parquetSinkBest(
      String brokers, Path outputRoot, Query q, boolean nativeRun) throws Exception {
    return parquetSinkBest(brokers, outputRoot, q, nativeRun, WARMUP, RUNS);
  }

  private static double parquetSinkBest(
      String brokers,
      Path outputRoot,
      Query q,
      boolean nativeRun,
      int warmup,
      int runs)
      throws Exception {
    String property = "streamfusion.native.enabled";
    String previous = System.getProperty(property);
    System.setProperty(property, Boolean.toString(nativeRun));
    try {
      double best = Double.MAX_VALUE;
      for (int run = 0; run < warmup + runs; run++) {
        Path output =
            outputRoot.resolve(
                q.label
                    + "/"
                    + (nativeRun ? "streamfusion" : "flink")
                    + "/run-"
                    + run);
        double seconds = runParquetSinkOnce(brokers, output, q, nativeRun);
        if (run >= warmup) {
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

  /** Captures matched steady-state CPU profiles of the q0 changelog Parquet sink boundary. */
  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PROFILE_PARQUET_SINK", matches = "true")
  void changelogParquetSinkProfile() throws Exception {
    String label = System.getProperty("profile.query", "q0");
    Query q =
        Arrays.stream(ALL_QUERIES)
            .filter(candidate -> candidate.label.equals(label))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("unknown profile.query: " + label));
    Path outputDir =
        Path.of(System.getProperty("profile.outputDir", "target/profiles/nexmark-parquet"))
            .toAbsolutePath();
    Files.createDirectories(outputDir);
    String asprof = System.getProperty("profile.asprof", "asprof");
    String pid = Long.toString(ProcessHandle.current().pid());
    long profileMillis = Long.getLong("profile.seconds", 20L) * 1000L;

    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      NexmarkKafkaBenchmark.produce(brokers, "nexmark", "json", ROWS, KAFKA_PARTITIONS);
      for (boolean nativeRun : new boolean[] {false, true}) {
        String engine = nativeRun ? "streamfusion" : "flink";
        Path engineOutput = outputDir.resolve(engine + "-output");
        parquetSinkBest(brokers, engineOutput.resolve("warmup"), q, nativeRun, 0, 1);
        Path recording = outputDir.resolve(engine + "-" + q.label + ".jfr");
        runProfiler(asprof, "start", "-e", "cpu", "-i", "1ms", "-f", recording.toString(), pid);
        long deadline = System.currentTimeMillis() + profileMillis;
        int iterations = 0;
        try {
          do {
            parquetSinkBest(
                brokers,
                engineOutput.resolve("profile-" + iterations),
                q,
                nativeRun,
                0,
                1);
            iterations++;
          } while (System.currentTimeMillis() < deadline);
        } finally {
          runProfiler(asprof, "stop", pid);
        }
        System.out.printf(
            "[profile-parquet] %-12s %-4s %d iterations -> %s%n",
            engine, q.label, iterations, recording);
      }
    }
  }

  private static double runParquetSinkOnce(
      String brokers, Path output, Query q, boolean nativeRun) throws Exception {
    StreamTableEnvironment tEnv = kafkaEnvironment(brokers, "json");
    tEnv.getConfig().getConfiguration().setString("execution.checkpointing.interval", "1 s");
    tEnv.getConfig().getConfiguration().setString("table.exec.mini-batch.enabled", "false");
    runSetup(tEnv, q);
    PhysicalPlanScan scan = nativeRun ? NativePlanner.install(tEnv) : null;
    tEnv.executeSql(changelogParquetSinkDdl(q, output));
    String plan =
        tEnv.explainSql(q.insertSql, org.apache.flink.table.api.ExplainDetail.JSON_EXECUTION_PLAN);
    long start = System.nanoTime();
    tEnv.executeSql(q.insertSql).await();
    double seconds = (System.nanoTime() - start) / 1e9;
    if (nativeRun
        && (!plan.contains("NativeKafkaDecode")
            || !plan.contains("native-kafka-source")
            || plan.contains("RowDataToArrow")
            || plan.contains("ArrowToRowData")
            || !plan.contains("native-parquet-partition-split")
            || scan.substitutions() < 3)) {
      throw new IllegalStateException(
          q.label + ": the native Kafka-to-query path or changelog Parquet sink did not engage. "
              + scan.explainSummary());
    }
    return seconds;
  }

  private static String changelogParquetSinkDdl(Query q, Path output) {
    String ddl = q.sinkDdl.replace("%TS%", "TIMESTAMP_LTZ(3)").replace("%WTS%", "TIMESTAMP(3)");
    String options =
        "WITH ('connector' = 'changelog-parquet', 'path' = '"
            + output.toUri()
            + "')";
    return ddl.replace("WITH ('connector' = 'blackhole')", options);
  }

  /**
   * The persistent-state-backend comparison on the readme's exactly-once Kafka pipeline: stock
   * Flink on RocksDB versus the native engine on the native RocksDB state backend, mini-batching off —
   * the same corpus, one-second checkpoints, exactly-once delivery, and best-of rule as {@link
   * #exactlyOnceKafkaSinkModeComparison}, with each engine's production disk backend swapped in.
   * A q4 preflight pins both backends actually engaging (RocksDB materializes working files
   * under a directed localdir; a live RocksDB store handle is observed), so a silent fall-back to
   * heap state cannot turn this into a heap-vs-heap comparison. Gated by
   * {@code SF_MATRIX_STATE_BACKENDS=true}.
   */
  @Test
  @EnabledIfEnvironmentVariable(named = "SF_MATRIX_STATE_BACKENDS", matches = "true")
  void stateBackendComparison() throws Exception {
    // SF_STATE_BACKENDS_MINI_BATCH=true runs the comparison in the tuned mini-batch mode (both
    // engines, the mode comparison's production-style configuration); "both" runs the off and on
    // tables in one pass, sharing the broker, the produced corpus, and the backend preflight.
    String modeEnv = System.getenv("SF_STATE_BACKENDS_MINI_BATCH");
    List<Boolean> modes =
        "both".equals(modeEnv)
            ? List.of(Boolean.FALSE, Boolean.TRUE)
            : List.of("true".equals(modeEnv));
    Query[] queries = selectQueries();
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
            .withEnv("KAFKA_TRANSACTION_MAX_TIMEOUT_MS", "7200000")
            // Deleted output topics free their segments immediately: the suites create and
            // delete one sizeable topic per run, and the default 60s delete delay lets pending
            // segments accumulate faster than they purge — enough to fill the Docker VM's disk
            // mid-suite and kill the broker.
            .withEnv("KAFKA_LOG_SEGMENT_DELETE_DELAY_MS", "0")) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      NexmarkKafkaBenchmark.produce(brokers, "nexmark", "json", ROWS, KAFKA_PARTITIONS);
      boolean preflighted = false;
      for (boolean miniBatch : modes) {
        Path rocksDir = Files.createTempDirectory("nexmark-rocksdb");
        Map<String, String> rocksdb = new LinkedHashMap<>();
        rocksdb.put("state.backend.type", "rocksdb");
        rocksdb.put("state.backend.rocksdb.localdir", rocksDir.toString());
        Map<String, String> nativeRocksDB = new LinkedHashMap<>();
        nativeRocksDB.put(
            "state.backend.type",
            "tech.streamfusion.state.RocksDBNativeStateBackendFactory");
        if (miniBatch) {
          for (Map<String, String> config : List.of(rocksdb, nativeRocksDB)) {
            config.put("table.exec.mini-batch.enabled", "true");
            config.put("table.exec.mini-batch.allow-latency", "2 s");
            config.put("table.exec.mini-batch.size", "50000");
          }
        }
        // SF_STATE_BACKENDS_OPTIONS adds job options (semicolon-separated key=value pairs) to
        // BOTH engines symmetrically — e.g. a tiny write buffer to exercise memtable flushing at
        // small event counts. The queries and data stay fixed; only backend tuning may vary.
        String extraOptions = System.getenv("SF_STATE_BACKENDS_OPTIONS");
        if (extraOptions != null && !extraOptions.isBlank()) {
          for (String pair : extraOptions.split(";")) {
            int split = pair.indexOf('=');
            for (Map<String, String> config : List.of(rocksdb, nativeRocksDB)) {
              config.put(pair.substring(0, split).trim(), pair.substring(split + 1).trim());
            }
          }
        }
        if (!preflighted) {
          assertStateBackendsEngage(brokers, rocksdb, nativeRocksDB, rocksDir);
          preflighted = true;
        }
        compareStateBackends(brokers, queries, rocksdb, nativeRocksDB, miniBatch);
      }
    }
  }

  private static void compareStateBackends(
      String brokers,
      Query[] queries,
      Map<String, String> rocksdb,
      Map<String, String> nativeRocksDB,
      boolean miniBatch)
      throws Exception {
    StringBuilder out =
        new StringBuilder(
            "\n##### NEXMARK STATE BACKENDS (exactly-once Kafka, mini-batch "
                + (miniBatch ? "on" : "off")
                + "; Flink RocksDB vs StreamFusion native RocksDB; "
                + ROWS
                + " events, best of "
                + RUNS
                + ") #####\n");
    out.append("query  Flink/RocksDB s      ev/s  SF/RocksDB s     ev/s  SF/Flink\n");
    for (Query q : queries) {
      String row;
      // See the mode comparison: a query lost to a transient stall is marked, not fatal.
      try {
        double flink = kafkaSinkBest(brokers, q, false, rocksdb);
        try {
          double nativeRun = kafkaSinkBest(brokers, q, true, nativeRocksDB);
          row =
              String.format(
                  "%4s  %15.3f  %8.0f  %11.3f  %8.0f  %7.2fx%n",
                  q.label,
                  flink,
                  ROWS / flink,
                  nativeRun,
                  ROWS / nativeRun,
                  flink / nativeRun);
        } catch (IllegalStateException fallback) {
          row = String.format("%4s  %15.3f  |  %s%n", q.label, flink, fallback.getMessage());
        }
      } catch (Exception failure) {
        row = String.format("%4s  FAILED: %s%n", q.label, rootCause(failure));
        if (rootCause(failure).contains("createTopics")) {
          // The broker itself is gone — every further cell would burn the same timeout.
          out.append(row);
          System.out.println(out);
          throw failure;
        }
      }
      out.append(row);
      System.out.print(row);
    }
    System.out.println(out);
  }

  /** q4 preflight proving both RocksDB backends engage under the job-level configuration. */
  private static void assertStateBackendsEngage(
      String brokers,
      Map<String, String> rocksdb,
      Map<String, String> nativeRocksDB,
      Path rocksDir)
      throws Exception {
    Query q4 =
        Arrays.stream(ALL_QUERIES).filter(q -> q.label.equals("q4")).findFirst().orElseThrow();
    AtomicBoolean rocksSeen = new AtomicBoolean();
    Thread rocksWatcher =
        engagementWatcher(
            rocksSeen,
            () -> {
              try (var files = Files.list(rocksDir)) {
                return files.findAny().isPresent();
              } catch (Exception e) {
                return false;
              }
            });
    try {
      kafkaSinkBest(brokers, q4, false, rocksdb, 0, 1);
    } finally {
      rocksWatcher.interrupt();
      rocksWatcher.join();
    }
    if (!rocksSeen.get()) {
      throw new IllegalStateException(
          "state.backend.type=rocksdb never materialized working files under its localdir;"
              + " the comparison would run stock Flink on heap state");
    }
    AtomicBoolean nativeRocksSeen = new AtomicBoolean();
    Thread nativeRocksWatcher =
        engagementWatcher(nativeRocksSeen, () -> Native.liveNativeHandles().contains("Rocks"));
    try {
      kafkaSinkBest(brokers, q4, true, nativeRocksDB, 0, 1);
    } finally {
      nativeRocksWatcher.interrupt();
      nativeRocksWatcher.join();
    }
    if (!nativeRocksSeen.get()) {
      throw new IllegalStateException(
          "the native RocksDB backend never engaged (no live RocksDB store handle was observed);"
              + " the comparison would run StreamFusion on memory state");
    }
  }

  private static Thread engagementWatcher(AtomicBoolean seen, BooleanSupplier probe) {
    Thread watcher =
        new Thread(
            () -> {
              while (!seen.get() && !Thread.currentThread().isInterrupted()) {
                if (probe.getAsBoolean()) {
                  seen.set(true);
                  return;
                }
                try {
                  Thread.sleep(50);
                } catch (InterruptedException e) {
                  return;
                }
              }
            });
    watcher.setDaemon(true);
    watcher.start();
    return watcher;
  }

  private static double kafkaSinkBest(
      String brokers, Query q, boolean nativeRun, Map<String, String> config) throws Exception {
    return kafkaSinkBest(brokers, q, nativeRun, config, WARMUP, RUNS);
  }

  /** The deepest cause — the actual failure under Flink's job-wrapper exception chain. */
  private static String rootCause(Throwable failure) {
    Throwable cause = failure;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause.toString();
  }

  private static double kafkaSinkBest(
      String brokers,
      Query q,
      boolean nativeRun,
      Map<String, String> config,
      int warmup,
      int runs)
      throws Exception {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("streamfusion.native.enabled", Boolean.toString(nativeRun));
    if (nativeRun && q.nativeVariantProps != null) {
      properties.putAll(q.nativeVariantProps);
    }
    Map<String, String> previous = new LinkedHashMap<>();
    properties.forEach((key, value) -> previous.put(key, System.getProperty(key)));
    properties.forEach(System::setProperty);
    try {
      double best = Double.MAX_VALUE;
      for (int run = 0; run < warmup + runs; run++) {
        double seconds = runKafkaSinkOnce(brokers, q, nativeRun, config);
        if (run >= warmup) {
          best = Math.min(best, seconds);
        }
      }
      return best;
    } finally {
      previous.forEach(
          (key, value) -> {
            if (value == null) {
              System.clearProperty(key);
            } else {
              System.setProperty(key, value);
            }
          });
    }
  }

  private static double runKafkaSinkOnce(
      String brokers, Query q, boolean nativeRun, Map<String, String> config) throws Exception {
    long t0 = System.nanoTime();
    StreamTableEnvironment tEnv = kafkaEnvironment(brokers, "json");
    tEnv.getConfig().getConfiguration().setString("execution.checkpointing.interval", "1 s");
    config.forEach((key, value) -> tEnv.getConfig().getConfiguration().setString(key, value));
    runSetup(tEnv, q);
    PhysicalPlanScan scan = nativeRun ? NativePlanner.install(tEnv) : null;
    String suffix = q.label + "-" + java.util.UUID.randomUUID();
    long tEnvReady = System.nanoTime();
    // Pre-create the output topic with one partition per sink subtask. Broker auto-creation gives
    // a single partition, which funnels every subtask's exactly-once writer into one partition
    // log — a sink-side ceiling that is not part of the workload (the corpus topic is already one
    // partition per source subtask).
    try (Admin admin =
        Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers))) {
      admin
          .createTopics(List.of(new NewTopic("nexmark-output-" + suffix, PARALLELISM, (short) 1)))
          .all()
          .get();
    }
    tEnv.executeSql(kafkaSinkDdl(q, brokers, "nexmark-output-" + suffix));
    long topicReady = System.nanoTime();
    String plan =
        tEnv.explainSql(
            q.insertSql, org.apache.flink.table.api.ExplainDetail.JSON_EXECUTION_PLAN);
    long explained = System.nanoTime();
    long start = System.nanoTime();
    tEnv.executeSql(q.insertSql).await();
    double seconds = (System.nanoTime() - start) / 1e9;
    long executed = System.nanoTime();
    // The exec-node and transformation names pin native serialization feeding Flink's KafkaSink.
    if (nativeRun
        && (!plan.contains("NativeKafkaDecode")
            || !plan.contains("native-kafka-source")
            || plan.contains("RowDataToArrow")
            || !plan.contains("NativeKafkaSink")
            || !plan.contains("flink-kafka-sink")
            || ("q3".equals(q.label)
                && (!plan.contains("NativeShare(consumers=[2])")
                    || countOccurrences(
                            plan, "\"contents\" : \"Source: native-kafka-source\"")
                        != 1))
            || scan.substitutions() < 2)) {
      throw new IllegalStateException(
          q.label
              + ": the required split-aware bytes-to-Arrow source and native Kafka serialization"
              + " path did not"
              + " engage. "
              + scan.explainSummary());
    }
    try (Admin admin =
        Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers))) {
      String topic = "nexmark-output-" + suffix;
      if (admin.listTopics().names().get().contains(topic)) {
        admin.deleteTopics(List.of(topic)).all().get();
      }
    }
    long cleaned = System.nanoTime();
    System.out.printf(
        "[phase] %s %s env=%.1fs topic=%.1fs explain=%.1fs execute=%.1fs delete=%.1fs%n",
        q.label,
        nativeRun ? "native" : "flink",
        (tEnvReady - t0) / 1e9,
        (topicReady - tEnvReady) / 1e9,
        (explained - topicReady) / 1e9,
        (executed - explained) / 1e9,
        (cleaned - executed) / 1e9);
    return seconds;
  }

  private static int countOccurrences(String text, String needle) {
    int count = 0;
    for (int at = text.indexOf(needle); at >= 0; at = text.indexOf(needle, at + 1)) {
      count++;
    }
    return count;
  }

  private static String kafkaSinkDdl(Query q, String brokers, String topic) {
    String ddl =
        q.sinkDdl.replace("%TS%", "TIMESTAMP_LTZ(3)").replace("%WTS%", "TIMESTAMP(3)");
    String key = UPSERT_KEYS.get(q.label);
    if (key != null) {
      ddl = ddl.replace(") WITH", ", PRIMARY KEY (" + key + ") NOT ENFORCED) WITH");
    }
    String transactionalId = "nexmark-" + topic;
    String producerBatching =
        "'properties.batch.size' = '"
            + KAFKA_PRODUCER_BATCH_BYTES
            + "', 'properties.linger.ms' = '"
            + KAFKA_PRODUCER_LINGER_MS
            + "', ";
    String options =
        key == null
            ? "WITH ('connector' = 'kafka', 'topic' = '"
                + topic
                + "', 'properties.bootstrap.servers' = '"
                + brokers
                + "', "
                + producerBatching
                + "'format' = 'json', 'sink.delivery-guarantee' = 'exactly-once', "
                + "'sink.transactional-id-prefix' = '"
                + transactionalId
                + "')"
            : "WITH ('connector' = 'upsert-kafka', 'topic' = '"
                + topic
                + "', 'properties.bootstrap.servers' = '"
                + brokers
                + "', "
                + producerBatching
                + "'key.format' = 'json', 'value.format' = 'json', "
                + "'sink.delivery-guarantee' = 'exactly-once', "
                + "'sink.transactional-id-prefix' = '"
                + transactionalId
                + "')";
    return ddl.replace("WITH ('connector' = 'blackhole')", options);
  }

  private static double generatorBestWithConfig(
      Query q, boolean nativeRun, Map<String, String> config) throws Exception {
    tableConfigExtras = config;
    try {
      return generatorBest(q, nativeRun, null);
    } finally {
      tableConfigExtras = Map.of();
    }
  }

  /** The changelog-family queries (mini-batch has no effect on the windowed ones), unless overridden. */
  private static Query[] selectTunedQueries() {
    if (System.getenv("SF_MATRIX_QUERIES") != null) {
      return selectQueries();
    }
    Set<String> family = Set.of("q3", "q4", "q9", "q15", "q16", "q17", "q18", "q19", "q20", "q23");
    return Arrays.stream(ALL_QUERIES).filter(q -> family.contains(q.label)).toArray(Query[]::new);
  }

  /**
   * Runs one query (default q19, override with {@code -Dprofile.query}) natively on the generator in a
   * loop for {@code -Dprofile.seconds} (default 60), so an attached sampler sees steady-state of the
   * changelog operator that query is bound by — no Kafka, no decode, just transpose → native island.
   * Gated by {@code SF_PROFILE=true} on top of {@code SF_BENCHMARK}; attach async-profiler to the fork.
   */
  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PROFILE", matches = "true")
  void generatorNativeProfileLoop() throws Exception {
    String label = System.getProperty("profile.query", "q19");
    Query q =
        Arrays.stream(ALL_QUERIES)
            .filter(x -> x.label.equals(label))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("unknown profile.query: " + label));
    // profile.native=false profiles the stock-Flink path instead, so the two can be diffed to isolate
    // what the native island actually spends beyond what Flink already pays (source/decode are shared).
    boolean nativeRun = !"false".equals(System.getProperty("profile.native", "true"));
    long deadline = System.currentTimeMillis() + Long.getLong("profile.seconds", 60L) * 1000L;
    long iterations = 0;
    Map<String, String> variant =
        "true".equals(System.getProperty("profile.variant")) ? q.nativeVariantProps : null;
    // -Dprofile.minibatch=true profiles the tuned (mini-batch) plan shape instead of the default.
    if ("true".equals(System.getProperty("profile.minibatch"))) {
      tableConfigExtras =
          Map.of(
              "table.exec.mini-batch.enabled", "true",
              "table.exec.mini-batch.allow-latency", "2 s",
              "table.exec.mini-batch.size", "50000");
    }
    try {
      while (System.currentTimeMillis() < deadline) {
        withProps(q, nativeRun, variant, () -> runGeneratorOnce(q, nativeRun));
        iterations++;
      }
    } finally {
      tableConfigExtras = Map.of();
    }
    System.out.println("[profile] " + (nativeRun ? "native " : "flink ") + label + " iterations: " + iterations);
  }

  /** Profiles one exactly-once Kafka input/output query repeatedly against a single broker. */
  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PROFILE_KAFKA_SINK", matches = "true")
  void exactlyOnceKafkaSinkProfileLoop() throws Exception {
    String label = System.getProperty("profile.query", "q19");
    Query q =
        Arrays.stream(ALL_QUERIES)
            .filter(candidate -> candidate.label.equals(label))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("unknown profile.query: " + label));
    boolean nativeRun = !"false".equals(System.getProperty("profile.native", "true"));
    Map<String, String> config = new LinkedHashMap<>();
    if ("true".equals(System.getProperty("profile.minibatch"))) {
      config.put("table.exec.mini-batch.enabled", "true");
      config.put("table.exec.mini-batch.allow-latency", "2 s");
      config.put("table.exec.mini-batch.size", "50000");
    }
    // profile.backend=rocksdb runs the loop on the Rust-owned persistent backend.
    if ("rocksdb".equals(System.getProperty("profile.backend"))) {
      config.put(
          "state.backend.type", "tech.streamfusion.state.RocksDBNativeStateBackendFactory");
    }
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
            .withEnv("KAFKA_TRANSACTION_MAX_TIMEOUT_MS", "7200000")
            // Deleted output topics free their segments immediately: the suites create and
            // delete one sizeable topic per run, and the default 60s delete delay lets pending
            // segments accumulate faster than they purge — enough to fill the Docker VM's disk
            // mid-suite and kill the broker.
            .withEnv("KAFKA_LOG_SEGMENT_DELETE_DELAY_MS", "0")) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      NexmarkKafkaBenchmark.produce(brokers, "nexmark", "json", ROWS, KAFKA_PARTITIONS);
      long deadline = System.currentTimeMillis() + Long.getLong("profile.seconds", 60L) * 1000L;
      long iterations = 0;
      do {
        kafkaSinkBest(brokers, q, nativeRun, config, 0, 1);
        iterations++;
      } while (System.currentTimeMillis() < deadline);
      System.out.println(
          "[profile] exactly-once Kafka "
              + (nativeRun ? "native " : "flink ")
              + label
              + " iterations: "
              + iterations);
    }
  }

  /**
   * Captures one matched async-profiler CPU recording for every selected exactly-once Kafka query
   * and engine while reusing one broker and one input corpus. Each cell gets one unprofiled warmup
   * followed by one profiled run. This keeps the profile matrix faithful to the front-page benchmark
   * without paying broker startup and corpus generation once per recording.
   */
  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PROFILE_ALL_KAFKA_SINK", matches = "true")
  void exactlyOnceKafkaSinkProfileAll() throws Exception {
    Path outputDir =
        Path.of(System.getProperty("profile.outputDir", "target/profiles/nexmark-memory-off"))
            .toAbsolutePath();
    Files.createDirectories(outputDir);
    String asprof = System.getProperty("profile.asprof", "asprof");
    String pid = Long.toString(ProcessHandle.current().pid());
    Map<String, String> config = Map.of();

    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
            .withEnv("KAFKA_TRANSACTION_MAX_TIMEOUT_MS", "7200000")
            .withEnv("KAFKA_LOG_SEGMENT_DELETE_DELAY_MS", "0")) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      NexmarkKafkaBenchmark.produce(brokers, "nexmark", "json", ROWS, KAFKA_PARTITIONS);
      for (Query q : selectQueries()) {
        for (boolean nativeRun : new boolean[] {false, true}) {
          String engine = nativeRun ? "streamfusion" : "flink";
          kafkaSinkBest(brokers, q, nativeRun, config, 0, 1);
          Path recording = outputDir.resolve(engine + "-" + q.label + ".jfr");
          runProfiler(asprof, "start", "-e", "cpu", "-i", "1ms", "-f", recording.toString(), pid);
          double seconds;
          try {
            seconds = kafkaSinkBest(brokers, q, nativeRun, config, 0, 1);
          } finally {
            runProfiler(asprof, "stop", pid);
          }
          System.out.printf(
              "[profile-all] %-12s %-4s %.3f s -> %s%n", engine, q.label, seconds, recording);
        }
      }
    }
  }

  private static void runProfiler(String executable, String... args) throws Exception {
    List<String> command = new ArrayList<>();
    command.add(executable);
    command.addAll(List.of(args));
    Process process = new ProcessBuilder(command).inheritIO().start();
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new IllegalStateException("async-profiler exited " + exitCode + ": " + command);
    }
  }

  private static Query[] selectQueries() {
    String subset = System.getenv("SF_MATRIX_QUERIES");
    if (subset == null) {
      return ALL_QUERIES;
    }
    Set<String> wanted = Set.copyOf(Arrays.asList(subset.split(",")));
    List<Query> picked = new ArrayList<>();
    for (Query q : ALL_QUERIES) {
      if (wanted.contains(q.label)) {
        picked.add(q);
      }
    }
    return picked.toArray(new Query[0]);
  }

  private static void runSetup(TableEnvironment tEnv, Query q) {
    if (q.setup != null) {
      for (String statement : q.setup) {
        tEnv.executeSql(statement);
      }
    }
  }

  private static String cell(String source, double flink, double nativeRun) {
    return String.format(
        "%-10s Flink %6.3fs (%,.0f ev/s)  |  Native %6.3fs (%,.0f ev/s)  %.2fx",
        source, flink, ROWS / flink, nativeRun, ROWS / nativeRun, flink / nativeRun);
  }

  private static String skipCell(String source, String reason) {
    return String.format("%-10s skipped (%s)", source, reason);
  }

  private static String variantCell(String source, String label, double flink, double variant) {
    return String.format(
        "%-10s [%s]  Native %6.3fs (%,.0f ev/s)  %.2fx",
        source, label, variant, ROWS / variant, flink / variant);
  }

  // ----- generator source -----

  private static double generatorBest(Query q, boolean nativeRun, Map<String, String> extra)
      throws Exception {
    double best = Double.MAX_VALUE;
    for (int run = 0; run < WARMUP + RUNS; run++) {
      double seconds = withProps(q, nativeRun, extra, () -> runGeneratorOnce(q, nativeRun));
      if (run >= WARMUP) {
        best = Math.min(best, seconds);
      }
    }
    return best;
  }

  private static double runGeneratorOnce(Query q, boolean nativeRun) throws Exception {
    TableEnvironment tEnv = NexmarkBenchmark.environment(ROWS);
    tableConfigExtras.forEach((k, v) -> tEnv.getConfig().getConfiguration().setString(k, v));
    tEnv.createTemporarySystemFunction("count_char", CountChar.class);
    runSetup(tEnv, q);
    PhysicalPlanScan scan = nativeRun ? NativePlanner.install(tEnv) : null;
    return execute(tEnv, scan, q, nativeRun, "TIMESTAMP(3)");
  }

  // ----- parquet file source -----

  /** Writes the wide event row to a fresh local Parquet directory once; every query reads it back. */
  private static Path writeParquetSource() throws Exception {
    Path dir = Files.createTempDirectory("bench-nexmark-parquet");
    TableEnvironment tEnv = NexmarkBenchmark.environment(ROWS);
    tEnv.executeSql(
        "CREATE TABLE parquet_write ("
            + PARQUET_SCHEMA
            + ") WITH ('connector' = 'filesystem', 'path' = '"
            + dir.toUri()
            + "', 'format' = 'parquet')");
    tEnv.executeSql(
            "INSERT INTO parquet_write SELECT event_type, person, auction, bid, `dateTime` FROM events")
        .await();
    return dir;
  }

  private static double parquetBest(Path dir, Query q, boolean nativeRun, Map<String, String> extra)
      throws Exception {
    double best = Double.MAX_VALUE;
    for (int run = 0; run < WARMUP + RUNS; run++) {
      double seconds = withProps(q, nativeRun, extra, () -> runParquetOnce(dir, nativeRun, q));
      if (run >= WARMUP) {
        best = Math.min(best, seconds);
      }
    }
    return best;
  }

  private static double runParquetOnce(Path dir, boolean nativeRun, Query q) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.getConfig().enableObjectReuse();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(
        "CREATE TABLE src ("
            + PARQUET_SCHEMA
            + ", WATERMARK FOR `dateTime` AS `dateTime` - INTERVAL '4' SECOND"
            + ") WITH ('connector' = 'filesystem', 'path' = '"
            + dir.toUri()
            + "', 'format' = 'parquet')");
    // The same person/auction/bid logical streams as the generator, off the watermarked event-time
    // `dateTime` (a plain TIMESTAMP(3) here, so DATE_FORMAT/HOUR stay native).
    tEnv.executeSql(
        "CREATE TEMPORARY VIEW person AS SELECT person.id AS id, person.name AS name,"
            + " person.emailAddress AS emailAddress, person.creditCard AS creditCard, person.city AS"
            + " city, person.state AS state, `dateTime`, person.extra AS extra FROM src WHERE"
            + " event_type = 0");
    tEnv.executeSql(
        "CREATE TEMPORARY VIEW auction AS SELECT auction.id AS id, auction.itemName AS itemName,"
            + " auction.description AS description, auction.initialBid AS initialBid, auction.reserve"
            + " AS reserve, `dateTime`, auction.expires AS expires, auction.seller AS seller,"
            + " auction.category AS category, auction.extra AS extra FROM src WHERE event_type = 1");
    tEnv.executeSql(
        "CREATE TEMPORARY VIEW bid AS SELECT bid.auction AS auction, bid.bidder AS bidder, bid.price"
            + " AS price, bid.channel AS channel, bid.url AS url, `dateTime`, bid.extra AS extra FROM"
            + " src WHERE event_type = 2");
    tEnv.createTemporarySystemFunction("count_char", CountChar.class);
    runSetup(tEnv, q);
    PhysicalPlanScan scan = nativeRun ? NativePlanner.install(tEnv) : null;
    return execute(tEnv, scan, q, nativeRun, "TIMESTAMP(3)");
  }

  // ----- kafka source -----

  /**
   * One Kafka cell for a query: the Flink baseline plus the two source/decode rungs, under the given extra
   * native props (null = the byte-parity default; the variant props = the allowIncompatible path). The
   * label prefix distinguishes the two rows.
   */
  private static String kafkaCell(
      String brokers,
      String format,
      Query q,
      double flink,
      String variantLabel,
      Map<String, String> extraProps)
      throws Exception {
    StringBuilder cell = new StringBuilder();
    cell.append(
        variantLabel == null
            ? String.format("kafka/%-8s Flink %6.3fs", format, flink)
            : String.format("kafka/%-8s [%s]", format, variantLabel));
    for (Rung rung : new Rung[] {Rung.JVM_TRANSPOSE, Rung.RUST_DECODE}) {
      double s = kafkaBest(brokers, format, rung, q, extraProps);
      cell.append(String.format("  | %s %6.3fs %.2fx", rung.label, s, flink / s));
    }
    return cell.toString();
  }

  private static double kafkaBest(
      String brokers, String format, Rung rung, Query q, Map<String, String> extraProps)
      throws Exception {
    Map<String, String> previous = new LinkedHashMap<>();
    boolean nativeRun = !"false".equals(rung.properties.get("streamfusion.native.enabled"));
    Map<String, String> props = new LinkedHashMap<>(rung.properties);
    if (nativeRun && q.approximateDecimal) {
      props.put("streamfusion.expression.decimalArithmetic.approximate", "true");
    }
    if (nativeRun && extraProps != null) {
      props.putAll(extraProps);
    }
    props.forEach((k, v) -> previous.put(k, System.getProperty(k)));
    props.forEach(System::setProperty);
    try {
      double best = Double.MAX_VALUE;
      for (int run = 0; run < WARMUP + RUNS; run++) {
        double seconds = runKafkaOnce(brokers, format, nativeRun, q);
        if (run >= WARMUP) {
          best = Math.min(best, seconds);
        }
      }
      return best;
    } finally {
      previous.forEach(
          (k, v) -> {
            if (v == null) {
              System.clearProperty(k);
            } else {
              System.setProperty(k, v);
            }
          });
    }
  }

  private static double runKafkaOnce(String brokers, String format, boolean nativeRun, Query q)
      throws Exception {
    StreamTableEnvironment tEnv = kafkaEnvironment(brokers, format);
    runSetup(tEnv, q);
    PhysicalPlanScan scan = nativeRun ? NativePlanner.install(tEnv) : null;
    return execute(tEnv, scan, q, nativeRun, "TIMESTAMP_LTZ(3)");
  }

  private static StreamTableEnvironment kafkaEnvironment(String brokers, String format) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(PARALLELISM);
    if (!"false".equals(System.getenv("SF_OBJECT_REUSE"))) {
      env.getConfig().enableObjectReuse();
    }
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(
        "CREATE TABLE src ("
            + NexmarkKafkaBenchmark.SCHEMA
            + ", rowtime AS TO_TIMESTAMP_LTZ(`dateTime`, 3),"
            + " WATERMARK FOR rowtime AS rowtime - INTERVAL '4' SECOND"
            + ") WITH ('connector' = 'kafka', 'topic' = 'nexmark', 'properties.bootstrap.servers' = '"
            + brokers
            + "', 'properties.group.id' = 'nexmark', 'properties.max.poll.records' = '8192',"
            + " 'scan.startup.mode' = 'earliest-offset',"
            + " 'scan.bounded.mode' = 'latest-offset', 'format' = '"
            + format
            + "'"
            + ("protobuf".equals(format)
                ? ", 'protobuf.message-class-name' = 'tech.streamfusion.proto.NexmarkEvent'"
                : "")
            + ")");
    // The same person/auction/bid logical streams the published Nexmark queries read, off the
    // watermarked event-time rowtime. expires becomes a timestamp too so q4/q9's BETWEEN typechecks.
    tEnv.executeSql(
        "CREATE TEMPORARY VIEW person AS SELECT person.id AS id, person.name AS name,"
            + " person.emailAddress AS emailAddress, person.creditCard AS creditCard, person.city AS"
            + " city, person.state AS state, rowtime AS `dateTime`, person.extra AS extra FROM src"
            + " WHERE event_type = 0");
    tEnv.executeSql(
        "CREATE TEMPORARY VIEW auction AS SELECT auction.id AS id, auction.itemName AS itemName,"
            + " auction.description AS description, auction.initialBid AS initialBid, auction.reserve"
            + " AS reserve, rowtime AS `dateTime`, TO_TIMESTAMP_LTZ(auction.expires, 3) AS expires,"
            + " auction.seller AS seller, auction.category AS category, auction.extra AS extra FROM"
            + " src WHERE event_type = 1");
    tEnv.executeSql(
        "CREATE TEMPORARY VIEW bid AS SELECT bid.auction AS auction, bid.bidder AS bidder, bid.price"
            + " AS price, bid.channel AS channel, bid.url AS url, rowtime AS `dateTime`, bid.extra AS"
            + " extra FROM src WHERE event_type = 2");
    tEnv.createTemporarySystemFunction("count_char", CountChar.class);
    return tEnv;
  }

  // ----- shared -----

  private static double execute(
      TableEnvironment tEnv, PhysicalPlanScan scan, Query q, boolean nativeRun, String tsType)
      throws Exception {
    // %TS% = the event-time rowtime passthrough (plain TIMESTAMP on the generator, TIMESTAMP_LTZ off
    // the Kafka epoch decode). %WTS% = a window-boundary column (window_start/_end, SESSION_START/_END),
    // which Flink always types as a plain TIMESTAMP even over an LTZ rowtime.
    tEnv.executeSql(q.sinkDdl.replace("%TS%", tsType).replace("%WTS%", "TIMESTAMP(3)"));
    long start = System.nanoTime();
    tEnv.executeSql(q.insertSql).await();
    double seconds = (System.nanoTime() - start) / 1e9;
    if (nativeRun && scan.substitutions() == 0) {
      throw new IllegalStateException(
          q.label + ": native island did not engage; comparison is moot. " + scan.fallbackReasons());
    }
    return seconds;
  }

  @FunctionalInterface
  private interface Run {
    double get() throws Exception;
  }

  private static double withProps(Query q, boolean nativeRun, Map<String, String> extra, Run run)
      throws Exception {
    Map<String, String> props = new LinkedHashMap<>();
    if (nativeRun && q.approximateDecimal) {
      props.put("streamfusion.expression.decimalArithmetic.approximate", "true");
    }
    if (nativeRun && extra != null) {
      props.putAll(extra);
    }
    if (props.isEmpty()) {
      return run.get();
    }
    Map<String, String> previous = new LinkedHashMap<>();
    props.forEach((k, v) -> previous.put(k, System.getProperty(k)));
    props.forEach(System::setProperty);
    try {
      return run.get();
    } finally {
      previous.forEach(
          (k, v) -> {
            if (v == null) {
              System.clearProperty(k);
            } else {
              System.setProperty(k, v);
            }
          });
    }
  }
}

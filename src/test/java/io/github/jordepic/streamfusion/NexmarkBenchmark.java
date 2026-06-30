package io.github.jordepic.streamfusion;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * End-to-end throughput of the first Nexmark queries (q0–q4) against stock Flink, each run once with
 * native substitution and once without, reporting rows/s and the speedup. Opt-in (millions of rows) —
 * enable with {@code SF_BENCHMARK=true}, run under {@code -Pbench} (release native build).
 *
 * <p>Faithful to the real Nexmark plan's perimeter: the source is the wide rowwise event row (the
 * {@code nexmark} datagen shape — {@code event_type} + nested {@code person}/{@code auction}/{@code
 * bid} structs, watermarked on the event {@code dateTime}), and the sinks are {@code blackhole} (also
 * rowwise). So a native island pays a RowData→Arrow transpose at the source and an Arrow→RowData
 * transpose at the sink, exactly as a real deployment would — the steelman per CLAUDE.md. The only
 * thing synthetic is the generator's value distribution; the schema shape, nested field access, and
 * operators exercised match the published queries.
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NexmarkBenchmark {

  private static final long ROWS =
      System.getenv("SF_ROWS") != null ? Long.parseLong(System.getenv("SF_ROWS")) : 2_000_000L;
  private static final int WARMUP = 1;
  private static final int RUNS = 2;

  /** nexmark default-ish mix per 50 events: 1 person, 3 auctions, 46 bids (~92% bids). */
  private static final int BLOCK = 50;
  private static final long BASE_MILLIS = LocalDateTime.of(2024, 1, 1, 0, 0, 0)
      .toInstant(ZoneOffset.UTC).toEpochMilli();
  private static final String[] STATES = {"OR", "ID", "CA", "WA", "NY", "TX"};

  @Test
  void q0PassThrough() throws Exception {
    compare(
        "q0 pass-through (project bid fields)",
        false,
        "CREATE TABLE nexmark_q0 (auction BIGINT, bidder BIGINT, price BIGINT, `dateTime`"
            + " TIMESTAMP(3), extra STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO nexmark_q0 SELECT auction, bidder, price, `dateTime`, extra FROM bid");
  }

  @Test
  void q1CurrencyConversion() throws Exception {
    // 0.908 * price is DECIMAL arithmetic; routes only with the approximate-decimal flag (off by
    // default). Computed in double then cast to DECIMAL(23,3) — for throughput, not byte-exactness.
    compare(
        "q1 currency conversion (0.908 * price)",
        true,
        "CREATE TABLE nexmark_q1 (auction BIGINT, bidder BIGINT, price DECIMAL(23, 3), `dateTime`"
            + " TIMESTAMP(3), extra STRING) WITH ('connector' = 'blackhole')",
        "INSERT INTO nexmark_q1 SELECT auction, bidder, 0.908 * price AS price, `dateTime`, extra"
            + " FROM bid");
  }

  @Test
  void q2Filter() throws Exception {
    compare(
        "q2 filter (MOD(auction, 123) = 0)",
        false,
        "CREATE TABLE nexmark_q2 (auction BIGINT, price BIGINT) WITH ('connector' = 'blackhole')",
        "INSERT INTO nexmark_q2 SELECT auction, price FROM bid WHERE MOD(auction, 123) = 0");
  }

  @Test
  void q3IncrementalJoin() throws Exception {
    compare(
        "q3 incremental join (auction x person on seller)",
        false,
        "CREATE TABLE nexmark_q3 (name STRING, city STRING, state STRING, id BIGINT) WITH"
            + " ('connector' = 'blackhole')",
        "INSERT INTO nexmark_q3 SELECT P.name, P.city, P.state, A.id FROM auction AS A INNER JOIN"
            + " person AS P ON A.seller = P.id WHERE A.category = 10 AND (P.state = 'OR' OR P.state"
            + " = 'ID' OR P.state = 'CA')");
  }

  @Test
  void q4AverageWinningBid() throws Exception {
    compare(
        "q4 average winning bid per category",
        false,
        "CREATE TABLE nexmark_q4 (id BIGINT, final BIGINT) WITH ('connector' = 'blackhole')",
        "INSERT INTO nexmark_q4 SELECT Q.category, AVG(Q.final) FROM (SELECT MAX(B.price) AS final,"
            + " A.category FROM auction A, bid B WHERE A.id = B.auction AND B.`dateTime` BETWEEN"
            + " A.`dateTime` AND A.expires GROUP BY A.id, A.category) Q GROUP BY Q.category");
  }

  private static void compare(
      String label, boolean approximateDecimal, String sinkDdl, String insertSql) throws Exception {
    double flink = bestOf(false, approximateDecimal, sinkDdl, insertSql);
    double nativeRun = bestOf(true, approximateDecimal, sinkDdl, insertSql);
    System.out.printf("%n[benchmark] %s over %,d events (best of %d)%n", label, ROWS, RUNS);
    System.out.printf("[benchmark]   Flink : %6.3f s  (%,.0f events/s)%n", flink, ROWS / flink);
    System.out.printf(
        "[benchmark]   Native: %6.3f s  (%,.0f events/s)  %.2fx vs Flink%n",
        nativeRun, ROWS / nativeRun, flink / nativeRun);
  }

  private static double bestOf(
      boolean useNative, boolean approximateDecimal, String sinkDdl, String insertSql)
      throws Exception {
    double best = Double.MAX_VALUE;
    for (int run = 0; run < WARMUP + RUNS; run++) {
      double seconds = runOnce(useNative, approximateDecimal, sinkDdl, insertSql);
      if (run >= WARMUP) {
        best = Math.min(best, seconds);
      }
    }
    return best;
  }

  private static double runOnce(
      boolean useNative, boolean approximateDecimal, String sinkDdl, String insertSql)
      throws Exception {
    String property = "streamfusion.expression.decimalArithmetic.approximate";
    String previous = System.getProperty(property);
    if (useNative && approximateDecimal) {
      System.setProperty(property, "true");
    }
    try {
      TableEnvironment tEnv = environment();
      PhysicalPlanScan scan = useNative ? NativePlanner.install(tEnv) : null;
      tEnv.executeSql(sinkDdl);
      long start = System.nanoTime();
      tEnv.executeSql(insertSql).await();
      double seconds = (System.nanoTime() - start) / 1e9;
      if (useNative && scan.substitutions() == 0) {
        throw new IllegalStateException(
            "native substitution did not engage; comparison is moot. fallback reasons: "
                + scan.fallbackReasons());
      }
      return seconds;
    } finally {
      if (previous == null) {
        System.clearProperty(property);
      } else {
        System.setProperty(property, previous);
      }
    }
  }

  /**
   * Runs native q0 (the pass-through) in a loop for {@code -Dprofile.seconds} (default 60) so an
   * attached sampler sees steady-state q0: source generate → RowData→Arrow transpose → native Calc →
   * Arrow→RowData transpose → blackhole sink. Gated by {@code SF_PROFILE=true} (in addition to the
   * class-level {@code SF_BENCHMARK}); attach async-profiler to the surefire fork while it runs.
   */
  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PROFILE", matches = "true")
  void q0NativeProfileLoop() throws Exception {
    String sinkDdl =
        "CREATE TABLE nexmark_q0 (auction BIGINT, bidder BIGINT, price BIGINT, `dateTime`"
            + " TIMESTAMP(3), extra STRING) WITH ('connector' = 'blackhole')";
    String insertSql =
        "INSERT INTO nexmark_q0 SELECT auction, bidder, price, `dateTime`, extra FROM bid";
    long deadline = System.currentTimeMillis() + Long.getLong("profile.seconds", 60L) * 1000L;
    long iterations = 0;
    while (System.currentTimeMillis() < deadline) {
      TableEnvironment tEnv = environment();
      PhysicalPlanScan scan = NativePlanner.install(tEnv);
      tEnv.executeSql(sinkDdl);
      tEnv.executeSql(insertSql).await();
      if (scan.substitutions() == 0) {
        throw new IllegalStateException("native q0 did not engage: " + scan.fallbackReasons());
      }
      iterations++;
    }
    System.out.println("[profile] native q0 iterations completed: " + iterations);
  }

  static TableEnvironment environment() {
    return environment(ROWS);
  }

  static TableEnvironment environment(long rows) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    // Object reuse (a standard tuned-prod setting, enabled for both the Flink baseline and the native
    // run) drops Flink's per-handoff defensive record copy, so the transpose's reused ColumnarRowData
    // flows to the sink without being materialized + boxed into a GenericRowData. The Flink SQL runtime
    // stays correct under it (the planner copies only where an operator retains references).
    env.getConfig().enableObjectReuse();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromSequence(0, rows - 1)
            .map(NexmarkBenchmark::event)
            .returns(eventType())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(4))
                    .withTimestampAssigner(
                        (row, ts) ->
                            ((LocalDateTime) row.getField(4))
                                .toInstant(ZoneOffset.UTC)
                                .toEpochMilli()));
    tEnv.createTemporaryView("events", source, eventSchema());
    tEnv.executeSql(
        "CREATE TEMPORARY VIEW person AS SELECT person.id AS id, person.name AS name,"
            + " person.emailAddress AS emailAddress, person.creditCard AS creditCard, person.city AS"
            + " city, person.state AS state, `dateTime`, person.extra AS extra FROM events WHERE"
            + " event_type = 0");
    tEnv.executeSql(
        "CREATE TEMPORARY VIEW auction AS SELECT auction.id AS id, auction.itemName AS itemName,"
            + " auction.description AS description, auction.initialBid AS initialBid, auction.reserve"
            + " AS reserve, `dateTime`, auction.expires AS expires, auction.seller AS seller,"
            + " auction.category AS category, auction.extra AS extra FROM events WHERE event_type ="
            + " 1");
    tEnv.executeSql(
        "CREATE TEMPORARY VIEW bid AS SELECT bid.auction AS auction, bid.bidder AS bidder, bid.price"
            + " AS price, bid.channel AS channel, bid.url AS url, `dateTime`, bid.extra AS extra FROM"
            + " events WHERE event_type = 2");
    return tEnv;
  }

  /** Deterministic wide event row for index {@code i}, mirroring the nexmark person/auction/bid mix. */
  private static Row event(long i) {
    long block = i / BLOCK;
    int pos = (int) (i % BLOCK);
    // One block per second; events spread by 10 ms within the block so bids land inside the auction's
    // 20 s expiry window (q4's interval join) and the rowtime stays monotonic across blocks.
    long millis = BASE_MILLIS + block * 1000L + pos * 10L;
    LocalDateTime ts = LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(millis), ZoneOffset.UTC);
    if (pos == 0) {
      Row person =
          Row.of(
              block,
              "person-" + block,
              "p" + block + "@nexmark.test",
              "1234-5678-9012-3456",
              "city-" + (block % 1000),
              STATES[(int) (block % STATES.length)],
              ts,
              "pextra");
      return Row.of(0, person, null, null, ts);
    }
    if (pos <= 3) {
      long auctionId = block * 3 + (pos - 1);
      Row auction =
          Row.of(
              auctionId,
              "item-" + auctionId,
              "desc-" + auctionId,
              10L,
              50L,
              ts,
              ts.plusSeconds(20),
              block, // seller = this block's person id
              block % 100, // category; q3 keeps category = 10
              "aextra");
      return Row.of(1, null, auction, null, ts);
    }
    long auctionId = block * 3 + (pos % 3);
    Row bid =
        Row.of(
            auctionId,
            (long) pos,
            (long) ((i % 1000) + 1),
            "channel-" + (pos % 8),
            "https://nexmark.test/" + auctionId,
            ts,
            "bextra");
    return Row.of(2, null, null, bid, ts);
  }

  @SuppressWarnings("unchecked")
  private static org.apache.flink.api.common.typeinfo.TypeInformation<Row> eventType() {
    return Types.ROW_NAMED(
        new String[] {"event_type", "person", "auction", "bid", "dateTime"},
        Types.INT,
        Types.ROW_NAMED(
            new String[] {
              "id", "name", "emailAddress", "creditCard", "city", "state", "dateTime", "extra"
            },
            Types.LONG,
            Types.STRING,
            Types.STRING,
            Types.STRING,
            Types.STRING,
            Types.STRING,
            Types.LOCAL_DATE_TIME,
            Types.STRING),
        Types.ROW_NAMED(
            new String[] {
              "id", "itemName", "description", "initialBid", "reserve", "dateTime", "expires",
              "seller", "category", "extra"
            },
            Types.LONG,
            Types.STRING,
            Types.STRING,
            Types.LONG,
            Types.LONG,
            Types.LOCAL_DATE_TIME,
            Types.LOCAL_DATE_TIME,
            Types.LONG,
            Types.LONG,
            Types.STRING),
        Types.ROW_NAMED(
            new String[] {"auction", "bidder", "price", "channel", "url", "dateTime", "extra"},
            Types.LONG,
            Types.LONG,
            Types.LONG,
            Types.STRING,
            Types.STRING,
            Types.LOCAL_DATE_TIME,
            Types.STRING),
        Types.LOCAL_DATE_TIME);
  }

  private static Schema eventSchema() {
    return Schema.newBuilder()
        .column("event_type", DataTypes.INT())
        .column(
            "person",
            DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT()),
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("emailAddress", DataTypes.STRING()),
                DataTypes.FIELD("creditCard", DataTypes.STRING()),
                DataTypes.FIELD("city", DataTypes.STRING()),
                DataTypes.FIELD("state", DataTypes.STRING()),
                DataTypes.FIELD("dateTime", DataTypes.TIMESTAMP(3)),
                DataTypes.FIELD("extra", DataTypes.STRING())))
        .column(
            "auction",
            DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT()),
                DataTypes.FIELD("itemName", DataTypes.STRING()),
                DataTypes.FIELD("description", DataTypes.STRING()),
                DataTypes.FIELD("initialBid", DataTypes.BIGINT()),
                DataTypes.FIELD("reserve", DataTypes.BIGINT()),
                DataTypes.FIELD("dateTime", DataTypes.TIMESTAMP(3)),
                DataTypes.FIELD("expires", DataTypes.TIMESTAMP(3)),
                DataTypes.FIELD("seller", DataTypes.BIGINT()),
                DataTypes.FIELD("category", DataTypes.BIGINT()),
                DataTypes.FIELD("extra", DataTypes.STRING())))
        .column(
            "bid",
            DataTypes.ROW(
                DataTypes.FIELD("auction", DataTypes.BIGINT()),
                DataTypes.FIELD("bidder", DataTypes.BIGINT()),
                DataTypes.FIELD("price", DataTypes.BIGINT()),
                DataTypes.FIELD("channel", DataTypes.STRING()),
                DataTypes.FIELD("url", DataTypes.STRING()),
                DataTypes.FIELD("dateTime", DataTypes.TIMESTAMP(3)),
                DataTypes.FIELD("extra", DataTypes.STRING())))
        .column("dateTime", DataTypes.TIMESTAMP(3))
        .watermark("dateTime", "SOURCE_WATERMARK()")
        .build();
  }
}

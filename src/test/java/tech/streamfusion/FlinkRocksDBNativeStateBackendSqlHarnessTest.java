package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

/**
 * The RocksDB state backend behind Flink's normal backend toggle: with {@code state.backend.type}
 * set to the StreamFusion factory, a native group aggregate keeps its state in a local RocksDB
 * table (read-through probes, barrier commits) and must produce exactly the host's results; host
 * (fallback) operators in the same job run unchanged on the wrapped hashmap backend. MIN/MAX keep
 * multiset state, which the RocksDB row codec does not carry — that query exercises the
 * per-operator fallback to memory state under the same backend.
 *
 * <p>Every run here is the production shape: Rust reads and writes its RocksDB instance directly,
 * while Java coordinates Flink checkpoint handles and uploads.
 */
class FlinkRocksDBNativeStateBackendSqlHarnessTest {

  // Collapsed-changelog parity: the bounded filesystem source may split the input across part
  // files whose read order differs run to run, so the raw -U/+U interleaving is not stable here.
  // Per-row changelog parity on the RocksDB backend is covered deterministically by the operator
  // harness test; this verifies the materialized end state through the whole SQL stack.

  @Test
  void groupBySumOnRocksDBBackendMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("rocksdb-sum-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        () -> rocksdbEnvironment(input), "SELECT k, SUM(v) AS total, COUNT(*) AS c FROM t GROUP BY k");
  }

  @Test
  void groupBySumWithTtlOnRocksDBBackendMatchesHost() throws Exception {
    // Idle-state TTL no longer forces the memory fallback: the store carries the last-write
    // timestamps in the state table's trailing ts column. 1h retention — nothing expires
    // in-test; the operator-harness TTL test covers expiry and proves the RocksDB route (its
    // snapshot is an incremental RocksDB handle). This pins the end-to-end SQL result.
    Path input = Files.createTempDirectory("rocksdb-ttl-sum-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        () -> {
          TableEnvironment tEnv = rocksdbEnvironment(input);
          tEnv.getConfig().set("table.exec.state.ttl", "1 h");
          return tEnv;
        },
        "SELECT k, SUM(v) AS total, COUNT(*) AS c FROM t GROUP BY k");
  }

  @Test
  void joinWithTtlOnRocksDBBackendMatchesHost() throws Exception {
    // A TTL'd regular join no longer forces the memory fallback: each side's RocksDB table
    // carries per-entry last-write timestamps in its trailing ts column. 1h retention — nothing
    // expires in-test; the native tests cover per-side expiry and tombstoning. This pins the
    // end-to-end SQL result on the RocksDB route.
    Path input = Files.createTempDirectory("rocksdb-ttl-join-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        () -> {
          TableEnvironment tEnv = rocksdbEnvironment(input);
          tEnv.getConfig().set("table.exec.state.ttl", "1 h");
          return tEnv;
        },
        "SELECT a.k, a.v, b.v FROM t a JOIN t b ON a.k = b.k");
  }

  @Test
  void topNWithTtlOnRocksDBBackendMatchesHost() throws Exception {
    // A TTL'd append-only Top-N stays on the RocksDB list store: element timestamps round-trip
    // through the ts column and the ranker's own first-touch prune enforces expiry (nothing
    // expires under the 1h retention here; the native tests cover expiry after restore).
    Path input = Files.createTempDirectory("rocksdb-ttl-topn-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        () -> {
          TableEnvironment tEnv = rocksdbEnvironment(input);
          tEnv.getConfig().set("table.exec.state.ttl", "1 h");
          return tEnv;
        },
        "SELECT k, v FROM (SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v DESC)"
            + " AS rn FROM t) WHERE rn <= 2");
  }

  @Test
  void retractingTopNWithTtlOnRocksDBBackendMatchesHost() throws Exception {
    // The retracting ranker's whole-buffer clock rides the head element's ts; with retention on
    // both stateful operators here (aggregate + rank) keep their state in TTL'd RocksDB tables.
    Path input = Files.createTempDirectory("rocksdb-ttl-retopn-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        () -> {
          TableEnvironment tEnv = rocksdbEnvironment(input);
          tEnv.getConfig().set("table.exec.state.ttl", "1 h");
          return tEnv;
        },
        "SELECT k, total FROM (SELECT k, total, ROW_NUMBER() OVER (ORDER BY total DESC) AS rn"
            + " FROM (SELECT k, SUM(v) AS total FROM t GROUP BY k)) WHERE rn <= 2");
  }

  @Test
  void updateFastTopNWithTtlOnRocksDBBackendMatchesHost() throws Exception {
    // The update-fast ranker (monotonic COUNT(*) DESC over a unique-keyed changelog) runs on its
    // row-keyed RocksDB map shape, TTL included: per-row-key clocks ride the ts column and
    // hydration expires per entry (nothing expires under the 1h retention here; the native tests
    // cover expiry, tombstones, and the ts-refresh re-persist). Both stateful operators —
    // aggregate and rank — keep TTL'd RocksDB tables; the operator harness test proves the route.
    Path input = Files.createTempDirectory("rocksdb-ttl-upfast-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        () -> {
          TableEnvironment tEnv = rocksdbEnvironment(input);
          tEnv.getConfig().set("table.exec.state.ttl", "1 h");
          return tEnv;
        },
        "SELECT k, c FROM (SELECT k, c, ROW_NUMBER() OVER (ORDER BY c DESC) AS rn"
            + " FROM (SELECT k, COUNT(*) AS c FROM t GROUP BY k)) WHERE rn <= 2");
  }

  @Test
  void proctimeDeduplicationOnRocksDBBackendMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("rocksdb-dedup-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        () -> rocksdbEnvironment(input),
        "SELECT k, v FROM (SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY PROCTIME() DESC)"
            + " AS rn FROM t) WHERE rn = 1");
  }

  @Test
  void retractingTopNOnRocksDBBackendMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("rocksdb-retopn-in");
    writeInput(input);
    // A Top-N over a GROUP BY changelog plans as the retracting ranker; both stateful operators
    // in this job keep their state in RocksDB tables.
    NativeParity.assertChangelogParity(
        () -> rocksdbEnvironment(input),
        "SELECT k, total FROM (SELECT k, total, ROW_NUMBER() OVER (ORDER BY total DESC) AS rn"
            + " FROM (SELECT k, SUM(v) AS total FROM t GROUP BY k)) WHERE rn <= 2");
  }

  @Test
  void rowtimeKeepFirstDeduplicationOnRocksDBBackendMatchesHost() throws Exception {
    // Watermark-driven keep-first: candidates and fired markers live in the RocksDB store; every
    // watermark firing is a range read merging the uncommitted write buffer with the committed
    // table, checkpointing every 50 ms so both sides of that merge are exercised.
    NativeParity.assertParity(
        FlinkRocksDBNativeStateBackendSqlHarnessTest::rocksdbRowtimeEnvironment,
        "SELECT k, v, ts FROM ("
            + "SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY rt ASC) AS rn FROM src)"
            + " WHERE rn = 1");
  }

  @Test
  void rowtimeKeepLastDeduplicationOnRocksDBBackendMatchesHost() throws Exception {
    // Rowtime keep-last rows carry the rowtime column too (nanosecond timestamps after the
    // bridge), so persisting them rides the same type-map support as keep-first.
    NativeParity.assertChangelogParity(
        FlinkRocksDBNativeStateBackendSqlHarnessTest::rocksdbRowtimeEnvironment,
        "SELECT k, v, ts FROM ("
            + "SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY rt DESC) AS rn FROM src)"
            + " WHERE rn = 1");
  }

  @Test
  void rowtimeOverAggregateOnRocksDBBackendMatchesHost() throws Exception {
    // Watermark-driven OVER: pending rows and the per-key running fold live in the RocksDB store;
    // every firing is a range read merging the write buffer with the committed table, and the
    // running sum crosses 50 ms barriers, so folds round-trip through the folds table.
    NativeParity.assertParity(
        FlinkRocksDBNativeStateBackendSqlHarnessTest::rocksdbRowtimeEnvironment,
        "SELECT k, v, ts, SUM(v) OVER (PARTITION BY k ORDER BY rt) AS s FROM src");
  }

  @Test
  void rowtimeOverAggregateWithTtlOnRocksDBBackendMatchesHost() throws Exception {
    // 1h retention — nothing expires in-test; the operator-harness test covers expiry across a
    // restore and proves the RocksDB route. This pins the end-to-end SQL result with the
    // deadlines table in the checkpoint.
    NativeParity.assertParity(
        () -> {
          org.apache.flink.table.api.TableEnvironment tEnv = rocksdbRowtimeEnvironment();
          tEnv.getConfig().set("table.exec.state.ttl", "1 h");
          return tEnv;
        },
        "SELECT k, v, ts, SUM(v) OVER (PARTITION BY k ORDER BY rt) AS s FROM src");
  }

  @Test
  void windowTopNOnRocksDBBackendMatchesHost() throws Exception {
    // Event-time window Top-N: open windows' buffers stage into the RocksDB table at each 50 ms
    // barrier, and every watermark firing merges the write buffer with a committed range scan
    // (a window buffered before a barrier and closed after it fires from the table).
    NativeParity.assertParity(
        FlinkRocksDBNativeStateBackendSqlHarnessTest::rocksdbRowtimeEnvironment,
        "SELECT k, v, window_start FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start,"
            + " window_end, k ORDER BY v DESC) AS rn FROM"
            + " TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND))) WHERE rn <= 2");
  }

  @Test
  void windowAggregateOnRocksDBBackendMatchesHost() throws Exception {
    // Event-time tumbling aggregate: the interval's touched (key, window) accumulators stage
    // into the RocksDB table at each 50 ms barrier and re-seed from it on the next touch, so a
    // window spanning barriers folds committed and uncommitted contributions; a watermark firing
    // merges the decoded windows with a committed range scan.
    NativeParity.assertParity(
        FlinkRocksDBNativeStateBackendSqlHarnessTest::rocksdbRowtimeEnvironment,
        "SELECT k, window_start, window_end, SUM(v) AS s, COUNT(*) AS c FROM"
            + " TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND))"
            + " GROUP BY k, window_start, window_end");
  }

  @Test
  void hoppingWindowAggregateOnRocksDBBackendMatchesHost() throws Exception {
    // Overlapping HOP windows: one row feeds several open windows, so the barrier's staged
    // rewrite and the firing's range scan both cover windows sharing rows.
    NativeParity.assertParity(
        FlinkRocksDBNativeStateBackendSqlHarnessTest::rocksdbRowtimeEnvironment,
        "SELECT k, window_start, window_end, SUM(v) AS s FROM"
            + " TABLE(HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '2' SECOND))"
            + " GROUP BY k, window_start, window_end");
  }

  @Test
  void sessionAggregateOnRocksDBBackendMatchesHost() throws Exception {
    // Event-time session aggregate: sessions extend and merge across 50 ms barriers — an
    // extension rewrites the same start, a merge tombstones the consumed start — and a watermark
    // firing merges the decoded sessions with a committed range scan.
    NativeParity.assertParity(
        FlinkRocksDBNativeStateBackendSqlHarnessTest::rocksdbRowtimeEnvironment,
        "SELECT k, window_start, window_end, SUM(v) AS s FROM"
            + " TABLE(SESSION(TABLE src PARTITION BY k, DESCRIPTOR(rt), INTERVAL '1' SECOND))"
            + " GROUP BY k, window_start, window_end");
  }

  @Test
  void windowJoinOnRocksDBBackendMatchesHost() throws Exception {
    // Event-time window join: both sides' rows buffer in per-side RocksDB row-buffer tables
    // across 50 ms barriers, and every watermark firing joins each side's range read (write
    // buffer merged with the committed table) — a window buffered before a barrier and closed
    // after it joins from the tables.
    NativeParity.assertParity(
        FlinkRocksDBNativeStateBackendSqlHarnessTest::rocksdbRowtimeEnvironment,
        "SELECT a.k, a.v, b.v FROM "
            + "(SELECT * FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND))) a "
            + "JOIN "
            + "(SELECT * FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND))) b "
            + "ON a.k = b.k AND a.window_start = b.window_start AND a.window_end = b.window_end");
  }

  @Test
  void intervalJoinOnRocksDBBackendMatchesHost() throws Exception {
    // Event-time interval self-join: rows buffer per side in RocksDB tables across 50 ms
    // barriers, each push probes the opposite table by its equi keys (a post-barrier row joins a
    // committed row through the probe), and watermarks evict retired rows from the tables.
    NativeParity.assertParity(
        FlinkRocksDBNativeStateBackendSqlHarnessTest::rocksdbRowtimeEnvironment,
        "SELECT a.k, a.v, b.v FROM src a JOIN src b ON a.k = b.k"
            + " AND a.rt BETWEEN b.rt - INTERVAL '1' SECOND AND b.rt + INTERVAL '1' SECOND");
  }

  @Test
  void temporalJoinOnRocksDBBackendMatchesHost() throws Exception {
    // Event-time temporal join: the probe rows and the versioned build side live in RocksDB
    // tables across 50 ms barriers (the versioned view's keep-last dedup is RocksDB-backed too),
    // and a watermark firing resolves buffered probes against committed versions.
    NativeParity.assertParity(
        FlinkRocksDBNativeStateBackendSqlHarnessTest::rocksdbTemporalEnvironment,
        "SELECT o.currency, o.amount, r.rate FROM Orders o"
            + " JOIN Rates FOR SYSTEM_TIME AS OF o.rt AS r ON o.currency = r.currency");
  }

  @Test
  void temporalJoinWithTtlOnRocksDBBackendMatchesHost() throws Exception {
    // 1h retention — nothing expires in-test; the operator-harness test covers expiry across a
    // restore and proves the RocksDB route. This pins the end-to-end SQL result with the
    // deadlines table in the checkpoint.
    NativeParity.assertParity(
        () -> {
          TableEnvironment tEnv = rocksdbTemporalEnvironment();
          tEnv.getConfig().set("table.exec.state.ttl", "1 h");
          return tEnv;
        },
        "SELECT o.currency, o.amount, r.rate FROM Orders o"
            + " JOIN Rates FOR SYSTEM_TIME AS OF o.rt AS r ON o.currency = r.currency");
  }

  /** The temporal-join harness sources (orders + versioned rates) on the RocksDB backend. */
  private static TableEnvironment rocksdbTemporalEnvironment() {
    Configuration configuration = new Configuration();
    configuration.setString(
        "state.backend.type", "tech.streamfusion.state.RocksDBNativeStateBackendFactory");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    env.setParallelism(1);
    env.enableCheckpointing(50);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    WatermarkStrategy<Row> watermarks =
        WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((row, ts) -> (Long) row.getField(2));
    java.util.function.Function<String, Schema> schema =
        valueColumn ->
            Schema.newBuilder()
                .column("currency", DataTypes.STRING())
                .column(valueColumn, DataTypes.BIGINT())
                .column("ts", DataTypes.BIGINT())
                .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
                .watermark("rt", "SOURCE_WATERMARK()")
                .build();
    DataStream<Row> orders =
        env.fromData(
                Types.ROW_NAMED(
                    new String[] {"currency", "amount", "ts"},
                    Types.STRING,
                    Types.LONG,
                    Types.LONG),
                Row.of("USD", 1L, 150L),
                Row.of("EUR", 2L, 250L),
                Row.of("GBP", 4L, 260L),
                Row.of("USD", 3L, 450L))
            .assignTimestampsAndWatermarks(watermarks);
    DataStream<Row> rates =
        env.fromData(
                Types.ROW_NAMED(
                    new String[] {"currency", "rate", "ts"},
                    Types.STRING,
                    Types.LONG,
                    Types.LONG),
                Row.of("USD", 10L, 100L),
                Row.of("EUR", 99L, 100L),
                Row.of("USD", 20L, 300L))
            .assignTimestampsAndWatermarks(watermarks);
    tEnv.createTemporaryView("Orders", orders, schema.apply("amount"));
    tEnv.createTemporaryView("RatesRaw", rates, schema.apply("rate"));
    tEnv.executeSql(
        "CREATE TEMPORARY VIEW Rates AS SELECT currency, rate, rt FROM "
            + "(SELECT *, ROW_NUMBER() OVER (PARTITION BY currency ORDER BY rt DESC) AS rn "
            + " FROM RatesRaw) WHERE rn = 1");
    return tEnv;
  }

  @Test
  void unsupportedAggregatesCheckpointThroughRocksDBBackend() throws Exception {
    Path input = Files.createTempDirectory("rocksdb-minmax-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        () -> rocksdbEnvironment(input),
        "SELECT k, MIN(v) AS mn, MAX(v) AS mx, SUM(v) AS s FROM t GROUP BY k");
  }

  // Batch mode at parallelism 1 writes exactly one part file. The proctime dedup query is
  // arrival-order sensitive, and the filesystem source's read order across multiple part files is
  // not stable between the two parity runs — a streaming-mode write rolling files at checkpoints
  // made this suite flaky.
  private static void writeInput(Path directory) throws Exception {
    TableEnvironment tEnv =
        TableEnvironment.create(
            org.apache.flink.table.api.EnvironmentSettings.inBatchMode());
    tEnv.getConfig().set("parallelism.default", "1");
    tEnv.executeSql(
        "CREATE TABLE in_write (k BIGINT, v BIGINT) WITH ('connector' = 'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    tEnv.executeSql(
            "INSERT INTO in_write VALUES (1, 10), (1, 20), (2, 5), (1, 30), (2, 15), (3, 7)")
        .await();
  }

  /** The rowtime dedup harness source (out-of-order rows per key) on the RocksDB backend. */
  private static TableEnvironment rocksdbRowtimeEnvironment() {
    Configuration configuration = new Configuration();
    configuration.setString(
        "state.backend.type", "tech.streamfusion.state.RocksDBNativeStateBackendFactory");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    env.setParallelism(1);
    env.enableCheckpointing(50);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
                Types.ROW_NAMED(new String[] {"k", "v", "ts"}, Types.LONG, Types.LONG, Types.LONG),
                Row.of(1L, 30L, 2000L),
                Row.of(2L, 50L, 1500L),
                Row.of(1L, 20L, 0L),
                Row.of(2L, 40L, 1000L),
                Row.of(1L, 25L, 800L))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner((row, ts) -> (Long) row.getField(2)));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return tEnv;
  }

  private static TableEnvironment rocksdbEnvironment(Path directory) {
    Configuration configuration = new Configuration();
    configuration.setString(
        "state.backend.type", "tech.streamfusion.state.RocksDBNativeStateBackendFactory");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    env.setParallelism(1);
    env.enableCheckpointing(50);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    tEnv.executeSql(
        "CREATE TABLE t (k BIGINT, v BIGINT) WITH ('connector' = 'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    return tEnv;
  }
}

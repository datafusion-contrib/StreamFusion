package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.FixedBucketRowKeyExtractor;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

/**
 * Streaming SQL inserts into Paimon append and primary-key tables through the native sink, checked
 * against twins written by the stock Paimon connector in the same MiniCluster.
 */
class PaimonSinkParityTest {

  private static final int ROWS = 300;

  private static final String[][] FIXTURE_COLUMNS = {
    {"id", "BIGINT NOT NULL"},
    {"name", "STRING"},
    {"price", "DECIMAL(10, 2)"},
    {"big", "DECIMAL(20, 4)"},
    {"ts", "TIMESTAMP(3)"},
    {"ts6", "TIMESTAMP_LTZ(6)"},
    {"dt", "DATE"},
    {"tags", "ARRAY<INT>"},
    {"attrs", "MAP<STRING, BIGINT>"},
    {"nested", "ROW<a INT, b STRING>"},
    {"flag", "BOOLEAN"},
    {"dbl", "DOUBLE"},
    {"bin", "BYTES"},
    {"pt", "STRING"}
  };

  private static final String COLUMNS =
      Stream.of(FIXTURE_COLUMNS)
          .map(column -> column[0] + " " + column[1])
          .collect(Collectors.joining(", "));

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        ", 'partition.sink-strategy' = 'PARTITION_DYNAMIC', 'clustering.columns' = 'id'"
      })
  void fixedBucketPartitionedTableMatchesTheStockTwin(String extra) throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-fixed");
    String options = "'bucket' = '2', 'bucket-key' = 'id,ts'" + extra;
    FileStoreTable nativeTable = insertFixture(warehouse, "PARTITIONED BY (pt)", options, 1, true);
    FileStoreTable stockTable = insertFixture(warehouse, "PARTITIONED BY (pt)", options, 1, false);

    assertSameTables(stockTable, nativeTable, true, ROWS);
    assertBucketsMatchPaimonsExtractor(nativeTable);
  }

  @Test
  void manyWritersInOneTaskKeepTheNativeEncoder() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-spill");
    String options = "'bucket' = '4', 'bucket-key' = 'id,ts'";
    FileStoreTable nativeTable = insertFixture(warehouse, "PARTITIONED BY (pt)", options, 1, true);
    FileStoreTable stockTable = insertFixture(warehouse, "PARTITIONED BY (pt)", options, 1, false);

    assertSameTables(stockTable, nativeTable, false, ROWS);
    assertBucketsMatchPaimonsExtractor(nativeTable);
    NativeAppendSinkWriteTest.assertNativeFiles(nativeTable);
  }

  @ParameterizedTest
  @ValueSource(strings = {"lz4", "zstd"})
  void appendSpillsArrowBatchesUnderMemoryPressure(String codec) throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-arrow-spill-sql");
    String options =
        "'bucket' = '-1', 'write-only' = 'true', 'write-max-writers-to-spill' = '0',"
            + " 'write-buffer-size' = '16 kb', 'page-size' = '4 kb', 'spill-compression' = '"
            + codec
            + "'";
    FileStoreTable stock = insertFixture(warehouse, "PARTITIONED BY (pt)", options, 1, false);
    FileStoreTable ours = insertFixture(warehouse, "PARTITIONED BY (pt)", options, 1, true);
    assertSameTables(stock, ours, true, ROWS);
    NativeAppendSinkWriteTest.assertNativeFiles(ours);
  }

  @Test
  void fixedBucketUnpartitionedTableShufflesLikeStockAcrossWriters() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-fixed-flat");
    String options = "'bucket' = '3', 'bucket-key' = 'name', 'file.compression' = 'snappy'";
    FileStoreTable nativeTable = insertFixture(warehouse, "", options, 2, true);
    FileStoreTable stockTable = insertFixture(warehouse, "", options, 2, false);

    assertSameTables(stockTable, nativeTable, true, ROWS);
    assertBucketsMatchPaimonsExtractor(nativeTable);
  }

  @Test
  void unawareHashPartitionedTableMatchesTheStockTwin() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-unaware");
    String options = "'bucket' = '-1', 'partition.sink-strategy' = 'hash'";
    FileStoreTable nativeTable = insertFixture(warehouse, "PARTITIONED BY (pt)", options, 2, true);
    FileStoreTable stockTable = insertFixture(warehouse, "PARTITIONED BY (pt)", options, 2, false);

    assertSameTables(stockTable, nativeTable, true, ROWS);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 3, 7})
  void dynamicPartitionRoutingPreservesRowsAndUnawareBuckets(int writers) throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-partition-dynamic");
    String options =
        "'bucket' = '-1', 'partition.sink-strategy' = 'PARTITION_DYNAMIC', 'sink.parallelism' = '"
            + writers
            + "'";
    FileStoreTable stock = insertFixture(warehouse, "PARTITIONED BY (pt)", options, 2, false);
    FileStoreTable ours = insertFixture(warehouse, "PARTITIONED BY (pt)", options, 2, true);
    assertEquals(ROWS, PaimonTestTables.readRows(ours, ours.rowType()).size());
    assertEquals(
        PaimonTestTables.readRows(stock, stock.rowType()),
        PaimonTestTables.readRows(ours, ours.rowType()));
    assertEquals(
        PaimonTestTables.dataFiles(stock).keySet(), PaimonTestTables.dataFiles(ours).keySet());
    // Weighted random routing changes rows per file, even between two stock executions.
    assertEquals(footerSchemasAndCodecs(stock), footerSchemasAndCodecs(ours));
  }

  private static Map<String, java.util.Set<String>> footerSchemasAndCodecs(FileStoreTable table)
      throws Exception {
    return PaimonTestTables.footers(table).entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry ->
                    entry.getValue().stream()
                        .map(footer -> footer.replaceAll("rows=\\d+", "rows"))
                        .collect(Collectors.toSet())));
  }

  @ParameterizedTest
  @ValueSource(strings = {"order", "zorder", "hilbert"})
  void sinkClusteringOptionsAreIgnoredInStreamingLikeStock(String strategy) throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-streaming-clustering");
    // Batch clustering would reject the absent column and low sample factor. Streaming skips
    // clustering before validating either option, even when its source happens to be bounded.
    String options =
        "'bucket' = '-1', 'sink.clustering.by-columns' = 'missing_column',"
            + " 'sink.clustering.strategy' = '"
            + strategy
            + "', 'sink.clustering.sort-in-cluster' = 'true',"
            + " 'sink.clustering.sample-factor' = '1'";
    FileStoreTable stock = insertFixture(warehouse, "", options, 1, false);
    FileStoreTable ours = insertFixture(warehouse, "", options, 1, true);
    assertSameTables(stock, ours, true, ROWS);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void incrementalClusteringKeepsStockStreamingWriteBehavior(boolean optimizeWrite)
      throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-incremental-clustering");
    String options =
        "'bucket' = '-1', 'clustering.columns' = 'id',"
            + " 'clustering.incremental' = 'true', 'clustering.incremental.optimize-write' = '"
            + optimizeWrite
            + "'";
    FileStoreTable stock = insertFixture(warehouse, "PARTITIONED BY (pt)", options, 1, false);
    FileStoreTable ours = insertFixture(warehouse, "PARTITIONED BY (pt)", options, 1, true);
    assertSameTables(stock, ours, true, ROWS);
  }

  @Test
  void dynamicPartitionStrategyWithoutPartitionsIsIgnoredLikeStock() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-dynamic-unpartitioned");
    String options = "'bucket' = '-1', 'partition.sink-strategy' = 'PARTITION_DYNAMIC'";
    FileStoreTable stock = insertFixture(warehouse, "", options, 1, false);
    FileStoreTable ours = insertFixture(warehouse, "", options, 1, true);
    assertSameTables(stock, ours, true, ROWS);
  }

  @ParameterizedTest
  @ValueSource(strings = {"sink.clustering.sample-factor", "sink.clustering.sort-in-cluster"})
  void streamingClusteringStillParsesTypedOptions(String option) throws Exception {
    List<String> failures = new ArrayList<>();
    for (boolean nativeSink : new boolean[] {false, true}) {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tableEnv =
          catalogEnvironment(env, Files.createTempDirectory("paimon-clustering-invalid"));
      tableEnv.executeSql(
          "CREATE TABLE invalid (id BIGINT) WITH ('bucket' = '-1', '"
              + option
              + "' = 'not-a-value')");
      tableEnv.createTemporaryView(
          "input_rows", tableEnv.fromDataStream(env.fromSequence(1, 2)).as("id"));
      if (nativeSink) NativePlanner.install(tableEnv);
      Throwable failure =
          assertThrows(
              Throwable.class,
              () -> tableEnv.executeSql("INSERT INTO invalid SELECT id FROM input_rows").await());
      while (failure.getCause() != null) failure = failure.getCause();
      failures.add(failure.getMessage());
    }
    assertEquals(failures.get(0), failures.get(1));
  }

  @Test
  void aliasedQueryColumnsBindToTheTablePositionally() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-aliased");
    FileStoreTable nativeTable = insertAliasedFixture(warehouse, true);
    FileStoreTable stockTable = insertAliasedFixture(warehouse, false);

    assertSameTables(stockTable, nativeTable, true, ROWS);
    assertEquals(
        List.of("id", "label", "nested", "pt"), nativeTable.rowType().getFieldNames());
  }

  @Test
  void sinkConstraintsMatchTheStockTwin() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-constraints");
    FileStoreTable stockTable = insertConstraintFixture(warehouse, false);
    FileStoreTable plannedTable = insertConstraintFixture(warehouse, true);

    List<String> expected = List.of("1|x  |abc", "2|too|yz");
    assertEquals(expected, PaimonTestTables.readRows(stockTable, stockTable.rowType()));
    assertEquals(expected, PaimonTestTables.readRows(plannedTable, plannedTable.rowType()));
  }

  private static FileStoreTable insertConstraintFixture(
      java.nio.file.Path warehouse, boolean installPlanner) throws Exception {
    String name = installPlanner ? "constraints_planned" : "constraints_stock";
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = catalogEnvironment(env, warehouse);
    tableEnv.getConfig().set("table.exec.sink.not-null-enforcer", "DROP");
    tableEnv.getConfig().set("table.exec.sink.type-length-enforcer", "TRIM_PAD");
    tableEnv.executeSql(
        "CREATE TABLE "
            + name
            + " (id BIGINT NOT NULL, fixed CHAR(3), limited VARCHAR(3)) WITH ('bucket' = '-1')");
    DataStream<Row> stream =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"id", "fixed", "limited"},
                Types.LONG,
                Types.STRING,
                Types.STRING),
            Row.of(null, "x", "abcdef"),
            Row.of(1L, "x", "abcdef"),
            Row.of(2L, "toolong", "yz"));
    tableEnv.createTemporaryView(
        "constraint_source",
        tableEnv.fromDataStream(
            stream,
            Schema.newBuilder()
                .column("id", "BIGINT")
                .column("fixed", "CHAR(3)")
                .column("limited", "VARCHAR(3)")
                .build()));
    PhysicalPlanScan scan = installPlanner ? NativePlanner.install(tableEnv) : null;

    tableEnv.executeSql("INSERT INTO " + name + " SELECT * FROM constraint_source").await();

    if (installPlanner) {
      assertDeclined(scan, "not-null-enforcer=DROP");
    }
    return openTable(warehouse, name);
  }

  private static FileStoreTable insertAliasedFixture(java.nio.file.Path warehouse, boolean nativeSink)
      throws Exception {
    String name = nativeSink ? "renamed_native" : "renamed_stock";
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = catalogEnvironment(env, warehouse);
    tableEnv.executeSql(
        "CREATE TABLE "
            + name
            + " (id BIGINT NOT NULL, label STRING, nested ROW<x INT, y STRING>, pt STRING)"
            + " PARTITIONED BY (pt) WITH ('bucket' = '2', 'bucket-key' = 'id')");
    DataStream<Row> stream = env.fromData(fixtureTypeInformation(), fixtureRows());
    tableEnv.createTemporaryView(
        "fixture_source", tableEnv.fromDataStream(stream, fixtureSchema()));
    PhysicalPlanScan scan = nativeSink ? NativePlanner.install(tableEnv) : null;

    tableEnv
        .executeSql(
            "INSERT INTO " + name + " SELECT id, name AS n, nested, pt AS p FROM fixture_source")
        .await();

    if (nativeSink) {
      assertAccelerated(scan);
    }
    return openTable(warehouse, name);
  }

  @Test
  void unawareTableCompactsInJobThroughTheStockRowWriter() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-compact");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(200);
    StreamTableEnvironment tableEnv = catalogEnvironment(env, warehouse);
    tableEnv.executeSql(
        "CREATE TABLE compacted (id BIGINT, name STRING, pt STRING) PARTITIONED BY (pt) WITH ("
            + "'bucket' = '-1', 'compaction.min.file-num' = '2', 'compaction.max.file-num' = '3',"
            + " 'continuous.discovery-interval' = '500 ms')");
    tableEnv.executeSql(
        "CREATE TEMPORARY TABLE ticks (id BIGINT, name STRING) WITH ('connector' = 'datagen',"
            + " 'rows-per-second' = '100', 'number-of-rows' = '600', 'fields.name.length' = '8')");
    PhysicalPlanScan scan = NativePlanner.install(tableEnv);

    tableEnv
        .executeSql("INSERT INTO compacted SELECT id, name, CAST(id % 3 AS STRING) FROM ticks")
        .await();

    assertAccelerated(scan);
    FileStoreTable table = openTable(warehouse, "compacted");
    assertEquals(600, PaimonTestTables.readRows(table, table.rowType()).size());
    List<Snapshot.CommitKind> kinds = new ArrayList<>();
    for (Iterator<Snapshot> snapshots = table.snapshotManager().snapshots(); snapshots.hasNext(); ) {
      kinds.add(snapshots.next().commitKind());
    }
    assertTrue(kinds.contains(Snapshot.CommitKind.COMPACT), kinds::toString);
    assertTrue(kinds.contains(Snapshot.CommitKind.APPEND), kinds::toString);
  }

  @Test
  void writerOptionsRefreshAtCheckpointsLikeTheStockOperator() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-refresh");
    java.nio.file.Path firstExternal = Files.createTempDirectory("paimon-sink-external-1");
    java.nio.file.Path secondExternal = Files.createTempDirectory("paimon-sink-external-2");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(200);
    StreamTableEnvironment tableEnv = catalogEnvironment(env, warehouse);
    tableEnv.executeSql(
        "CREATE TABLE relocated (id BIGINT, name STRING) WITH ('bucket' = '-1',"
            + " 'sink.writer-refresh-detectors' = 'external-paths',"
            + " 'data-file.external-paths' = '"
            + firstExternal.toUri()
            + "', 'data-file.external-paths.strategy' = 'round-robin')");
    tableEnv.executeSql(
        "CREATE TEMPORARY TABLE ticks (id BIGINT, name STRING) WITH ('connector' = 'datagen',"
            + " 'rows-per-second' = '50', 'number-of-rows' = '400', 'fields.name.length' = '8')");
    PhysicalPlanScan scan = NativePlanner.install(tableEnv);

    TableResult insert = tableEnv.executeSql("INSERT INTO relocated SELECT id, name FROM ticks");
    waitForFiles(firstExternal);
    tableEnv.executeSql(
        "ALTER TABLE relocated SET ('data-file.external-paths' = '"
            + secondExternal.toUri()
            + "')");
    insert.await();

    assertAccelerated(scan);
    FileStoreTable table = openTable(warehouse, "relocated");
    assertEquals(400, PaimonTestTables.readRows(table, table.rowType()).size());
    assertTrue(countFiles(secondExternal) >= 1, "no data file reached the refreshed external path");
  }

  private static void waitForFiles(java.nio.file.Path root) throws Exception {
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(60);
    while (countFiles(root) == 0) {
      assertTrue(System.nanoTime() < deadline, "no data file reached " + root);
      Thread.sleep(100);
    }
  }

  private static long countFiles(java.nio.file.Path root) throws Exception {
    try (Stream<java.nio.file.Path> paths = Files.walk(root)) {
      return paths.filter(Files::isRegularFile).count();
    }
  }


  private static final int CHANGELOG_ROWS = 600;
  private static final int CHANGELOG_KEYS = 150;

  @Test
  void primaryKeyPartitionedTableMatchesTheStockTwin() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-pk");
    String options = "'bucket' = '2'";
    FileStoreTable nativeTable = upsertFixture(warehouse, "pk_native", options, 1, true, 0);
    FileStoreTable stockTable = upsertFixture(warehouse, "pk_stock", options, 1, false, 0);

    assertSameTables(stockTable, nativeTable, true, mergedRows(false));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "'bucket' = '2'",
        "'bucket' = '2', 'changelog-producer' = 'input'",
        "'bucket' = '2', 'changelog-producer' = 'lookup'",
        "'bucket' = '2', 'changelog-producer' = 'full-compaction'",
        "'bucket' = '2', 'deletion-vectors.enabled' = 'true'",
        "'bucket' = '-1', 'dynamic-bucket.target-row-num' = '10'"
      })
  void writerCoordinatorRestoresPrimaryKeyFilesAcrossSqlJobs(String mode) throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-coordinated-restore");
    String options =
        mode
            + ", 'sink.writer-coordinator.enabled' = 'true',"
            + " 'sink.writer-coordinator.page-size' = '10 b'";
    for (int job = 0; job < 2; job++) {
      FileStoreTable stock = upsertFixture(warehouse, "coordinated_stock", options, 2, false, job);
      FileStoreTable ours = upsertFixture(warehouse, "coordinated_native", options, 2, true, job);
      assertEquals(
          PaimonTestTables.readRows(stock, stock.rowType()),
          PaimonTestTables.readRows(ours, ours.rowType()));
      assertEquals(
          PaimonChangelogSinkWriteTest.changelogRows(stock),
          PaimonChangelogSinkWriteTest.changelogRows(ours));
      assertEquals(
          stock.latestSnapshot().get().commitKind(), ours.latestSnapshot().get().commitKind());
    }
  }

  @Test
  void writerCoordinatorRestoresFixedBucketAppendFilesAcrossSqlJobs() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-append-coordinated-restore");
    String options =
        "'bucket' = '2', 'bucket-key' = 'id', 'sink.writer-coordinator.enabled' = 'true',"
            + " 'sink.writer-coordinator.page-size' = '10 b'";
    for (int job = 0; job < 2; job++) {
      FileStoreTable stock = insertFixture(warehouse, "", options, 2, false);
      FileStoreTable ours = insertFixture(warehouse, "", options, 2, true);
      assertSameTables(stock, ours, true, (job + 1) * ROWS);
    }
  }

  private static final AtomicBoolean COORDINATED_JOB_FAILED = new AtomicBoolean();
  private static final AtomicBoolean COORDINATED_JOB_RESTARTED = new AtomicBoolean();

  @ParameterizedTest
  @ValueSource(
      strings = {
        "append",
        "fixed-primary-key",
        "dynamic-primary-key",
        "dynamic-partition",
        "append-spill"
      })
  @Timeout(120)
  void writerCoordinatorRecoversAfterACompletedCheckpoint(String mode) throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-coordinated-checkpoint");
    List<List<String>> contents = new ArrayList<>();
    boolean primaryKey = mode.contains("primary-key");
    for (boolean nativeWriter : new boolean[] {false, true}) {
      COORDINATED_JOB_FAILED.set(false);
      COORDINATED_JOB_RESTARTED.set(false);
      Configuration configuration = new Configuration();
      configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
      configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 5);
      configuration.set(
          RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, java.time.Duration.ZERO);
      StreamExecutionEnvironment env =
          StreamExecutionEnvironment.getExecutionEnvironment(configuration);
      env.setParallelism(1);
      env.enableCheckpointing(100);
      StreamTableEnvironment tableEnv = catalogEnvironment(env, warehouse);
      String name = nativeWriter ? "recovered_native" : "recovered_stock";
      tableEnv.executeSql(
          "CREATE TABLE "
              + name
              + " (id BIGINT NOT NULL, v BIGINT"
              + (primaryKey ? ", PRIMARY KEY (id) NOT ENFORCED" : "")
              + (mode.equals("dynamic-partition") ? ", pt BIGINT) PARTITIONED BY (pt)" : ")")
              + " WITH ("
              + (mode.equals("dynamic-partition")
                  ? "'bucket' = '-1', 'partition.sink-strategy' = 'PARTITION_DYNAMIC'"
                  : mode.equals("dynamic-primary-key")
                      ? "'bucket' = '-1', 'dynamic-bucket.target-row-num' = '10'"
                      : "'bucket' = '2', 'bucket-key' = 'id'")
              + ", 'sink.parallelism' = '2', 'sink.writer-coordinator.enabled' = 'true',"
              + (mode.equals("append-spill")
                  ? " 'write-max-writers-to-spill' = '0', 'write-buffer-size' = '16 kb',"
                      + " 'page-size' = '4 kb',"
                  : "")
              + " 'sink.writer-coordinator.page-size' = '10 b')");
      DataStream<Row> rows =
          env.fromSequence(0, 999)
              .map(new FailAfterCheckpoint())
              .returns(Types.ROW_NAMED(new String[] {"id", "v"}, Types.LONG, Types.LONG));
      tableEnv.createTemporaryView(
          "recovery_source",
          tableEnv.fromDataStream(
              rows,
              Schema.newBuilder().column("id", "BIGINT NOT NULL").column("v", "BIGINT").build()));
      PhysicalPlanScan scan = nativeWriter ? NativePlanner.install(tableEnv) : null;
      tableEnv
          .executeSql(
              "INSERT INTO "
                  + name
                  + " SELECT "
                  + (primaryKey ? "MOD(id, 100)" : "id")
                  + ", v"
                  + (mode.equals("dynamic-partition") ? ", MOD(id, 5)" : "")
                  + " FROM recovery_source")
          .await();
      if (nativeWriter) {
        assertAccelerated(scan);
      }
      assertTrue(COORDINATED_JOB_FAILED.get(), "the job must fail after a completed checkpoint");
      assertTrue(COORDINATED_JOB_RESTARTED.get(), "the source must replay after recovery");
      FileStoreTable table = openTable(warehouse, name);
      contents.add(PaimonTestTables.readRows(table, table.rowType()));
    }
    assertEquals(primaryKey ? 100 : 1000, contents.get(0).size());
    assertEquals(contents.get(0), contents.get(1));
  }

  private static final class FailAfterCheckpoint extends RichMapFunction<Long, Row>
      implements CheckpointListener {
    private int seen;

    @Override
    public Row map(Long id) {
      seen++;
      if (getRuntimeContext().getTaskInfo().getAttemptNumber() > 0) {
        COORDINATED_JOB_RESTARTED.set(true);
      }
      LockSupport.parkNanos(2_000_000);
      return Row.of(id, id * 10);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      if (seen >= 100 && COORDINATED_JOB_FAILED.compareAndSet(false, true)) {
        throw new RuntimeException("intentional failure after checkpoint " + checkpointId);
      }
    }
  }

  @Test
  @Timeout(120)
  void coordinatorCommitsBufferedAppendBatches() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-spill-coordinator");
    List<List<String>> contents = new ArrayList<>();
    for (boolean nativeWriter : new boolean[] {false, true}) {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      env.enableCheckpointing(100);
      StreamTableEnvironment tableEnv = catalogEnvironment(env, warehouse);
      String name = nativeWriter ? "buffered_native" : "buffered_stock";
      tableEnv.executeSql(
          "CREATE TABLE "
              + name
              + " (id BIGINT) WITH ('bucket' = '-1', 'write-only' = 'true',"
              + " 'sink.coordinator-commit.enabled' = 'true','write-max-writers-to-spill' = '0',"
              + " 'write-buffer-size' = '16 kb', 'page-size' = '4 kb')");
      DataStream<Row> rows =
          env.fromSequence(0, Long.MAX_VALUE)
              .map(
                  value -> {
                    LockSupport.parkNanos(1_000_000);
                    return value;
                  })
              .returns(Types.LONG)
              .filter(value -> value < 1000)
              .map(value -> Row.of(value))
              .returns(Types.ROW_NAMED(new String[] {"id"}, Types.LONG));
      tableEnv.createTemporaryView(
          "buffer_source",
          tableEnv.fromDataStream(rows, Schema.newBuilder().column("id", "BIGINT").build()));
      PhysicalPlanScan scan = nativeWriter ? NativePlanner.install(tableEnv) : null;
      TableResult result =
          tableEnv.executeSql("INSERT INTO " + name + " SELECT * FROM buffer_source");
      FileStoreTable table = openTable(warehouse, name);
      // Like Paimon's coordinator SQL harness, keep the stream alive until checkpoints commit
      // the input. Released 2.0.0 does not commit a bounded stream's final partial checkpoint.
      try {
        long deadline = System.nanoTime() + java.time.Duration.ofSeconds(30).toNanos();
        while (PaimonTestTables.readRows(table, table.rowType()).size() != 1000) {
          assertTrue(
              System.nanoTime() < deadline, "all rows must be committed by a streaming checkpoint");
          Thread.sleep(50);
        }
      } finally {
        result.getJobClient().orElseThrow().cancel().get();
      }
      if (nativeWriter) {
        assertAccelerated(scan);
        NativeAppendSinkWriteTest.assertNativeFiles(table);
      }
      contents.add(PaimonTestTables.readRows(table, table.rowType()));
    }
    assertEquals(1000, contents.get(0).size());
    assertEquals(contents.get(0), contents.get(1));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "checkpointing",
        "write-only",
        "precommit-compact",
        "sink.savepoint.auto-tag",
        "execution.checkpointing.max-concurrent-checkpoints"
      })
  void coordinatorCommitKeepsPaimonsConfigurationChecks(String invalid) throws Exception {
    List<String> failures = new ArrayList<>();
    for (boolean nativeWriter : new boolean[] {false, true}) {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      if (!invalid.equals("checkpointing")) {
        env.enableCheckpointing(100);
      }
      if (invalid.equals("execution.checkpointing.max-concurrent-checkpoints")) {
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
      }
      StreamTableEnvironment tableEnv =
          catalogEnvironment(env, Files.createTempDirectory("paimon-coordinator-invalid"));
      tableEnv.executeSql(
          "CREATE TABLE rejected (id BIGINT, v BIGINT) WITH ("
              + "'bucket' = '-1', 'sink.coordinator-commit.enabled' = 'true', 'write-only' = '"
              + !invalid.equals("write-only")
              + "'"
              + (invalid.equals("precommit-compact") || invalid.equals("sink.savepoint.auto-tag")
                  ? ", '" + invalid + "' = 'true'"
                  : "")
              + ")");
      if (nativeWriter) {
        NativePlanner.install(tableEnv);
      }
      Exception failure =
          assertThrows(
              Exception.class,
              () ->
                  tableEnv.explainSql(
                      "INSERT INTO rejected SELECT CAST(1 AS BIGINT), CAST(2 AS BIGINT)",
                      ExplainDetail.JSON_EXECUTION_PLAN));
      Throwable cause = failure;
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }
      assertTrue(cause.getMessage().contains(invalid), cause::getMessage);
      failures.add(cause.getMessage());
    }
    assertEquals(failures.get(0), failures.get(1));
  }

  @ParameterizedTest
  @MethodSource("tech.streamfusion.paimon.PaimonChangelogSinkWriteTest#modes")
  void primaryKeyChangelogModesMatchStockThroughTheFlinkSink(Map<String, String> mode)
      throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-changelog");
    String options =
        "'bucket' = '2', 'sink.parallelism' = '2', "
            + mode.entrySet().stream()
                .map(e -> "'" + e.getKey() + "' = '" + e.getValue() + "'")
                .collect(Collectors.joining(", "));
    for (int run = 0; run < 2; run++) {
      FileStoreTable ours = upsertFixture(warehouse, "pk_native", options, 1, true, run);
      FileStoreTable stock = upsertFixture(warehouse, "pk_stock", options, 1, false, run);
      assertEquals(
          PaimonTestTables.readRows(stock, stock.rowType()),
          PaimonTestTables.readRows(ours, ours.rowType()));
      if (ours.coreOptions().changelogProducer()
          != org.apache.paimon.CoreOptions.ChangelogProducer.NONE) {
        List<String> expected = PaimonChangelogSinkWriteTest.changelogRows(stock);
        assertTrue(!expected.isEmpty(), "the stock sink produced changelog records");
        assertEquals(expected, PaimonChangelogSinkWriteTest.changelogRows(ours));
      }
    }
  }

  @Test
  void primaryKeyTableIgnoringDeletesAcrossWritersMatchesTheStockTwin() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-pk-ignore-delete");
    // Normalize on one upstream task so both sink writers see deterministic bucket-local order.
    String options =
        "'bucket' = '3', 'ignore-delete' = 'true', 'file.compression' = 'zstd',"
            + " 'sink.parallelism' = '2'";
    FileStoreTable nativeTable = upsertFixture(warehouse, "pk_native", options, 1, true, 0);
    FileStoreTable stockTable = upsertFixture(warehouse, "pk_stock", options, 1, false, 0);

    assertSameTables(stockTable, nativeTable, true, mergedRows(true));
  }

  static Stream<String> dynamicBucketOptions() {
    return Stream.of(
        "",
        ", 'partition.sink-strategy' = 'PARTITION_DYNAMIC', 'clustering.columns' = 'id'",
        ", 'dynamic-bucket.assigner-parallelism' = '3', 'dynamic-bucket.initial-buckets' = '2'",
        ", 'dynamic-bucket.assigner-parallelism' = '1'",
        ", 'dynamic-bucket.max-buckets' = '2'",
        ", 'ignore-delete' = 'true'",
        ", 'changelog-producer' = 'input'",
        ", 'changelog-producer' = 'lookup'",
        ", 'force-lookup' = 'true'",
        ", 'changelog-producer' = 'full-compaction', 'full-compaction.delta-commits' = '3'",
        ", 'deletion-vectors.enabled' = 'true'",
        ", 'write-only' = 'true', 'changelog-producer' = 'input'");
  }

  @ParameterizedTest
  @MethodSource("dynamicBucketOptions")
  void dynamicBucketsRestoreTheirIndexesAcrossSqlJobs(String extra) throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-dynamic-sql");
    String options =
        "'bucket' = '-1', 'sink.parallelism' = '2', "
            + "'dynamic-bucket.target-row-num' = '25'"
            + extra;
    for (int run = 0; run < 2; run++) {
      if (run == 1 && extra.contains("initial-buckets")) {
        StreamTableEnvironment catalog =
            catalogEnvironment(StreamExecutionEnvironment.getExecutionEnvironment(), warehouse);
        for (String name : List.of("pk_native", "pk_stock")) {
          catalog.executeSql(
              "ALTER TABLE "
                  + name
                  + " SET ("
                  + "'dynamic-bucket.assigner-parallelism' = '2', 'sink.parallelism' = '3')");
        }
      }
      FileStoreTable ours = upsertFixture(warehouse, "pk_native", options, 1, true, run);
      FileStoreTable stock = upsertFixture(warehouse, "pk_stock", options, 1, false, run);
      assertEquals(
          PaimonTestTables.readRows(stock, stock.rowType()),
          PaimonTestTables.readRows(ours, ours.rowType()));
      assertTrue(
          !ours.store()
              .newIndexFileHandler()
              .scan(org.apache.paimon.index.HashIndexFile.HASH_INDEX)
              .isEmpty(),
          "hash index persisted");
      if (ours.coreOptions().changelogProducer()
          != org.apache.paimon.CoreOptions.ChangelogProducer.NONE) {
        assertEquals(
            PaimonChangelogSinkWriteTest.changelogRows(stock),
            PaimonChangelogSinkWriteTest.changelogRows(ours));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        ", 'partition.sink-strategy' = 'hash'",
        ", 'partition.sink-strategy' = 'PARTITION_DYNAMIC', 'clustering.columns' = 'id'",
        ", 'ignore-delete' = 'true'",
        ", 'changelog-producer' = 'input'",
        ", 'changelog-producer' = 'lookup'",
        ", 'changelog-producer' = 'full-compaction'",
        ", 'deletion-vectors.enabled' = 'true'",
        ", 'deletion-vectors.enabled' = 'true', 'changelog-producer' = 'lookup'"
      })
  void postponeFilesAreReplayedByPaimonsSqlCompactor(String extra) throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-postpone-sql");
    String options = "'bucket' = '-2', 'sink.parallelism' = '2'" + extra;
    for (int run = 0; run < 2; run++) {
      FileStoreTable ours = upsertFixture(warehouse, "pk_native", options, 1, true, run);
      FileStoreTable stock = upsertFixture(warehouse, "pk_stock", options, 1, false, run);
      assertTrue(ours.store().newScan().plan().files().stream().anyMatch(e -> e.bucket() == -2));
      compactPostponeTables(warehouse);
      assertEquals(
          PaimonTestTables.readRows(stock, stock.rowType()),
          PaimonTestTables.readRows(ours, ours.rowType()));
      assertTrue(!PaimonTestTables.readRows(ours, ours.rowType()).isEmpty());
      if (ours.coreOptions().changelogProducer()
          != org.apache.paimon.CoreOptions.ChangelogProducer.NONE) {
        assertEquals(
            PaimonChangelogSinkWriteTest.changelogRows(stock),
            PaimonChangelogSinkWriteTest.changelogRows(ours));
      }
    }
  }

  static void compactPostponeTables(java.nio.file.Path warehouse) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRuntimeMode(org.apache.flink.api.common.RuntimeExecutionMode.BATCH);
    StreamTableEnvironment compactor = catalogEnvironment(env, warehouse);
    compactor
        .getConfig()
        .set(org.apache.flink.table.api.config.TableConfigOptions.TABLE_DML_SYNC, true);
    compactor.executeSql("CALL sys.compact(`table` => 'default.pk_native')").await();
    compactor.executeSql("CALL sys.compact(`table` => 'default.pk_stock')").await();
  }

  @Test
  void postponeCompactionPreservesChangesAcrossRolledFiles() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-postpone-rolling");
    String options =
        "'bucket' = '-2', 'sink.parallelism' = '2', "
            + "'target-file-size' = '1 kb', 'write-buffer-size' = '1 mb'";
    List<Object[]> changes = PaimonTestTables.changelog(30_000, 90);
    FileStoreTable ours =
        writePrimaryKeyFixture(warehouse, "pk_native", options, 1, true, true, true, changes);
    FileStoreTable stock =
        writePrimaryKeyFixture(warehouse, "pk_stock", options, 1, false, true, true, changes);
    assertTrue(
        ours.store().newScan().plan().files().size() > 4,
        "postpone data must span multiple files per writer");
    compactPostponeTables(warehouse);
    assertEquals(
        PaimonTestTables.readRows(stock, stock.rowType()),
        PaimonTestTables.readRows(ours, ours.rowType()));
    assertTrue(!PaimonTestTables.readRows(ours, ours.rowType()).isEmpty());
  }

  @ParameterizedTest
  @ValueSource(strings = {"-1", "-2"})
  void dynamicAndPostponeUnpartitionedSqlSinksMatchStock(String bucket) throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-unpartitioned-buckets");
    String options = "'bucket' = '" + bucket + "', 'sink.parallelism' = '2'";
    FileStoreTable ours = upsertFixture(warehouse, "pk_native", options, 1, true, 0, false);
    FileStoreTable stock = upsertFixture(warehouse, "pk_stock", options, 1, false, 0, false);
    if (bucket.equals("-2")) {
      compactPostponeTables(warehouse);
    }
    assertEquals(
        PaimonTestTables.readRows(stock, stock.rowType()),
        PaimonTestTables.readRows(ours, ours.rowType()));
    assertTrue(!PaimonTestTables.readRows(ours, ours.rowType()).isEmpty());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void explicitSinkParallelismKeepsTheUpstreamNormalizationOrdered(boolean nativeSink)
      throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-parallel-plan");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = catalogEnvironment(env, warehouse);
    createPrimaryKeyTable(tableEnv, "parallel_sink", "'bucket' = '3', 'sink.parallelism' = '2'");
    registerChangelog(env, tableEnv);
    if (nativeSink) {
      NativePlanner.install(tableEnv);
    }
    String plan =
        tableEnv.explainSql(
            "INSERT INTO parallel_sink SELECT * FROM changelog_source",
            ExplainDetail.JSON_EXECUTION_PLAN);
    String marker = "== Physical Execution Plan ==";
    JsonNode nodes =
        new ObjectMapper()
            .readTree(plan.substring(plan.indexOf(marker) + marker.length()).trim())
            .get("nodes");
    int normalizers = 0;
    int writers = 0;
    for (JsonNode node : nodes) {
      if (node.get("type").asText().contains("ChangelogNormalize")) {
        assertEquals(1, node.get("parallelism").asInt(), plan);
        normalizers++;
      }
      if (node.get("type").asText().equals("Writer : parallel_sink")) {
        assertEquals(2, node.get("parallelism").asInt(), plan);
        writers++;
      }
    }
    assertEquals(1, normalizers, plan);
    assertEquals(1, writers, plan);
    if (nativeSink) {
      assertTrue(plan.contains("NativePaimonSink"), plan);
    }
  }

  /**
   * A second job over the same table continues the sequence numbering from the committed files, and
   * the files it adds trip Paimon's compaction inside the write job, so both twins end with the
   * compacted files Paimon's own writer produced from level-0 files of identical content.
   */
  @Test
  void primaryKeyTableCompactsInJobAndContinuesSequenceNumbersAcrossJobs() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-pk-compact");
    String options = "'bucket' = '2', 'num-sorted-run.compaction-trigger' = '2'";
    upsertFixture(warehouse, "pk_native", options, 1, true, 0);
    upsertFixture(warehouse, "pk_stock", options, 1, false, 0);
    FileStoreTable nativeTable = upsertFixture(warehouse, "pk_native", options, 1, true, 1);
    FileStoreTable stockTable = upsertFixture(warehouse, "pk_stock", options, 1, false, 1);

    assertSameTables(stockTable, nativeTable, true, mergedRows(false));
    assertTrue(
        PaimonTestTables.dataFiles(nativeTable).values().stream()
            .flatMap(List::stream)
            .anyMatch(file -> file.level() > 0),
        "the second job's files were compacted in the job");
  }

  @Test
  void writeOnlyPrimaryKeyTableLeavesLevelZeroFilesForADedicatedCompaction() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-pk-write-only");
    String options =
        "'bucket' = '2', 'write-only' = 'true', 'num-sorted-run.compaction-trigger' = '2'";
    upsertFixture(warehouse, "pk_native", options, 1, true, 0);
    upsertFixture(warehouse, "pk_stock", options, 1, false, 0);
    FileStoreTable nativeTable = upsertFixture(warehouse, "pk_native", options, 1, true, 1);
    FileStoreTable stockTable = upsertFixture(warehouse, "pk_stock", options, 1, false, 1);

    assertSameTables(stockTable, nativeTable, true, mergedRows(false));
    assertTrue(
        PaimonTestTables.dataFiles(nativeTable).values().stream()
            .flatMap(List::stream)
            .allMatch(file -> file.level() == 0));
  }

  @Test
  void insertOnlyStreamIntoAPrimaryKeyTableMatchesTheStockTwin() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-pk-insert-only");
    FileStoreTable nativeTable = insertOnlyUpsertFixture(warehouse, "pk_native", true);
    FileStoreTable stockTable = insertOnlyUpsertFixture(warehouse, "pk_stock", false);

    assertSameTables(stockTable, nativeTable, true, ROWS);
  }

  @Test
  void aForcedUpsertMaterializerDeclinesThePrimaryKeySink() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-pk-materializer");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = catalogEnvironment(env, warehouse);
    tableEnv.getConfig().set("table.exec.sink.upsert-materialize", "FORCE");
    createPrimaryKeyTable(tableEnv, "pk_forced", "'bucket' = '2'");
    registerChangelog(env, tableEnv);
    PhysicalPlanScan scan = NativePlanner.install(tableEnv);

    tableEnv.explainSql("INSERT INTO pk_forced SELECT * FROM changelog_source");

    assertDeclined(scan, "upsert-materialized");
  }

  private static int mergedRows(boolean ignoreDelete) {
    Map<String, Object[]> live = new java.util.LinkedHashMap<>();
    for (Object[] row : PaimonTestTables.changelog(CHANGELOG_ROWS, CHANGELOG_KEYS)) {
      String key = row[1] + "|" + row[2];
      if (row[0] == RowKind.DELETE) {
        if (!ignoreDelete) {
          live.remove(key);
        }
      } else {
        live.put(key, row);
      }
    }
    return live.size();
  }

  /**
   * Writes the changelog fixture's rows {@code job * CHANGELOG_ROWS ..} through one streaming job.
   */
  private static FileStoreTable upsertFixture(
      java.nio.file.Path warehouse,
      String name,
      String options,
      int parallelism,
      boolean nativeSink,
      int job)
      throws Exception {
    return upsertFixture(warehouse, name, options, parallelism, nativeSink, job, true);
  }

  private static FileStoreTable upsertFixture(
      java.nio.file.Path warehouse,
      String name,
      String options,
      int parallelism,
      boolean nativeSink,
      int job,
      boolean partitioned)
      throws Exception {
    return writePrimaryKeyFixture(
        warehouse,
        name,
        options,
        parallelism,
        nativeSink,
        job == 0,
        partitioned,
        PaimonTestTables.changelog((job + 1) * CHANGELOG_ROWS, CHANGELOG_KEYS)
            .subList(job * CHANGELOG_ROWS, (job + 1) * CHANGELOG_ROWS));
  }

  static FileStoreTable writePrimaryKeyFixture(
      java.nio.file.Path warehouse,
      String name,
      String options,
      int parallelism,
      boolean nativeSink,
      boolean create,
      boolean partitioned,
      List<Object[]> changes)
      throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);
    StreamTableEnvironment tableEnv = catalogEnvironment(env, warehouse);
    if (create) {
      createPrimaryKeyTable(tableEnv, name, options, partitioned);
    }
    registerChangelog(env, tableEnv, changes);
    PhysicalPlanScan scan = nativeSink ? NativePlanner.install(tableEnv) : null;

    tableEnv.executeSql("INSERT INTO " + name + " SELECT * FROM changelog_source").await();

    if (nativeSink) {
      assertAccelerated(scan);
    }
    return openTable(warehouse, name);
  }

  private static FileStoreTable insertOnlyUpsertFixture(
      java.nio.file.Path warehouse, String name, boolean nativeSink) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = catalogEnvironment(env, warehouse);
    tableEnv.executeSql(
        "CREATE TABLE "
            + name
            + " (id BIGINT NOT NULL, name STRING, price DECIMAL(10, 2), pt STRING NOT NULL,"
            + " PRIMARY KEY (id, pt) NOT ENFORCED) PARTITIONED BY (pt) WITH ('bucket' = '2')");
    DataStream<Row> stream = env.fromData(fixtureTypeInformation(), fixtureRows());
    tableEnv.createTemporaryView(
        "fixture_source", tableEnv.fromDataStream(stream, fixtureSchema()));
    PhysicalPlanScan scan = nativeSink ? NativePlanner.install(tableEnv) : null;

    tableEnv
        .executeSql(
            "INSERT INTO "
                + name
                + " SELECT id, name, price, COALESCE(pt, '-') FROM fixture_source")
        .await();

    if (nativeSink) {
      assertAccelerated(scan);
    }
    return openTable(warehouse, name);
  }

  private static void createPrimaryKeyTable(
      StreamTableEnvironment tableEnv, String name, String options) {
    createPrimaryKeyTable(tableEnv, name, options, true);
  }

  private static void createPrimaryKeyTable(
      StreamTableEnvironment tableEnv, String name, String options, boolean partitioned) {
    tableEnv.executeSql(
        "CREATE TABLE "
            + name
            + " (id BIGINT NOT NULL, cat STRING NOT NULL, name STRING, price DECIMAL(10, 2),"
            + " ts TIMESTAMP(3), dt DATE, flag BOOLEAN, dbl DOUBLE, bin BYTES, small TINYINT,"
            + " pt STRING NOT NULL, PRIMARY KEY (id, cat, pt) NOT ENFORCED)"
            + (partitioned ? " PARTITIONED BY (pt)" : "")
            + " WITH ("
            + options
            + ")");
  }

  private static void registerChangelog(
      StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
    registerChangelog(env, tableEnv, 0);
  }

  private static void registerChangelog(
      StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, int job) {
    List<Object[]> changelog =
        PaimonTestTables.changelog((job + 1) * CHANGELOG_ROWS, CHANGELOG_KEYS)
            .subList(job * CHANGELOG_ROWS, (job + 1) * CHANGELOG_ROWS);
    registerChangelog(env, tableEnv, changelog);
  }

  private static void registerChangelog(
      StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, List<Object[]> changelog) {
    List<Row> rows = new ArrayList<>();
    for (Object[] v : changelog) {
      rows.add(
          Row.ofKind(
              (RowKind) v[0],
              v[1],
              v[2],
              v[3],
              v[4],
              v[5] == null
                  ? null
                  : LocalDateTime.ofEpochSecond(
                      Math.floorDiv((Long) v[5], 1000L),
                      (int) Math.floorMod((Long) v[5], 1000L) * 1_000_000,
                      ZoneOffset.UTC),
              v[6] == null ? null : LocalDate.ofEpochDay((Integer) v[6]),
              v[7],
              v[8],
              v[9],
              v[10],
              v[11]));
    }
    DataStream<Row> stream =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {
                  "id", "cat", "name", "price", "ts", "dt", "flag", "dbl", "bin", "small", "pt"
                },
                Types.LONG,
                Types.STRING,
                Types.STRING,
                Types.BIG_DEC,
                Types.LOCAL_DATE_TIME,
                Types.LOCAL_DATE,
                Types.BOOLEAN,
                Types.DOUBLE,
                Types.PRIMITIVE_ARRAY(Types.BYTE),
                Types.BYTE,
                Types.STRING),
            rows.toArray(new Row[0]));
    Table source =
        tableEnv.fromChangelogStream(
            stream,
            Schema.newBuilder()
                .column("id", "BIGINT NOT NULL")
                .column("cat", "STRING NOT NULL")
                .column("name", "STRING")
                .column("price", "DECIMAL(10, 2)")
                .column("ts", "TIMESTAMP(3)")
                .column("dt", "DATE")
                .column("flag", "BOOLEAN")
                .column("dbl", "DOUBLE")
                .column("bin", "BYTES")
                .column("small", "TINYINT")
                .column("pt", "STRING NOT NULL")
                .primaryKey("id", "cat", "pt")
                .build(),
            ChangelogMode.upsert());
    tableEnv.createTemporaryView("changelog_source", source);
  }

  private static final String PK_SCHEMA =
      "(id BIGINT NOT NULL, v INT, PRIMARY KEY (id) NOT ENFORCED)";

  static Stream<Arguments> declinedTables() {
    return Stream.of(
        Arguments.of(
            "(id BIGINT, v INT)",
            "'bucket' = '-1', 'spill-compression' = 'none'",
            "spill-compression"),
        Arguments.of(
            "(id BIGINT, v INT)",
            "'bucket' = '-1', 'sink.use-managed-memory-allocator' = 'true'",
            "sink.use-managed-memory-allocator"),
        Arguments.of(
            "(id BIGINT, v INT)",
            "'bucket' = '-1', 'spill-compression' = 'lzo'",
            "spill-compression"),
        Arguments.of(
            "(id BIGINT NOT NULL, v INT, pt STRING, PRIMARY KEY (id) NOT ENFORCED) PARTITIONED BY"
                + " (pt)",
            "'bucket' = '-1'",
            "bucket mode KEY_DYNAMIC"),
        Arguments.of(
            "(id BIGINT NOT NULL, v INT, pt STRING, PRIMARY KEY (id) NOT ENFORCED) PARTITIONED BY"
                + " (pt)",
            "'bucket' = '-2'",
            "cross-partition postpone updates"),
        Arguments.of(
            PK_SCHEMA,
            "'bucket' = '2', 'changelog-producer' = 'input', 'changelog-file.format' = 'avro'",
            "changelog-file.format avro"),
        Arguments.of(
            PK_SCHEMA,
            "'bucket' = '2', 'deletion-vectors.enabled' = 'true', 'pk-btree.index.columns' = 'v'",
            "primary-key indexes"),
        Arguments.of(
            PK_SCHEMA,
            "'bucket' = '2', 'deletion-vectors.enabled' = 'true', 'pk-bitmap.index.columns' = 'v'",
            "primary-key indexes"),
        Arguments.of(
            PK_SCHEMA,
            "'bucket' = '2', 'changelog-producer' = 'input', 'changelog-file.compression' ="
                + " 'brotli'",
            "compression BROTLI"),
        Arguments.of(
            PK_SCHEMA,
            "'bucket' = '2', 'merge-engine' = 'partial-update'",
            "merge-engine partial-update"),
        Arguments.of(
            PK_SCHEMA, "'bucket' = '2', 'merge-engine' = 'first-row'", "merge-engine first-row"),
        Arguments.of(PK_SCHEMA, "'bucket' = '2', 'sequence.field' = 'v'", "sequence.field"),
        Arguments.of(
            PK_SCHEMA,
            "'bucket' = '2', 'local-merge-buffer-size' = '1 mb'",
            "local-merge-buffer-size"),
        Arguments.of(
            PK_SCHEMA, "'bucket' = '2', 'data-file.thin-mode' = 'true'", "data-file.thin-mode"),
        Arguments.of(
            PK_SCHEMA,
            "'bucket' = '2', 'sink.key-only-deletes.enabled' = 'true'",
            "sink.key-only-deletes.enabled"),
        Arguments.of(
            PK_SCHEMA, "'bucket' = '2', 'precommit-compact' = 'true'", "precommit-compact"),
        Arguments.of(
            PK_SCHEMA,
            "'bucket' = '2', 'write.sequence-number-init-mode' = 'snapshot'",
            "write.sequence-number-init-mode"),
        Arguments.of(
            PK_SCHEMA,
            "'bucket' = '2', 'sink.use-managed-memory-allocator' = 'true'",
            "sink.use-managed-memory-allocator"),
        Arguments.of(
            "(id BIGINT NOT NULL, k DOUBLE NOT NULL, PRIMARY KEY (id, k) NOT ENFORCED)",
            "'bucket' = '2'",
            "DOUBLE"),
        Arguments.of(
            "(id BIGINT, v INT)", "'bucket' = '-1', 'file.format' = 'orc'", "file.format orc"),
        Arguments.of(
            "(id BIGINT, v INT)",
            "'bucket' = '-1', 'write-buffer-for-append' = 'true'",
            "write-buffer-for-append"),
        Arguments.of(
            "(id BIGINT, v INT)",
            "'bucket' = '-1', 'file-index.bloom-filter.columns' = 'v'",
            "file indexes"),
        Arguments.of(
            "(id BIGINT, v INT)",
            "'bucket' = '-1', 'row-tracking.enabled' = 'true'",
            "row tracking"),
        Arguments.of("(id BIGINT, v TIMESTAMP(9))", "'bucket' = '-1'", "INT96"),
        Arguments.of(
            "(id BIGINT, v INT)",
            "'bucket' = '-1', 'parquet.bloom.filter.enabled' = 'true'",
            "bloom"));
  }

  @ParameterizedTest
  @MethodSource("declinedTables")
  void tablesOutsideTheWhitelistFallBackWithAReason(String schema, String options, String reason)
      throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-declined");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(1000);
    StreamTableEnvironment tableEnv = catalogEnvironment(env, warehouse);
    tableEnv.executeSql("CREATE TABLE declined " + schema + " WITH (" + options + ")");
    tableEnv.executeSql(
        "CREATE TEMPORARY TABLE src (id BIGINT NOT NULL, v INT, pt STRING) WITH ('connector' ="
            + " 'datagen')");
    PhysicalPlanScan scan = NativePlanner.install(tableEnv);

    tableEnv.explainSql(
        "INSERT INTO declined SELECT * FROM (SELECT id, " + selectFor(schema) + " FROM src)");

    assertDeclined(scan, reason);
  }

  @Test
  void dynamicTableOptionsFromTheJobConfigurationShapeAdmission() throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-sink-dynamic");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = catalogEnvironment(env, warehouse);
    tableEnv.executeSql("CREATE TABLE dyn_opts (id BIGINT, v INT) WITH ('bucket' = '-1')");
    tableEnv.executeSql(
        "CREATE TEMPORARY TABLE src (id BIGINT, v INT) WITH ('connector' = 'datagen')");
    tableEnv.getConfig().set("paimon.*.*.dyn_opts.write-buffer-for-append", "true");
    PhysicalPlanScan scan = NativePlanner.install(tableEnv);

    tableEnv.explainSql("INSERT INTO dyn_opts SELECT * FROM src");

    assertDeclined(scan, "write-buffer-for-append");
  }

  private static String selectFor(String schema) {
    if (schema.contains("k DOUBLE")) {
      return "CAST(v AS DOUBLE) AS k";
    }
    if (schema.contains("TIMESTAMP(9)")) {
      return "CAST(TO_TIMESTAMP_LTZ(id, 3) AS TIMESTAMP(9)) AS v";
    }
    return schema.contains("pt STRING") ? "v, pt" : "v";
  }

  private static FileStoreTable insertFixture(
      java.nio.file.Path warehouse,
      String partitioning,
      String options,
      int parallelism,
      boolean nativeSink)
      throws Exception {
    String name = nativeSink ? "fixture_native" : "fixture_stock";
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);
    StreamTableEnvironment tableEnv = catalogEnvironment(env, warehouse);
    tableEnv.executeSql(
        "CREATE TABLE IF NOT EXISTS "
            + name
            + " ("
            + COLUMNS
            + ") "
            + partitioning
            + " WITH ("
            + options
            + ")");
    DataStream<Row> stream = env.fromData(fixtureTypeInformation(), fixtureRows());
    Table source = tableEnv.fromDataStream(stream, fixtureSchema());
    tableEnv.createTemporaryView("fixture_source", source);
    PhysicalPlanScan scan = nativeSink ? NativePlanner.install(tableEnv) : null;

    tableEnv.executeSql("INSERT INTO " + name + " SELECT * FROM fixture_source").await();

    if (nativeSink) {
      assertAccelerated(scan);
    }
    return openTable(warehouse, name);
  }

  private static StreamTableEnvironment catalogEnvironment(
      StreamExecutionEnvironment env, java.nio.file.Path warehouse) {
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    tableEnv.executeSql(
        "CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse' = '"
            + warehouse.toUri()
            + "')");
    tableEnv.executeSql("USE CATALOG paimon");
    return tableEnv;
  }

  private static FileStoreTable openTable(java.nio.file.Path warehouse, String name) {
    Path path = new Path(warehouse.resolve("default.db").resolve(name).toUri());
    return FileStoreTableFactory.create(LocalFileIO.create(), path);
  }

  private static TypeInformation<Row> fixtureTypeInformation() {
    return Types.ROW_NAMED(
        Stream.of(FIXTURE_COLUMNS).map(column -> column[0]).toArray(String[]::new),
        Types.LONG,
        Types.STRING,
        Types.BIG_DEC,
        Types.BIG_DEC,
        Types.LOCAL_DATE_TIME,
        Types.INSTANT,
        Types.LOCAL_DATE,
        Types.OBJECT_ARRAY(Types.INT),
        Types.MAP(Types.STRING, Types.LONG),
        Types.ROW_NAMED(new String[] {"a", "b"}, Types.INT, Types.STRING),
        Types.BOOLEAN,
        Types.DOUBLE,
        Types.PRIMITIVE_ARRAY(Types.BYTE),
        Types.STRING);
  }

  private static Schema fixtureSchema() {
    Schema.Builder schema = Schema.newBuilder();
    for (String[] column : FIXTURE_COLUMNS) {
      schema.column(column[0], column[1]);
    }
    return schema.build();
  }

  private static Row[] fixtureRows() {
    List<Row> rows = new ArrayList<>();
    for (Object[] v : PaimonTestTables.values(ROWS)) {
      long[] micros = (long[]) v[5];
      Object[] nested = (Object[]) v[9];
      rows.add(
          Row.of(
              v[0],
              v[1],
              v[2],
              v[3],
              v[4] == null ? null : LocalDateTime.ofEpochSecond((Long) v[4] / 1000, (int) ((Long) v[4] % 1000) * 1_000_000, ZoneOffset.UTC),
              micros == null
                  ? null
                  : Instant.ofEpochSecond(
                      Math.floorDiv(micros[0], 1000),
                      (int) Math.floorMod(micros[0], 1000) * 1_000_000 + (int) micros[1]),
              v[6] == null ? null : LocalDate.ofEpochDay((Integer) v[6]),
              v[7],
              v[8],
              nested == null ? null : Row.of(nested[0], nested[1]),
              v[10],
              v[11],
              v[12],
              v[13]));
    }
    return rows.toArray(new Row[0]);
  }

  private static void assertSameTables(
      FileStoreTable expected, FileStoreTable actual, boolean absoluteSequenceNumbers, int rows)
      throws Exception {
    RowType rowType = expected.rowType();
    List<String> expectedRows = PaimonTestTables.readRows(expected, rowType);
    assertEquals(rows, expectedRows.size());
    assertEquals(expectedRows, PaimonTestTables.readRows(actual, rowType));
    Map<String, List<DataFileMeta>> expectedFiles = PaimonTestTables.dataFiles(expected);
    Map<String, List<DataFileMeta>> actualFiles = PaimonTestTables.dataFiles(actual);
    assertEquals(expectedFiles.keySet(), actualFiles.keySet());
    RowType keyType =
        expected.primaryKeys().isEmpty() ? null : PaimonKeyValueLayout.of(expected).keyType;
    for (String destination : expectedFiles.keySet()) {
      assertEquals(
          describeAll(expectedFiles.get(destination), rowType, keyType, absoluteSequenceNumbers),
          describeAll(actualFiles.get(destination), rowType, keyType, absoluteSequenceNumbers),
          destination);
    }
    assertEquals(PaimonTestTables.footers(expected), PaimonTestTables.footers(actual));
  }

  private static List<String> describeAll(
      List<DataFileMeta> files, RowType rowType, RowType keyType, boolean absoluteSequenceNumbers) {
    return files.stream()
        .map(
            file ->
                (keyType == null ? "" : PaimonTestTables.describeKeys(file, keyType) + "\n")
                    + "rows="
                    + file.rowCount()
                    + " seq="
                    + (absoluteSequenceNumbers ? file.minSequenceNumber() + ".." : "span ")
                    + (file.maxSequenceNumber() - (absoluteSequenceNumbers ? 0 : file.minSequenceNumber()))
                    + " level="
                    + file.level()
                    + " schema="
                    + file.schemaId()
                    + " format="
                    + file.fileFormat()
                    + " deletes="
                    + file.deleteRowCount()
                    + "\n"
                    + PaimonTestTables.describe(file, rowType))
        .collect(Collectors.toList());
  }

  /** Every row read from a (partition, bucket) split routes there under Paimon's own extractor. */
  private static void assertBucketsMatchPaimonsExtractor(FileStoreTable table) throws Exception {
    FixedBucketRowKeyExtractor extractor = new FixedBucketRowKeyExtractor(table.schema());
    RowType rowType = table.rowType();
    int checked = 0;
    for (Split split : table.newReadBuilder().newScan().plan().splits()) {
      DataSplit dataSplit = (DataSplit) split;
      List<InternalRow> rows = new ArrayList<>();
      table
          .newReadBuilder()
          .newRead()
          .createReader(dataSplit)
          .forEachRemaining(row -> rows.add(InternalRowUtils.copyInternalRow(row, rowType)));
      for (InternalRow row : rows) {
        extractor.setRecord(row);
        BinaryRow partition = extractor.partition();
        assertEquals(dataSplit.partition(), partition, () -> "partition of " + row);
        assertEquals(dataSplit.bucket(), extractor.bucket(), () -> "bucket of " + row);
        checked++;
      }
    }
    assertEquals(ROWS, checked);
  }

  private static void assertAccelerated(PhysicalPlanScan scan) {
    assertTrue(
        scan.substitutions() > 0,
        () -> "Paimon sink did not accelerate: " + scan.explainSummary());
  }

  private static void assertDeclined(PhysicalPlanScan scan, String reason) {
    List<String> reasons = scan.fallbackReasons();
    assertTrue(
        reasons.stream().anyMatch(r -> r.startsWith("paimon sink: ") && r.contains(reason)),
        () -> "expected a paimon sink decline mentioning '" + reason + "' in " + reasons);
  }
}

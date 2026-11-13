package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class PaimonBatchSqlTest {
  @TempDir Path directory;

  @org.junit.jupiter.api.Test
  void insertOnlyPrivilegesDoNotRequireReadingCommittedSequences() throws Exception {
    try {
      Class.forName("org.apache.paimon.privilege.PrivilegedFileStore");
    } catch (ClassNotFoundException absent) {
      org.junit.jupiter.api.Assumptions.abort(
          "File-based privileges are absent in this Paimon release");
    }
    var config = new Configuration();
    config.setString("parallelism.default", "2");
    var stock = environment(config, false);
    stock.executeSql(
        ddl("input", true, false, "parquet")
            .replace("'write-only'='true'", "'write-only'='false'"));
    stock.executeSql("INSERT INTO input VALUES (0,0,'before')").await();
    stock.executeSql("CALL sys.init_file_based_privilege('root-password')");
    String warehouse = directory.toUri().toString();
    stock.executeSql(
        "CREATE CATALOG rootcat WITH ('type'='paimon','warehouse'='"
            + warehouse
            + "','user'='root','password'='root-password')");
    stock.executeSql("USE CATALOG rootcat");
    stock.executeSql("CALL sys.create_privileged_user('writer', 'writer-password')");
    stock.executeSql("CALL sys.grant_privilege_to_user('writer', 'INSERT', 'default', 'input')");
    var nativeEnv = environment(config, true);
    nativeEnv.executeSql(
        "CREATE CATALOG writercat WITH ('type'='paimon','warehouse'='"
            + warehouse
            + "','user'='writer','password'='writer-password')");
    nativeEnv.executeSql("USE CATALOG writercat");
    String insert = "INSERT INTO input VALUES (1,1,'after')";
    assertFalse(nativeEnv.explainSql(insert).contains("NativeConnectorSink"));
    nativeEnv.executeSql(insert).await();
    assertThrows(Exception.class, () -> collect(nativeEnv.executeSql("SELECT * FROM input")));
    assertEquals(
        List.of("+I[0, 0, before]", "+I[1, 1, after]"),
        collect(stock.executeSql("SELECT * FROM input ORDER BY id")));
  }

  @ParameterizedTest
  @CsvSource({"DELETE FROM input WHERE id = 1", "UPDATE input SET v = 'updated' WHERE id = 1"})
  void rowLevelMutationsKeepFlinksRowKindSetter(String mutation) throws Exception {
    var config = new Configuration();
    config.setString("parallelism.default", "2");
    var stock = environment(config, false);
    for (String name : List.of("input", "stock_copy")) {
      stock.executeSql(ddl(name, true, false, "parquet"));
      stock.executeSql("INSERT INTO " + name + " VALUES (0,0,'a'),(1,1,'b')").await();
    }
    stock.executeSql(mutation.replace("input", "stock_copy")).await();
    var nativeEnv = environment(config, true);
    String plan = nativeEnv.explainSql(mutation);
    assertFalse(plan.contains("NativeConnectorSink"), plan);
    nativeEnv.executeSql(mutation).await();
    assertEquals(
        collect(stock.executeSql("SELECT * FROM stock_copy ORDER BY id")),
        collect(stock.executeSql("SELECT * FROM input ORDER BY id")));
  }

  @org.junit.jupiter.api.Test
  void parquetSnapshotsIgnoreUnusedOrcChangelogTimezoneRules() throws Exception {
    var config = new Configuration();
    config.setString("parallelism.default", "2");
    var stock = environment(config, false);
    stock.executeSql(
        ddl("input", true, false, "parquet")
            .replace("v STRING", "v TIMESTAMP(3)")
            .replace(
                "'write-only'='true'",
                "'write-only'='true','changelog-producer'='input',"
                    + "'changelog-file.format'='orc'"));
    var original = java.util.TimeZone.getDefault();
    try {
      java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("America/Los_Angeles"));
      String plan = environment(config, true).explainSql("SELECT * FROM input");
      assertTrue(plan.contains("NativeConnectorSource"), plan);
    } finally {
      java.util.TimeZone.setDefault(original);
    }
  }

  @ParameterizedTest
  @CsvSource({"false,-1", "true,2", "true,-1", "true,-2"})
  void inferredSourceParallelismKeepsBoundedSinkRouting(boolean primaryKey, int bucket)
      throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        bucket != -2 || tech.streamfusion.paimon.PaimonTestCompat.postponeSinks());
    var config = new Configuration();
    config.setString("parallelism.default", "2");
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 256L << 20);
      var stock = environment(config, false);
      stock.executeSql(ddl("input", false, true, "parquet"));
      stock.executeSql(
          ddl("native_copy", primaryKey, true, "parquet")
              .replace("'bucket'='" + (primaryKey ? 2 : -1) + "'", "'bucket'='" + bucket + "'")
              .replace(",'sink.parallelism'='2'", ""));
      stock.executeSql("INSERT INTO input VALUES (0,0,'a'),(1,1,'b'),(2,1,'c')").await();
      var nativeEnv = environment(config, true);
      var result = nativeEnv.executeSql("INSERT INTO native_copy SELECT * FROM input WHERE id > 0");
      result.await();
      result.getJobClient().orElseThrow().getJobExecutionResult().get();
      String job = result.getJobClient().orElseThrow().getJobID().toString();
      assertTrue(metrics.counterValue(job, "nativePaimonSourceRows") > 0);
      assertEquals(0, metrics.counterValue(job, "numInputRows", "RowDataToArrow"));
      assertEquals(
          collect(stock.executeSql("SELECT * FROM input WHERE id > 0 ORDER BY id")),
          collect(stock.executeSql("SELECT * FROM native_copy ORDER BY id")));
      assertNativeFiles(table("native_copy"));
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @ParameterizedTest
  @CsvSource({"false,parquet", "true,parquet", "false,orc", "true,orc"})
  void boundedPostponeStagingReplaysThroughStockCompaction(boolean partitioned, String format)
      throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.paimon.PaimonTestCompat.postponeSinks());
    var config = new Configuration();
    config.setString("table.dml-sync", "true");
    config.setString("parallelism.default", "2");
    config.setString("table.exec.resource.default-parallelism", "2");
    config.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 256L << 20);
      var stock = environment(config, false);
      stock.executeSql(ddl("input", false, partitioned, format));
      for (String name : List.of("stock_copy", "native_copy")) {
        stock.executeSql(
            ddl(name, true, partitioned, format)
                .replace(
                    "'bucket'='2'", "'bucket'='-2','postpone.batch-write-fixed-bucket'='false'")
                .replace("'write-only'='true'", "'write-only'='false'"));
      }
      var nativeEnv = environment(config, true);
      for (String values : List.of("(0,0,'a'),(1,1,'b')", "(0,0,'updated'),(2,1,'c')")) {
        stock.executeSql("INSERT OVERWRITE input VALUES " + values).await();
        stock.executeSql("INSERT INTO stock_copy SELECT * FROM input").await();
        var inserted = nativeEnv.executeSql("INSERT INTO native_copy SELECT * FROM input");
        inserted.await();
        inserted.getJobClient().orElseThrow().getJobExecutionResult().get();
        var nativeTable = table("native_copy");
        var staged =
            nativeTable.store().newScan().plan().files().stream()
                .filter(entry -> entry.bucket() == -2)
                .collect(java.util.stream.Collectors.toList());
        assertFalse(staged.isEmpty());
        for (var entry : staged) {
          var path =
              new org.apache.paimon.fs.Path(
                  nativeTable.store().pathFactory().bucketPath(entry.partition(), entry.bucket()),
                  entry.file().fileName());
          assertFileEncoder(nativeTable, path, entry.file(), true);
        }
        for (String name : List.of("stock_copy", "native_copy")) {
          stock
              .executeSql(
                  "CALL sys.compact('default." + name + "'" + ", ''".repeat(6) + ", 'full')")
              .await();
        }
        var expected = collect(stock.executeSql("SELECT * FROM stock_copy ORDER BY id,k"));
        assertFalse(expected.isEmpty());
        assertEquals(
            expected, collect(stock.executeSql("SELECT * FROM native_copy ORDER BY id,k")));
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @ParameterizedTest
  @CsvSource({
    "false,false,parquet",
    "true,false,parquet",
    "false,true,parquet",
    "true,true,parquet",
    "false,false,orc",
    "true,false,orc",
    "false,true,orc",
    "true,true,orc"
  })
  void boundedSourceAndCommittedSinkMatchStock(
      boolean primaryKey, boolean partitioned, String format) throws Exception {
    var config = new Configuration();
    config.setString("parallelism.default", "2");
    config.setString("taskmanager.memory.network.min", "512m");
    config.setString("taskmanager.memory.network.max", "512m");
    config.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
    config.setString("table.exec.resource.default-parallelism", "2");
    config.setString("table.optimizer.range-sort.enabled", "false");
    config.setString("streamfusion.transpose.batchRows", "31");
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 256L << 20);
      var stock = environment(config, false);
      stock.executeSql(ddl("input", primaryKey, partitioned, format));
      stock.executeSql(ddl("stock_copy", primaryKey, partitioned, format));
      stock.executeSql(ddl("native_copy", primaryKey, partitioned, format));
      var values = new ArrayList<String>();
      for (int i = 0; i < 257; i++)
        values.add(
            "("
                + i
                + ","
                + i % 7
                + ","
                + (i % 13 == 0 ? "CAST(NULL AS STRING)" : "'value-" + i + "'")
                + ")");
      stock.executeSql("INSERT INTO input VALUES " + String.join(",", values)).await();
      if (primaryKey)
        stock.executeSql("INSERT INTO input VALUES (0,0,'updated'),(256,4,'last')").await();
      String query =
          "SELECT k, COUNT(*), SUM(CAST(id AS BIGINT)) FROM input WHERE id >= 10 AND v IS NOT NULL"
              + " GROUP BY k ORDER BY k";
      var stockResult = stock.executeSql(query);
      String stockJob = stockResult.getJobClient().orElseThrow().getJobID().toString();
      var expected = collect(stockResult);
      assertEquals(0, metrics.counterValue(stockJob, "nativePaimonSourceRows"));
      var nativeEnv = environment(config, true);
      var result = nativeEnv.executeSql(query);
      String job = result.getJobClient().orElseThrow().getJobID().toString();
      assertEquals(expected, collect(result));
      assertTrue(
          metrics.recordsIn(job, "NativeAggregate") > 0, metrics.nativeOperators().toString());
      assertEquals(
          0,
          metrics.counterValue(job, "numInputRows", "RowDataToArrow"),
          metrics.nativeOperators().toString());
      long nativeRows = metrics.counterValue(job, "nativePaimonSourceRows");
      long fallbackRows = metrics.counterValue(job, "paimonSourceFallbackRows");
      assertTrue(nativeRows > 0);
      assertEquals(257, nativeRows + fallbackRows);
      if (!primaryKey) assertEquals(0, fallbackRows);
      for (String projection :
          List.of(
              "SELECT v,id FROM input ORDER BY id",
              "SELECT v,id FROM input WHERE k = 3 ORDER BY id",
              "SELECT a.id,a.v FROM input a JOIN input b ON a.id = b.id AND a.k = b.k"
                  + " WHERE a.k = 3 ORDER BY a.id")) {
        var projected = nativeEnv.executeSql(projection);
        String projectedJob = projected.getJobClient().orElseThrow().getJobID().toString();
        assertEquals(collect(stock.executeSql(projection)), collect(projected), projection);
        assertTrue(
            metrics.counterValue(projectedJob, "nativePaimonSourceRows") > 0,
            projection + metrics.nativeOperators());
      }
      String select = "SELECT id,k,v FROM input WHERE id >= 10";
      stock.executeSql("INSERT INTO stock_copy " + select).await();
      assertEncoder(table("stock_copy"), false);
      var inserted = nativeEnv.executeSql("INSERT INTO native_copy " + select);
      inserted.await();
      inserted.getJobClient().orElseThrow().getJobExecutionResult().get();
      assertEquals(
          collect(stock.executeSql("SELECT * FROM stock_copy ORDER BY id")),
          collect(stock.executeSql("SELECT * FROM native_copy ORDER BY id")));
      String writeJob = inserted.getJobClient().orElseThrow().getJobID().toString();
      assertTrue(
          metrics.recordsIn(writeJob, "native-paimon-bucket-route") > 0,
          metrics.nativeOperators().toString());
      assertEquals(0, metrics.counterValue(writeJob, "numInputRows", "RowDataToArrow"));
      assertNativeFiles(table("native_copy"));
      if (!partitioned) {
        var disabled = new Configuration(config);
        disabled.setString("streamfusion.operator.paimonSource.enabled", "false");
        var fallback = environment(disabled, true).executeSql(query);
        String fallbackJob = fallback.getJobClient().orElseThrow().getJobID().toString();
        assertEquals(expected, collect(fallback));
        assertEquals(0, metrics.counterValue(fallbackJob, "nativePaimonSourceRows"));
        assertEquals(257, metrics.counterValue(fallbackJob, "numInputRows", "RowDataToArrow"));
        if (primaryKey) {
          String historical =
              "SELECT v,id FROM input /*+ OPTIONS('scan.snapshot-id'='1') */ ORDER BY id";
          var snapshot = nativeEnv.executeSql(historical);
          String snapshotJob = snapshot.getJobClient().orElseThrow().getJobID().toString();
          assertEquals(collect(stock.executeSql(historical)), collect(snapshot));
          assertEquals(257, metrics.counterValue(snapshotJob, "nativePaimonSourceRows"));
          assertEquals(0, metrics.counterValue(snapshotJob, "numInputRows", "RowDataToArrow"));
        }
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  public static final class FailOnce extends org.apache.flink.table.functions.ScalarFunction {
    private static final java.util.concurrent.atomic.AtomicBoolean FAILED =
        new java.util.concurrent.atomic.AtomicBoolean();

    public Integer eval(Integer id) {
      if (id != null && id == 3 && FAILED.compareAndSet(false, true)) {
        throw new IllegalStateException("retry bounded Paimon producer");
      }
      return id;
    }
  }

  @ParameterizedTest
  @CsvSource({
    "deduplicate,parquet",
    "first-row,parquet",
    "partial-update,parquet",
    "aggregation,parquet",
    "deduplicate,orc",
    "first-row,orc",
    "partial-update,orc",
    "aggregation,orc"
  })
  void boundedMergeEnginesPreserveCommittedUpdates(String engine, String format) throws Exception {
    var config = new Configuration();
    config.setString("parallelism.default", "2");
    config.setString("table.exec.resource.default-parallelism", "2");
    config.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 256L << 20);
      var stock = environment(config, false);
      for (String name : List.of("stock_copy", "native_copy")) {
        String options = ",'merge-engine'='" + engine + "','num-sorted-run.compaction-trigger'='2'";
        if (engine.equals("aggregation")) options += ",'fields.v.aggregate-function'='sum'";
        stock.executeSql(
            ddl(name, true, true, format)
                .replace("v STRING", "v BIGINT")
                .replace("'write-only'='true'", "'write-only'='false'" + options));
      }
      var nativeEnv = environment(config, true);
      boolean initialCommit = true;
      for (String values :
          List.of("(0,0,10),(1,1,20)", "(0,0,3),(1,1,CAST(NULL AS BIGINT))", "(0,0,7),(2,1,5)")) {
        stock.executeSql("INSERT INTO stock_copy VALUES " + values).await();
        var result = nativeEnv.executeSql("INSERT INTO native_copy VALUES " + values);
        result.await();
        result.getJobClient().orElseThrow().getJobExecutionResult().get();
        String job = result.getJobClient().orElseThrow().getJobID().toString();
        assertTrue(metrics.recordsIn(job, "native-paimon-bucket-route") > 0);
        assertEquals(
            collect(stock.executeSql("SELECT * FROM stock_copy ORDER BY id,k")),
            collect(stock.executeSql("SELECT * FROM native_copy ORDER BY id,k")));
        // Released aggregation compactions can retain Java; the initial level-0 write is native.
        if (initialCommit || !engine.equals("aggregation")) assertNativeFiles(table("native_copy"));
        initialCommit = false;
        assertEncoder(table("stock_copy"), false);
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @ParameterizedTest
  @CsvSource({"false,parquet", "true,parquet", "false,orc", "true,orc"})
  void nestedPartitionedCopiesKeepColumnarBoundaries(boolean primaryKey, String format)
      throws Exception {
    var config = new Configuration();
    config.setString("parallelism.default", "2");
    config.setString("table.exec.resource.default-parallelism", "2");
    config.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 256L << 20);
      var stock = environment(config, false);
      for (String name : List.of("input", "native_copy")) {
        stock.executeSql(
            ddl(name, primaryKey, true, format)
                .replace("v STRING", "v ROW<a ARRAY<INT>, b MAP<STRING,DECIMAL(10,2)>>"));
      }
      stock
          .executeSql(
              "INSERT INTO input VALUES"
                  + " (1,3,ROW(ARRAY[1,CAST(NULL AS INT)],MAP['x',CAST(12.34 AS DECIMAL(10,2))])),"
                  + " (2,3,CAST(NULL AS ROW<a ARRAY<INT>, b MAP<STRING,DECIMAL(10,2)>>)),"
                  + " (3,4,ROW(ARRAY[3],MAP['y',CAST(NULL AS DECIMAL(10,2))]))")
          .await();
      var nativeEnv = environment(config, true);
      String query = "SELECT v,id FROM input WHERE k=3 ORDER BY id";
      var selected = nativeEnv.executeSql(query);
      String sourceJob = selected.getJobClient().orElseThrow().getJobID().toString();
      assertEquals(collect(stock.executeSql(query)), collect(selected));
      assertTrue(
          metrics.counterValue(sourceJob, "nativePaimonSourceRows") > 0,
          nativeEnv.explainSql(query));
      var result = nativeEnv.executeSql("INSERT INTO native_copy SELECT * FROM input");
      result.await();
      result.getJobClient().orElseThrow().getJobExecutionResult().get();
      String job = result.getJobClient().orElseThrow().getJobID().toString();
      assertEquals(
          collect(stock.executeSql("SELECT * FROM input ORDER BY id")),
          collect(stock.executeSql("SELECT * FROM native_copy ORDER BY id")));
      assertEquals(
          0,
          metrics.counterValue(job, "numInputRows", "RowDataToArrow"),
          nativeEnv.explainSql("INSERT INTO native_copy SELECT * FROM input"));
      assertNativeFiles(table("native_copy"));
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @ParameterizedTest
  @CsvSource({
    "false,parquet,-1",
    "true,parquet,-1",
    "false,orc,-1",
    "true,orc,-1",
    "false,parquet,-2",
    "true,parquet,-2",
    "false,orc,-2",
    "true,orc,-2"
  })
  void bucketModesRetainCommittedKeyAssignments(boolean partitioned, String format, int bucket)
      throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        bucket != -2 || tech.streamfusion.paimon.PaimonTestCompat.postponeSinks());
    var config = new Configuration();
    config.setString("parallelism.default", "2");
    config.setString("table.exec.resource.default-parallelism", "2");
    config.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
    config.setString("streamfusion.transpose.batchRows", "3");
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 256L << 20);
      var stock = environment(config, false);
      stock.executeSql(ddl("input", false, partitioned, format));
      for (String name : List.of("stock_copy", "native_copy")) {
        stock.executeSql(
            ddl(name, true, partitioned, format)
                .replace(
                    "'bucket'='2'",
                    "'bucket'='" + bucket + "','dynamic-bucket.target-row-num'='2'"));
      }
      var nativeEnv = environment(config, true);
      for (String values :
          List.of(
              "(0,0,'a'),(1,0,'b'),(2,1,'c'),(3,1,'d')", "(0,0,'updated'),(4,0,'e'),(5,1,'f')")) {
        stock.executeSql("INSERT OVERWRITE input VALUES " + values).await();
        stock.executeSql("INSERT INTO stock_copy SELECT * FROM input").await();
        var inserted = nativeEnv.executeSql("INSERT INTO native_copy SELECT * FROM input");
        inserted.await();
        inserted.getJobClient().orElseThrow().getJobExecutionResult().get();
        String job = inserted.getJobClient().orElseThrow().getJobID().toString();
        assertTrue(
            metrics.recordsIn(
                    job,
                    bucket == -1
                        ? "dynamic-bucket-assigner"
                        : "native-paimon-postpone-fixed-bucket-route")
                > 0);
        assertEquals(0, metrics.counterValue(job, "numInputRows", "RowDataToArrow"));
        assertEquals(
            collect(stock.executeSql("SELECT * FROM stock_copy ORDER BY id,k")),
            collect(stock.executeSql("SELECT * FROM native_copy ORDER BY id,k")));
        assertNativeFiles(table("native_copy"));
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @org.junit.jupiter.api.Test
  void commitsOnlyTheCompletedBatchAttempt() throws Exception {
    var config = new Configuration();
    config.setString("parallelism.default", "2");
    config.setString("table.exec.resource.default-parallelism", "2");
    config.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
    config.setString("taskmanager.memory.network.min", "512m");
    config.setString("taskmanager.memory.network.max", "512m");
    config.setString("streamfusion.transpose.batchRows", "2");
    config.setString("restart-strategy.type", "fixed-delay");
    config.setString("restart-strategy.fixed-delay.attempts", "1");
    config.setString("restart-strategy.fixed-delay.delay", "1 ms");
    FailOnce.FAILED.set(false);
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 256L << 20);
      var stock = environment(config, false);
      stock.executeSql(ddl("input", false, false, "parquet"));
      stock.executeSql(ddl("copy", false, false, "parquet"));
      stock
          .executeSql(
              "INSERT INTO input VALUES (0,0,'a'),(1,1,'b'),(2,2,'c'),"
                  + "(3,3,CAST(NULL AS STRING)),(4,4,'e'),(5,5,'f')")
          .await();
      var nativeEnv = environment(config, true);
      nativeEnv.createTemporarySystemFunction("paimon_fail_once", FailOnce.class);
      var result =
          nativeEnv.executeSql("INSERT INTO copy SELECT paimon_fail_once(id),k,v FROM input");
      result.await();
      result.getJobClient().orElseThrow().getJobExecutionResult().get();
      assertTrue(FailOnce.FAILED.get());
      assertTrue(metrics.attempts("Source: NativePaimonBatchSource").contains("1"));
      assertEquals(
          collect(stock.executeSql("SELECT * FROM input ORDER BY id")),
          collect(stock.executeSql("SELECT * FROM copy ORDER BY id")));
      assertNativeFiles(table("copy"));
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  private TableEnvironment environment(Configuration config, boolean nativeExecution)
      throws Exception {
    var options = new Configuration(config);
    options.setString("streamfusion.native.enabled", Boolean.toString(nativeExecution));
    var env = NativePlannerTestEnvironment.createBatch(options);
    env.executeSql(
        "CREATE CATALOG p WITH ('type'='paimon','warehouse'='" + directory.toUri() + "')");
    env.executeSql("USE CATALOG p");
    return env;
  }

  private static String ddl(String name, boolean primaryKey, boolean partitioned, String format) {
    return "CREATE TABLE "
        + name
        + " (id INT NOT NULL,k INT NOT NULL,v STRING"
        + (primaryKey ? ", PRIMARY KEY(id,k) NOT ENFORCED" : "")
        + ")"
        + (partitioned ? " PARTITIONED BY(k)" : "")
        + " WITH ('bucket'='"
        + (primaryKey ? "2" : "-1")
        + "','sink.parallelism'='2','file.format'='"
        + format
        + "','write-only'='true')";
  }

  private org.apache.paimon.table.FileStoreTable table(String name) {
    return org.apache.paimon.table.FileStoreTableFactory.create(
        org.apache.paimon.fs.local.LocalFileIO.create(),
        new org.apache.paimon.fs.Path(directory.resolve("default.db/" + name).toUri()));
  }

  private static void assertNativeFiles(org.apache.paimon.table.FileStoreTable table)
      throws Exception {
    assertEncoder(table, true);
  }

  static void assertEncoder(org.apache.paimon.table.FileStoreTable table, boolean nativeEncoder)
      throws Exception {
    int count = 0;
    for (var split : table.newReadBuilder().newScan().plan().splits()) {
      var data = (org.apache.paimon.table.source.DataSplit) split;
      for (var file : data.dataFiles()) {
        assertFileEncoder(
            table,
            new org.apache.paimon.fs.Path(data.bucketPath(), file.fileName()),
            file,
            nativeEncoder);
        count++;
      }
    }
    assertTrue(count > 0);
  }

  private static void assertFileEncoder(
      org.apache.paimon.table.FileStoreTable table,
      org.apache.paimon.fs.Path path,
      org.apache.paimon.io.DataFileMeta file,
      boolean nativeEncoder)
      throws Exception {
    if (file.fileFormat().equals("orc")) {
      try (var reader =
          org.apache.paimon.shade.org.apache.orc.OrcFile.createReader(
              new org.apache.hadoop.fs.Path(path.toUri()),
              org.apache.paimon.shade.org.apache.orc.OrcFile.readerOptions(
                  new org.apache.hadoop.conf.Configuration()))) {
        assertEquals(
            nativeEncoder,
            reader.hasMetadataValue(tech.streamfusion.orc.OrcVectorWriter.ARROW_WRITER_METADATA));
      }
      return;
    }
    try (var reader =
        tech.streamfusion.paimon.PaimonTestCompat.parquetReader(
            table.fileIO(), path, file.fileSize())) {
      assertEquals(
          nativeEncoder,
          reader.getFooter().getFileMetaData().getCreatedBy().contains("parquet-rs"));
    }
  }

  private static List<String> collect(TableResult result) throws Exception {
    var rows = new ArrayList<String>();
    try (var iterator = result.collect()) {
      while (iterator.hasNext()) rows.add(iterator.next().toString());
    }
    return rows;
  }
}

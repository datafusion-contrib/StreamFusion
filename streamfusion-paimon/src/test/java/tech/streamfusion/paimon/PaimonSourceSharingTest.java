package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.FileStoreTableFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.NativePlannerTestEnvironment;

class PaimonSourceSharingTest {
  private static final String FALLBACK_ID_EXPRESSION =
      "IF(TRY_CAST(CAST(id AS STRING) AS BOOLEAN), -id, id)";

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  void sharedWatermarksCloseEveryBranchWindow(String format) throws Exception {
    List<List<String>> twins = new ArrayList<>();
    for (boolean nativePlanner : new boolean[] {false, true}) {
      SharingCaptureFactory.reset();
      var env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      env.getConfig().setAutoWatermarkInterval(10);
      var sql =
          nativePlanner
              ? NativePlannerTestEnvironment.create(env)
              : StreamTableEnvironment.create(env);
      var warehouse = createTable(sql, format, false);
      sql.executeSql(
          "CREATE TABLE wm (id BIGINT, ts TIMESTAMP(3), WATERMARK FOR ts AS ts - INTERVAL '1'"
              + " SECOND) WITH ('bucket'='-1', 'file.format'='"
              + format
              + "', 'continuous.discovery-interval'='10 ms')");
      var table =
          FileStoreTableFactory.create(
              LocalFileIO.create(), new Path(warehouse.resolve("default.db/wm").toUri()));
      var builder = table.newStreamWriteBuilder().withCommitUser("sharing-watermarks");
      try (var writer = builder.newWrite();
          var commit = builder.newCommit()) {
        for (long i = 0; i < 3; i++) {
          writer.write(
              GenericRow.of(i, org.apache.paimon.data.Timestamp.fromEpochMillis(i * 2000)));
        }
        writer.write(GenericRow.of(10L, org.apache.paimon.data.Timestamp.fromEpochMillis(21000)));
        commit.commit(1, writer.prepareCommit(true, 1));
        var statements = sql.createStatementSet();
        for (String sink : List.of("first_window", "second_window")) {
          sql.executeSql(
              "CREATE TEMPORARY TABLE "
                  + sink
                  + " (id BIGINT, v STRING) WITH ('connector'='sharing-capture')");
          statements.addInsertSql(
              "INSERT INTO "
                  + sink
                  + " SELECT SUM(id), 'window' FROM TABLE(TUMBLE(TABLE wm,"
                  + " DESCRIPTOR(ts), INTERVAL '10' SECOND)) GROUP BY window_start, window_end");
        }
        var compiled = statements.compilePlan();
        if (nativePlanner) {
          String plan =
              compiled.explain()
                  + "\n"
                  + tech.streamfusion.planner.NativePlanner.install(sql).explainSummary();
          assertTrue(plan.contains("NativeShare(consumers=[2]"), plan);
          assertTrue(plan.contains("NativeColumnarGlobalWindowAggregate"), plan);
        }
        var job = compiled.execute().getJobClient().orElseThrow();
        try {
          SharingCaptureFactory.awaitSize(2);
          writer.write(GenericRow.of(20L, org.apache.paimon.data.Timestamp.fromEpochMillis(42000)));
          commit.commit(2, writer.prepareCommit(true, 2));
          try {
            SharingCaptureFactory.awaitSize(4);
          } catch (AssertionError failure) {
            throw new AssertionError("Watermark progress with nativePlanner=" + nativePlanner, failure);
          }
          twins.add(SharingCaptureFactory.rows());
        } finally {
          job.cancel().get(30, TimeUnit.SECONDS);
        }
      }
    }
    assertEquals(twins.get(0), twins.get(1));
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  void sharesProjectionsAcrossSinksButKeepsDifferentScansSeparate(String format) throws Exception {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    var sql = NativePlannerTestEnvironment.create(env);
    createTable(sql, format, false);
    sql.executeSql("CREATE TEMPORARY TABLE out1 (v STRING) WITH ('connector'='blackhole')");
    sql.executeSql("CREATE TEMPORARY TABLE out2 (n BIGINT) WITH ('connector'='blackhole')");
    sql.executeSql("CREATE TEMPORARY TABLE out3 (id BIGINT) WITH ('connector'='blackhole')");
    var statements = sql.createStatementSet();
    statements.addInsertSql("INSERT INTO out1 SELECT v FROM t");
    statements.addInsertSql("INSERT INTO out2 SELECT n FROM t");
    statements.addInsertSql("INSERT INTO out3 SELECT id FROM t");
    for (int attempt = 0; attempt < 2; attempt++) {
      String plan = statements.compilePlan().explain();
      assertTrue(plan.contains("NativePaimonSource"), plan);
      assertTrue(plan.contains("NativeShare(consumers=[3]"), plan);
      assertTrue(plan.contains("Reused"), plan);
      assertEquals(1, occurrences(plan, "NativePaimonSource("), plan);
    }
    for (String option :
        List.of(
            "streamfusion.plan.shareSources",
            "table.optimizer.reuse-sub-plan-enabled",
            "table.optimizer.reuse-source-enabled")) {
      sql.getConfig().set(option, "false");
      String plan = statements.compilePlan().explain();
      assertFalse(plan.contains("NativeShare"), plan);
      assertEquals(3, occurrences(plan, "NativePaimonSource("), plan);
      sql.getConfig().set(option, "true");
    }
    var withAbs = sql.createStatementSet();
    withAbs.addInsertSql("INSERT INTO out1 SELECT v FROM t");
    withAbs.addInsertSql("INSERT INTO out2 SELECT n FROM t");
    withAbs.addInsertSql("INSERT INTO out3 SELECT ABS(id) FROM t");
    String absPlan = withAbs.compilePlan().explain();
    assertTrue(absPlan.contains("NativeShare(consumers=[3]"), absPlan);
    assertEquals(1, occurrences(absPlan, "NativePaimonSource("), absPlan);

    var mixed = sql.createStatementSet();
    mixed.addInsertSql("INSERT INTO out1 SELECT v FROM t");
    mixed.addInsertSql("INSERT INTO out2 SELECT n FROM t");
    mixed.addInsertSql("INSERT INTO out3 SELECT " + FALLBACK_ID_EXPRESSION + " FROM t");
    String mixedPlan = mixed.compilePlan().explain();
    assertTrue(mixedPlan.contains("NativeShare(consumers=[2]"), mixedPlan);
    assertTrue(mixedPlan.contains("TableSourceScan"), mixedPlan);
    assertTrue(
        tech.streamfusion.planner.NativePlanner.install(sql).fallbackReasons().stream()
            .anyMatch(reason -> reason.contains("TRY_CAST")),
        mixedPlan);

    var identical = sql.createStatementSet();
    identical.addInsertSql("INSERT INTO out2 SELECT id FROM t");
    identical.addInsertSql("INSERT INTO out3 SELECT id FROM t");
    String identicalPlan = identical.compilePlan().explain();
    assertTrue(identicalPlan.contains("NativeShare(consumers=[2]"), identicalPlan);
    sql.executeSql(
        "CREATE TEMPORARY TABLE all1 (id BIGINT, v STRING, n BIGINT) WITH"
            + " ('connector'='blackhole')");
    sql.executeSql(
        "CREATE TEMPORARY TABLE all2 (id BIGINT, v STRING, n BIGINT) WITH"
            + " ('connector'='blackhole')");
    for (String different :
        List.of(
            "SELECT * FROM t WHERE id = 1",
            "SELECT * FROM t LIMIT 5",
            "SELECT * FROM t /*+ OPTIONS('scan.mode'='latest') */")) {
      var separate = sql.createStatementSet();
      separate.addInsertSql("INSERT INTO all1 " + different);
      separate.addInsertSql("INSERT INTO all2 SELECT * FROM t");
      String plan = separate.compilePlan().explain();
      assertFalse(plan.contains("NativeShare"), plan);
      assertFalse(plan.contains("Reused"), plan);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "parquet,false,false,false",
    "parquet,true,false,false",
    "orc,false,false,false",
    "orc,true,false,false",
    "parquet,true,true,false",
    "orc,true,true,false",
    "parquet,true,false,true",
    "orc,true,false,true"
  })
  void sharedSnapshotAndTailMatchFlink(
      String format, boolean primaryKey, boolean network, boolean fallback) throws Exception {
    List<List<String>> twins = new ArrayList<>();
    for (boolean nativePlanner : new boolean[] {false, true}) {
      SharingCaptureFactory.reset();
      var env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(2);
      env.enableCheckpointing(100);
      if (network) env.disableOperatorChaining();
      var sql =
          nativePlanner
              ? NativePlannerTestEnvironment.create(env)
              : StreamTableEnvironment.create(env);
      var warehouse = createTable(sql, format, primaryKey);
      for (String sink : List.of("left_sink", "right_sink", "third_sink")) {
        sql.executeSql(
            "CREATE TEMPORARY TABLE "
                + sink
                + " (id BIGINT, v STRING) WITH ('connector'='sharing-capture')");
      }
      var table =
          FileStoreTableFactory.create(
              LocalFileIO.create(), new Path(warehouse.resolve("default.db/t").toUri()));
      var builder = table.newStreamWriteBuilder().withCommitUser("sharing-test");
      try (var writer = builder.newWrite();
          var commit = builder.newCommit()) {
        for (long i = 0; i < 3; i++) writer.write(row(i, "initial", i * 10));
        commit.commit(1, writer.prepareCommit(true, 1));
        if (primaryKey) {
          for (long i = 0; i < 3; i++) writer.write(row(i, "merged", i * 10));
          commit.commit(2, writer.prepareCommit(true, 2));
        }
        var statements = sql.createStatementSet();
        statements.addInsertSql("INSERT INTO left_sink SELECT id, v FROM t");
        statements.addInsertSql("INSERT INTO right_sink SELECT n, v FROM t");
        statements.addInsertSql(
            "INSERT INTO third_sink SELECT "
                + (fallback ? FALLBACK_ID_EXPRESSION : "ABS(id)")
                + " + n, v FROM t");
        var compiled = statements.compilePlan();
        String plan = compiled.explain();
        if (nativePlanner) {
          assertTrue(plan.contains("NativeShare(consumers=[" + (fallback ? 2 : 3) + "]"), plan);
          assertEquals(1, occurrences(plan, "NativePaimonSource("), plan);
          if (fallback) {
            assertTrue(plan.contains("TableSourceScan"), plan);
            assertTrue(
                tech.streamfusion.planner.NativePlanner.install(sql).fallbackReasons().stream()
                    .anyMatch(reason -> reason.contains("TRY_CAST")),
                plan);
          }
        }
        var job = compiled.execute().getJobClient().orElseThrow();
        try {
          SharingCaptureFactory.awaitSize(9);
          for (long i = 3; i < 6; i++) writer.write(row(i, i == 4 ? null : "tail", i * 10));
          if (primaryKey) {
            GenericRow deleted = row(0, "merged", 0);
            deleted.setRowKind(org.apache.paimon.types.RowKind.DELETE);
            writer.write(deleted);
          }
          long checkpoint = primaryKey ? 3 : 2;
          commit.commit(checkpoint, writer.prepareCommit(true, checkpoint));
          SharingCaptureFactory.awaitSize(primaryKey ? 21 : 18);
          List<String> rows = SharingCaptureFactory.rows();
          assertEquals(primaryKey ? 21 : 18, rows.size());
          if (primaryKey) assertEquals(3, rows.stream().filter(r -> r.contains("-D:")).count());
          twins.add(rows);
        } finally {
          job.cancel().get(30, TimeUnit.SECONDS);
        }
      }
    }
    assertEquals(twins.get(0), twins.get(1));
  }

  private static GenericRow row(long id, String value, long n) {
    return GenericRow.of(id, value == null ? null : BinaryString.fromString(value), n);
  }

  private static java.nio.file.Path createTable(
      StreamTableEnvironment sql, String format, boolean pk) throws Exception {
    var warehouse = Files.createTempDirectory("paimon-source-sharing");
    sql.executeSql(
        "CREATE CATALOG p WITH ('type'='paimon', 'warehouse'='" + warehouse.toUri() + "')");
    sql.executeSql("USE CATALOG p");
    sql.executeSql(
        "CREATE TABLE t (id BIGINT, v STRING, n BIGINT"
            + (pk ? ", PRIMARY KEY (id) NOT ENFORCED" : "")
            + ") WITH ('bucket'='"
            + (pk ? "2" : "-1")
            + "', 'file.format'='"
            + format
            + "', 'changelog-producer'='"
            + (pk ? "input" : "none")
            + "', 'write-only'='true', 'continuous.discovery-interval'='10 ms')");
    return warehouse;
  }

  private static int occurrences(String text, String needle) {
    return (text.length() - text.replace(needle, "").length()) / needle.length();
  }
}

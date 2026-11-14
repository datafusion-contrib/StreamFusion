package tech.streamfusion.hudi;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.planner.NativePlanner;

class HudiExpandedCoverageTest {
  @TempDir Path temporary;

  private static final String FIELDS =
      "id BIGINT,p STRING,tiny INT,small INT,f FLOAT,d DOUBLE,amount DECIMAL(25,6),payload"
          + " BYTES,fixed BINARY(16),label VARCHAR(12),event_day DATE,event_clock TIME(3),ts"
          + " TIMESTAMP(6),ts_millis TIMESTAMP(3),instant TIMESTAMP_LTZ(3),instant_micros"
          + " TIMESTAMP_LTZ(6),nested ROW<txt STRING,n BIGINT,t TIMESTAMP(3)>,tags"
          + " ARRAY<STRING>,attrs MAP<STRING,BIGINT>,event_times ARRAY<TIMESTAMP(6)>,nested_attrs"
          + " MAP<STRING,ROW<n BIGINT,t TIMESTAMP_LTZ(6)>>";

  @Test
  void onlyUnobservedWatermarksKeepWholeBatchesBeforeHudi() throws Exception {
    var stock = environment();
    stock.getConfig().getConfiguration().setInteger("table.exec.resource.default-parallelism", 1);
    String source =
        "CREATE TABLE timed (id BIGINT,ts TIMESTAMP(3),WATERMARK FOR ts AS ts - INTERVAL '1'"
            + " SECOND) WITH ("
            + watermarkOptions("timed", true)
            + ")";
    stock.executeSql(source);
    stock
        .executeSql(
            "INSERT INTO timed VALUES (1,TIMESTAMP '2026-01-01 00:00:00'),"
                + "(2,TIMESTAMP '2026-01-01 00:00:10'),(3,TIMESTAMP '2026-01-01 00:00:05')")
        .await();
    var nativeSql = environment();
    nativeSql
        .getConfig()
        .getConfiguration()
        .setInteger("table.exec.resource.default-parallelism", 1);
    nativeSql.executeSql(source);
    var scan = NativePlanner.install(nativeSql);
    for (String kind : List.of("plain", "join", "observed")) {
      boolean observed = kind.equals("observed");
      String expression = observed ? "CURRENT_WATERMARK(ts)" : "ts";
      for (var sql : List.of(stock, nativeSql)) {
        String name = "result_" + kind + "_" + (sql == stock ? "stock" : "native");
        sql.executeSql(
            "CREATE TABLE "
                + name
                + " (id BIGINT,ts TIMESTAMP(3)) WITH ("
                + watermarkOptions(name, false)
                + ")");
        String select =
            kind.equals("join")
                ? "SELECT a.id,a.ts FROM timed a JOIN timed b ON a.id=b.id"
                : "SELECT id," + expression + " FROM timed";
        String insert = "INSERT INTO " + name + " " + select;
        if (sql == nativeSql) {
          String plan = sql.explainSql(insert);
          assertTrue(plan.contains("NativeHudiSource"), plan + scan.explainSummary());
          assertTrue(plan.contains("NativeHudiSink"), plan);
          assertEquals(!observed, plan.contains("watermarkGranularity=[batch]"), plan);
          if (kind.equals("join")) assertTrue(plan.contains("NativeColumnarUpdatingJoin"), plan);
        }
        sql.executeSql(insert).await();
      }
      var reader = environment();
      for (String engine : List.of("stock", "native")) {
        String name = "result_" + kind + "_" + engine;
        reader.executeSql(
            "CREATE TABLE "
                + name
                + " (id BIGINT,ts TIMESTAMP(3)) WITH ("
                + watermarkOptions(name, false)
                + ")");
      }
      assertEquals(
          read(reader, "result_" + kind + "_stock"), read(reader, "result_" + kind + "_native"));
    }
  }

  private String watermarkOptions(String name, boolean metadata) {
    return "'connector'='hudi','path'='"
        + temporary.resolve(name).toUri()
        + "','table.type'='COPY_ON_WRITE','write.operation'='insert','write.tasks'='1',"
        + "'read.tasks'='1','read.source-v2.enabled'='true','metadata.enabled'='false',"
        + "'hoodie.populate.meta.fields'='"
        + metadata
        + "','hoodie.datasource.write.recordkey.field'='id','hoodie.parquet.block.size'='1048576'";
  }

  @Test
  void terminalCommitTimeUpsertsBatchThroughRegularJoinAndAggregate() throws Exception {
    var seed = environment();
    String source =
        "CREATE TABLE timed (id BIGINT,ts TIMESTAMP(3),WATERMARK FOR ts AS ts - INTERVAL '1'"
            + " SECOND) WITH ("
            + watermarkOptions("aggregate_input", true)
            + ")";
    seed.executeSql(source);
    seed.executeSql(
            "INSERT INTO timed VALUES (1,TIMESTAMP '2026-01-01 00:00:00'),"
                + "(2,TIMESTAMP '2026-01-01 00:00:10'),(3,TIMESTAMP '2026-01-01 00:00:05')")
        .await();
    for (boolean miniBatch : List.of(false, true)) {
      for (String mode : List.of("commit", "event", "no_precombine", "user_function")) {
        var results = new ArrayList<List<String>>();
        for (boolean nativeEngine : List.of(false, true)) {
          var sql = environment();
          var config = sql.getConfig().getConfiguration();
          config.setInteger("table.exec.resource.default-parallelism", 1);
          config.setString("table.exec.mini-batch.enabled", Boolean.toString(miniBatch));
          if (miniBatch) {
            config.setString("table.exec.mini-batch.allow-latency", "1 s");
            config.setString("table.exec.mini-batch.size", "100");
          }
          sql.executeSql(source);
          sql.createTemporarySystemFunction("user_identity", StatefulIdentity.class);
          String name = "aggregate_" + mode + "_" + miniBatch + "_" + nativeEngine;
          String ddl =
              "CREATE TABLE aggregate_result (id BIGINT,total BIGINT NOT NULL,PRIMARY KEY(id) NOT"
                  + " ENFORCED) WITH ('connector'='hudi','path'='"
                  + temporary.resolve(name).toUri()
                  + "','table.type'='MERGE_ON_READ','write.operation'='upsert','write.tasks'='1',"
                  + "'read.tasks'='1','read.source-v2.enabled'='true','metadata.enabled'='false',"
                  + "'index.type'='BUCKET','hoodie.bucket.index.num.buckets'='1',"
                  + "'write.table.version'='9','precombine.field'='total','write.precombine'='"
                  + !mode.equals("no_precombine")
                  + "','hoodie.write.record.merge.mode'='"
                  + (mode.equals("event") ? "EVENT_TIME_ORDERING" : "COMMIT_TIME_ORDERING")
                  + "','hoodie.logfile.data.block.format'='parquet','compaction.async.enabled'='false',"
                  + "'compaction.schedule.enabled'='false')";
          sql.executeSql(ddl);
          var scan = nativeEngine ? NativePlanner.install(sql) : null;
          String value = "COALESCE(SUM(2-b.id),CAST(0 AS BIGINT))";
          if (mode.equals("user_function")) value = "user_identity(" + value + ")";
          String insert =
              "INSERT INTO aggregate_result SELECT a.id % 2,"
                  + value
                  + " FROM timed a"
                  + " JOIN timed b ON a.id=b.id GROUP BY a.id % 2";
          if (nativeEngine) {
            String plan = sql.explainSql(insert);
            assertTrue(plan.contains("NativeHudiSource"), plan + scan.explainSummary());
            assertTrue(plan.contains("NativeHudiSink"), plan + scan.explainSummary());
            assertTrue(plan.contains("NativeColumnarUpdatingJoin"), plan);
            assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
            assertEquals(
                mode.equals("commit"), plan.contains("watermarkGranularity=[batch]"), plan);
          }
          sql.executeSql(insert).await();
          var reader = environment();
          reader.executeSql(ddl);
          results.add(read(reader, "aggregate_result"));
        }
        assertEquals(2, results.get(0).size());
        assertEquals(results.get(0), results.get(1), mode + " miniBatch=" + miniBatch);
      }
    }
  }

  public static final class StatefulIdentity
      extends org.apache.flink.table.functions.ScalarFunction {
    private long calls;

    public Long eval(Long value) {
      calls++;
      return value;
    }
  }

  @Test
  void terminalRankAndKeepLastBatchButWatermarkKeepFirstDoesNot() throws Exception {
    var seed = environment();
    String source =
        "CREATE TABLE timed (id BIGINT,ts TIMESTAMP(3),WATERMARK FOR ts AS ts"
            + " - INTERVAL '1' SECOND) WITH ("
            + watermarkOptions("rank_input", true)
            + ")";
    seed.executeSql(source);
    seed.executeSql(
            "INSERT INTO timed VALUES (1,TIMESTAMP '2026-01-01 00:00:00'),"
                + "(2,TIMESTAMP '2026-01-01 00:00:10'),(3,TIMESTAMP '2026-01-01 00:00:05')")
        .await();
    for (boolean miniBatch : List.of(false, true)) {
      for (String ordering : List.of("id DESC", "ts DESC", "ts ASC")) {
        var results = new ArrayList<List<String>>();
        for (boolean nativeEngine : List.of(false, true)) {
          var sql = environment();
          var config = sql.getConfig().getConfiguration();
          config.setInteger("table.exec.resource.default-parallelism", 1);
          config.setString("table.exec.mini-batch.enabled", Boolean.toString(miniBatch));
          if (miniBatch) {
            config.setString("table.exec.mini-batch.allow-latency", "1 s");
            config.setString("table.exec.mini-batch.size", "100");
          }
          sql.executeSql(source);
          String name = "rank_" + ordering.replace(' ', '_') + "_" + miniBatch + "_" + nativeEngine;
          String ddl =
              "CREATE TABLE rank_result (gid BIGINT,id BIGINT,ts TIMESTAMP(3),"
                  + "PRIMARY KEY(gid) NOT ENFORCED) WITH ('connector'='hudi','path'='"
                  + temporary.resolve(name).toUri()
                  + "','table.type'='MERGE_ON_READ',"
                  + "'write.operation'='upsert','write.tasks'='1','read.tasks'='1',"
                  + "'read.source-v2.enabled'='true','metadata.enabled'='false','index.type'='BUCKET',"
                  + "'hoodie.bucket.index.num.buckets'='1','write.precombine'='true',"
                  + "'hoodie.write.record.merge.mode'='COMMIT_TIME_ORDERING',"
                  + "'hoodie.logfile.data.block.format'='parquet','compaction.async.enabled'='false',"
                  + "'compaction.schedule.enabled'='false')";
          sql.executeSql(ddl);
          var scan = nativeEngine ? NativePlanner.install(sql) : null;
          String insert =
              "INSERT INTO rank_result SELECT gid,id,ts FROM (SELECT CAST(MOD(id,2) AS BIGINT) AS"
                  + " gid,id,ts,ROW_NUMBER() OVER (PARTITION BY MOD(id,2) ORDER BY "
                  + ordering
                  + ") AS r FROM timed) WHERE r=1";
          if (nativeEngine) {
            String plan = sql.explainSql(insert);
            assertTrue(plan.contains("NativeHudiSource"), plan + scan.explainSummary());
            assertTrue(plan.contains("NativeHudiSink"), plan + scan.explainSummary());
            assertTrue(
                plan.contains(
                    ordering.startsWith("id") ? "NativeColumnarTopN" : "NativeDeduplicate"),
                plan);
            assertEquals(
                !ordering.equals("ts ASC"), plan.contains("watermarkGranularity=[batch]"), plan);
          }
          sql.executeSql(insert).await();
          var reader = environment();
          reader.executeSql(ddl);
          results.add(read(reader, "rank_result"));
        }
        assertEquals(2, results.get(0).size());
        assertEquals(results.get(0), results.get(1), ordering + " miniBatch=" + miniBatch);
      }
    }
  }

  @Test
  void fourMorWritersUseHudiPartitionRoutingAndRustLogsAcrossCommits() throws Exception {
    var sql = environment();
    String schema =
        "id BIGINT,p STRING,ts BIGINT NOT NULL,v DECIMAL(25,6),event_time TIMESTAMP(6),payload"
            + " BYTES";
    String common =
        ",'metadata.enabled'='false','write.tasks'='4','read.tasks'='4',"
            + "'read.source-v2.enabled'='true','hoodie.parquet.block.size'='1048576'";
    sql.executeSql(
        "CREATE TABLE input ("
            + schema
            + ",PRIMARY KEY(id,p) NOT ENFORCED) PARTITIONED BY(p) WITH ('connector'='hudi','path'='"
            + temporary.resolve("input").toUri()
            + "','table.type'='COPY_ON_WRITE','write.operation'='insert'"
            + common
            + ")");
    var values = new ArrayList<String>();
    for (int i = 0; i < 64; i++)
      values.add(
          "("
              + i
              + ",'p"
              + i % 4
              + "',10,CAST('1234567890123456789.123456' AS DECIMAL(25,6)),"
              + "TIMESTAMP '2026-01-02 03:04:05.123456',X'0001FF')");
    sql.executeSql("INSERT INTO input VALUES " + String.join(",", values)).await();
    sql.executeSql(
        "CREATE TABLE output ("
            + schema
            + ",PRIMARY KEY(id,p) NOT ENFORCED) PARTITIONED BY(p) WITH ('connector'='hudi','path'='"
            + temporary.resolve("output").toUri()
            + "','table.type'='MERGE_ON_READ','write.operation'='upsert',"
            + "'index.type'='BUCKET','hoodie.bucket.index.num.buckets'='4','write.precombine'='false',"
            + "'precombine.field'='ts','hoodie.logfile.data.block.format'='parquet',"
            + "'compaction.async.enabled'='false','compaction.schedule.enabled'='false'"
            + common
            + ")");
    var scan = NativePlanner.install(sql);
    String insert = "INSERT INTO output SELECT * FROM input";
    String plan = sql.explainSql(insert);
    assertTrue(plan.contains("NativeHudiSource"), plan + "\n" + scan.explainSummary());
    assertTrue(plan.contains("NativeHudiSink"), plan);
    assertFalse(plan.contains("ArrowToRow"), plan);
    sql.executeSql(insert).await();
    assertEquals(64, read(sql, "output").size(), "First MOR commit");
    String update = "INSERT INTO output SELECT id,p,ts+1,v,event_time,payload FROM input";
    assertTrue(sql.explainSql(update).contains("NativeHudiSink"));
    sql.executeSql(update).await();
    var actual = read(sql, "output");
    assertEquals(64, actual.size(), actual.toString());
    assertTrue(
        actual.stream().allMatch(row -> row.contains(", 11, 1234567890123456789.123456,")),
        actual.toString());
  }

  @Test
  void fourPartitionReadAndAppendKeepNestedAndScalarPayloadsInArrow() throws Exception {
    var sql = environment();
    sql.executeSql(ddl("seed", true));
    var values = new ArrayList<String>();
    for (int i = 0; i < 40; i++)
      values.add(
          "(CAST("
              + i
              + " AS BIGINT),'p"
              + i % 4
              + "',CAST(-7 AS INT),CAST(32000 AS INT),CAST(1.25 AS FLOAT),CAST(-2.5 AS"
              + " DOUBLE),CAST('1234567890123456789.123456' AS"
              + " DECIMAL(25,6)),X'0001FF',CAST(X'000102030405060708090A0B0C0D0E0F' AS"
              + " BINARY(16)),'finite',DATE '1969-12-31',TIME '12:34:56.123',TIMESTAMP '1969-12-31"
              + " 23:59:59.123456',TIMESTAMP '1969-12-31"
              + " 23:59:59.123',TO_TIMESTAMP_LTZ(-877,3),CAST(TIMESTAMP '1969-12-31"
              + " 23:59:59.123456' AS TIMESTAMP_LTZ(6)),ROW('nested',CAST(42 AS BIGINT),TIMESTAMP"
              + " '1969-12-31 23:59:59.123'),ARRAY['a',CAST(NULL AS STRING),'é'],MAP['k',CAST(9 AS"
              + " BIGINT)],ARRAY[TIMESTAMP '1969-12-31 23:59:59.123456',CAST(NULL AS"
              + " TIMESTAMP(6))],MAP['k',ROW(CAST(42 AS BIGINT),CAST(TIMESTAMP '1969-12-31"
              + " 23:59:59.123456' AS TIMESTAMP_LTZ(6)))])");
    var types = sql.from("seed").getResolvedSchema().getColumnDataTypes();
    values.add(
        "(40,'p0',"
            + java.util.stream.IntStream.range(2, types.size())
                .mapToObj(
                    index ->
                        "CAST(NULL AS "
                            + types.get(index).getLogicalType().copy(true).asSerializableString()
                            + ")")
                .collect(java.util.stream.Collectors.joining(","))
            + ")");
    sql.executeSql("INSERT INTO seed VALUES " + String.join(",", values)).await();
    var expected = read(sql, "seed");
    sql.executeSql(ddl("output", false));
    var scan = NativePlanner.install(sql);
    String insert = "INSERT INTO output SELECT * FROM seed";
    String plan = sql.explainSql(insert);
    assertTrue(plan.contains("NativeHudiSource"), plan + "\n" + scan.explainSummary());
    assertTrue(plan.contains("NativeHudiSink"), plan);
    assertFalse(plan.contains("ArrowToRow"), plan);
    sql.executeSql(insert).await();
    var stock = environment();
    stock.executeSql(ddl("output", false));
    assertEquals(expected, read(stock, "output"));
    sql.executeSql(ddl("filtered", false));
    String filtered =
        "INSERT INTO filtered SELECT * FROM seed WHERE p IN ('p0','p2') AND tiny=-7 AND"
            + " small=32000";
    String filteredPlan = sql.explainSql(filtered);
    assertTrue(filteredPlan.contains("NativeHudiSource"), filteredPlan + scan.explainSummary());
    assertTrue(filteredPlan.contains("NativeHudiSink"), filteredPlan);
    assertFalse(filteredPlan.contains("ArrowToRow"), filteredPlan);
    sql.executeSql(filtered).await();
    stock.executeSql(ddl("filtered", false));
    assertEquals(
        expected.stream()
            .filter(
                row ->
                    (row.contains(", p0,") || row.contains(", p2,"))
                        && row.contains(", -7, 32000,"))
            .collect(java.util.stream.Collectors.toList()),
        read(stock, "filtered"));
    try (var files = java.nio.file.Files.walk(temporary.resolve("output"))) {
      var parquet = files.filter(p -> p.toString().endsWith(".parquet")).toArray(Path[]::new);
      assertTrue(parquet.length >= 4);
      var storage =
          org.apache.hudi.storage.HoodieStorageUtils.getStorage(
              temporary.toUri().toString(),
              new org.apache.hudi.storage.hadoop.HadoopStorageConfiguration(
                  new org.apache.hadoop.conf.Configuration()));
      for (var path : parquet) {
        var footer =
            org.apache.hudi.common.util.ParquetUtils.readMetadata(
                storage, new org.apache.hudi.storage.StoragePath(path.toUri()));
        assertTrue(footer.getFileMetaData().getCreatedBy().contains("parquet-rs"));
      }
    }
  }

  private String ddl(String name, boolean metadata) {
    return "CREATE TABLE "
        + name
        + " ("
        + FIELDS
        + ",PRIMARY KEY(id,p) NOT ENFORCED) PARTITIONED BY(p) WITH ("
        + "'connector'='hudi','path'='"
        + temporary.resolve(name).toUri()
        + "','table.type'='COPY_ON_WRITE','write.operation'='insert','write.tasks'='4',"
        + "'read.tasks'='4','read.source-v2.enabled'='true','metadata.enabled'='false',"
        + "'write.insert.deduplicate'='false','hoodie.populate.meta.fields'='"
        + metadata
        + "','hoodie.parquet.block.size'='1048576','hoodie.parquet.page.size'='65536')";
  }

  private static StreamTableEnvironment environment() {
    var memory = new Configuration();
    memory.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(2048));
    var env = StreamExecutionEnvironment.createLocalEnvironment(4, memory);
    var sql = StreamTableEnvironment.create(env);
    sql.getConfig().setLocalTimeZone(java.time.ZoneId.of("UTC"));
    return sql;
  }

  private static List<String> read(StreamTableEnvironment sql, String table) throws Exception {
    var rows = new ArrayList<String>();
    try (var result = sql.executeSql("SELECT * FROM " + table).collect()) {
      while (result.hasNext()) rows.add(result.next().toString());
    }
    Collections.sort(rows);
    return rows;
  }
}

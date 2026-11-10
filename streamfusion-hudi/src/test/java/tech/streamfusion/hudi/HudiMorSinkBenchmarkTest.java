package tech.streamfusion.hudi;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.StreamerUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

class HudiMorSinkBenchmarkTest {
  @TempDir Path temporary;

  @Test
  @EnabledIfSystemProperty(named = "sf.hudi.benchmark", matches = "true")
  void completeParquetReadCalcAndMorUpsertPipeline() throws Exception {
    assertTrue(System.getProperty("java.library.path").contains("/release"), "Use -Pbench");
    int rows = Integer.getInteger("sf.hudi.benchmark.rows", 200000);
    assertTrue(rows >= 4 && rows % 2 == 0);
    int keys = rows / 2;
    var memory = new Configuration();
    memory.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(1024));
    var env = StreamExecutionEnvironment.createLocalEnvironment(1, memory);
    var sql = StreamTableEnvironment.create(env);
    sql.executeSql(
        "CREATE TABLE seed (id BIGINT,name STRING,n INT,active BOOLEAN,ts BIGINT,"
            + "PRIMARY KEY(id) NOT ENFORCED) WITH ('connector'='hudi','path'='"
            + temporary.resolve("seed").toUri()
            + "','table.type'='COPY_ON_WRITE',"
            + "'write.operation'='insert','write.tasks'='1','metadata.enabled'='false',"
            + "'hoodie.populate.meta.fields'='false','read.source-v2.enabled'='true')");
    sql.createTemporaryView(
        "seed_rows",
        env.fromSequence(0, rows - 1L)
            .map(
                index -> {
                  long id = index % keys;
                  return Row.of(
                      id,
                      id % 7 == 0 ? null : "value-" + index,
                      id % 11 == 0 ? null : (int) id,
                      id % 5 == 0 ? null : id % 2 == 0,
                      index / keys + 1);
                })
            .returns(
                Types.ROW_NAMED(
                    new String[] {"id", "name", "n", "active", "ts"},
                    Types.LONG,
                    Types.STRING,
                    Types.INT,
                    Types.BOOLEAN,
                    Types.LONG)));
    sql.executeSql("INSERT INTO seed SELECT * FROM seed_rows").await();
    tech.streamfusion.planner.NativePlanner.install(sql);
    var config = sql.getConfig().getConfiguration();
    config.setString("table.exec.mini-batch.enabled", "true");
    config.setString("table.exec.mini-batch.allow-latency", "50 ms");
    config.setString("table.exec.mini-batch.size", "4096");
    long nameLength = 0;
    for (int id = 0; id < keys; id++)
      if (id % 7 != 0) nameLength += ("value-" + (keys + id)).length();
    long multiplesOf11 = (keys - 1L) / 11;
    long sum = keys * (keys - 1L) / 2;
    List<Long> expected =
        List.of(
            (long) keys,
            (long) keys,
            sum,
            keys * 3L,
            sum - 11 * multiplesOf11 * (multiplesOf11 + 1) / 2,
            keys - (keys + 6L) / 7,
            keys - (keys + 4L) / 5,
            (keys + 1L) / 2 - (keys + 9L) / 10,
            nameLength);
    var times = List.of(new ArrayList<Double>(), new ArrayList<Double>(), new ArrayList<Double>());
    for (int iteration = -2; iteration < 5; iteration++) {
      for (int side = 0; side < 3; side++) {
        int engine = Math.floorMod(iteration + side, 3);
        Path output = temporary.resolve("output-" + iteration + "-" + engine);
        sql.executeSql(
            "CREATE TABLE target (id BIGINT,name STRING,n INT,active BOOLEAN,ts BIGINT NOT NULL,"
                + "PRIMARY KEY(id) NOT ENFORCED) WITH ('connector'='hudi','path'='"
                + output.toUri()
                + "','table.type'='MERGE_ON_READ','write.operation'='upsert','write.tasks'='1',"
                + "'metadata.enabled'='false','write.precombine'='false','ordering.fields'='ts',"
                + "'index.type'='BUCKET','hoodie.bucket.index.num.buckets'='4',"
                + "'hoodie.logfile.data.block.format'='parquet','compaction.async.enabled'='false',"
                + "'compaction.schedule.enabled'='false','write.task.max.size'='256',"
                + "'write.batch.size'='16')");
        config.setString("streamfusion.native.enabled", Boolean.toString(engine > 0));
        config.setString("streamfusion.operator.hudiSink.enabled", Boolean.toString(engine == 2));
        String query = "INSERT INTO target SELECT id,name,n,active,ts+1 FROM seed";
        String plan = sql.explainSql(query);
        assertEquals(engine == 2, plan.contains("writeMode=[MOR upsert]"), plan);
        if (engine > 0) {
          assertTrue(plan.contains("native Parquet"), plan);
          assertTrue(plan.contains("NativeCalc"), plan);
        }
        if (engine == 2) {
          assertFalse(plan.contains("RowToArrow"), plan);
          assertFalse(plan.contains("ArrowToRow"), plan);
        }
        String trial = "rows=" + rows + " iteration=" + iteration + " engine=" + engine;
        System.out.println("HUDI_MOR_UPSERT_START " + trial);
        long start = System.nanoTime();
        sql.executeSql(query).await();
        double millis = (System.nanoTime() - start) / 1_000_000.0;
        System.out.println("HUDI_MOR_UPSERT_COMMITTED " + trial + " ms=" + millis);
        config.setString("streamfusion.native.enabled", "false");
        Row last = null;
        try (var result =
            sql.executeSql(
                    "SELECT COUNT(*),COUNT(DISTINCT id),SUM(id),SUM(ts),SUM(CAST(n AS"
                        + " BIGINT)),COUNT(name),COUNT(active),SUM(CAST(CASE WHEN active THEN 1"
                        + " ELSE 0 END AS BIGINT)),SUM(CAST(CHAR_LENGTH(name) AS BIGINT)) FROM"
                        + " target")
                .collect()) {
          while (result.hasNext()) {
            Row next = result.next();
            if (next.getKind() == RowKind.INSERT || next.getKind() == RowKind.UPDATE_AFTER)
              last = next;
          }
        }
        assertNotNull(last, "Stock verification produced no row: " + trial);
        for (int field = 0; field < expected.size(); field++)
          assertEquals(expected.get(field), last.getField(field));
        verifyEncoding(output, engine == 2);
        System.out.println("HUDI_MOR_UPSERT_VERIFIED " + trial);
        sql.executeSql("DROP TABLE target");
        org.apache.flink.util.FileUtils.deleteDirectory(output.toFile());
        if (iteration >= 0) times.get(engine).add(millis);
      }
    }
    System.out.println(
        "HUDI_MOR_UPSERT_BENCH rows="
            + rows
            + " keys="
            + keys
            + " stock_ms="
            + times.get(0)
            + " previous_sf_ms="
            + times.get(1)
            + " native_ms="
            + times.get(2));
  }

  private void verifyEncoding(Path output, boolean nativeEncoder) throws Exception {
    var conf = new Configuration();
    conf.set(FlinkOptions.PATH, output.toUri().toString());
    var meta = StreamerUtil.createMetaClient(conf);
    var storage = meta.getStorage();
    var schema = new TableSchemaResolver(meta).getTableSchema();
    for (var entry : storage.listDirectEntries(new StoragePath(output.toUri()))) {
      if (!entry.getPath().getName().contains(".log.")) continue;
      try (var reader =
          HoodieLogFormat.newReader(storage, new HoodieLogFile(entry.getPath()), schema)) {
        while (reader.hasNext()) {
          var block = reader.next();
          if (block.getBlockType() != HoodieLogBlockType.PARQUET_DATA_BLOCK) continue;
          var location = block.getBlockContentLocation().get();
          byte[] bytes = new byte[Math.toIntExact(location.getBlockSize())];
          try (var input = storage.openSeekable(entry.getPath(), 64 << 10, true)) {
            input.seek(location.getContentPositionInLogFile());
            input.readFully(bytes);
          }
          Path proof = temporary.resolve("encoder-proof.parquet");
          try {
            Files.write(proof, bytes);
            var footer = ParquetUtils.readMetadata(storage, new StoragePath(proof.toUri()));
            assertEquals(
                nativeEncoder, footer.getFileMetaData().getCreatedBy().contains("parquet-rs"));
          } finally {
            Files.deleteIfExists(proof);
          }
          return;
        }
      }
    }
    fail("No committed Parquet log block");
  }
}

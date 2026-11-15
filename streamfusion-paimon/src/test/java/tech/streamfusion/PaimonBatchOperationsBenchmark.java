package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

/** Includes Java mutation, sorting, Arrow conversion, encoding, IO and final commit. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class PaimonBatchOperationsBenchmark {
  @TempDir Path directory;

  @Test
  void compareJavaOperationsWithNativeEncoding() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"), "Use -Pbench");
    int rows = Integer.getInteger("sf.paimon.operations.rows", 250_000);
    int warmups = Integer.getInteger("sf.paimon.operations.warmups", 2);
    int trials = Integer.getInteger("sf.paimon.operations.trials", 5);
    var observations = new ArrayList<Map<String, Object>>();
    var config = new Configuration();
    config.setString("parallelism.default", "2");
    config.setString("table.exec.resource.default-parallelism", "2");
    config.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
    config.setString("streamfusion.transpose.batchRows", "8192");
    config.setString("taskmanager.memory.network.min", "512m");
    config.setString("taskmanager.memory.network.max", "512m");
    try (var metrics = RecoveryExecutionMetrics.countersOnly()) {
      metrics.setAsContext(config, 2, 512L << 20);
      var engines = new ArrayList<TableEnvironment>();
      for (int mode = 0; mode < 3; mode++) {
        var options = new Configuration(config);
        options.setString("streamfusion.native.enabled", Boolean.toString(mode != 0));
        options.setString(
            "streamfusion.operator.paimonBatchEncoding.enabled", Boolean.toString(mode == 2));
        engines.add(NativePlannerTestEnvironment.createBatch(options));
      }
      create("input", false, false);
      fixture(table("input"), rows);
      for (var env : engines) env.executeSql(ddl("input", false, false));
      for (String operation :
          System.getProperty(
                  "sf.paimon.operations.cases",
                  "overwrite-append,overwrite-key,update,delete,cluster")
              .split(",")) {
        boolean key = Set.of("overwrite-key", "update", "delete").contains(operation);
        boolean cluster = operation.equals("cluster");
        for (int trial = -warmups; trial < trials; trial++) {
          for (int offset = 0; offset < 3; offset++) {
            int mode = Math.floorMod(trial + warmups + offset, 3);
            var env = engines.get(mode);
            String output =
                "result_" + operation.replace('-', '_') + "_" + (trial + warmups) + "_" + mode;
            create(output, key, cluster);
            env.executeSql(ddl(output, key, cluster));
            if (operation.equals("update") || operation.equals("delete"))
              fixture(table(output), rows);
            if (operation.startsWith("overwrite")) fixture(table(output), 1);
            String sql;
            if (operation.equals("update"))
              sql = "UPDATE " + output + " SET v = 'updated' WHERE id >= " + rows / 4;
            else if (operation.equals("delete"))
              sql = "DELETE FROM " + output + " WHERE id >= " + rows / 4;
            else
              sql =
                  "INSERT "
                      + (operation.startsWith("overwrite") ? "OVERWRITE " : "INTO ")
                      + output
                      + " SELECT * FROM input WHERE id >= "
                      + rows / 4;
            long start = System.nanoTime();
            var result = env.executeSql(sql);
            result.await();
            var client = result.getJobClient().orElseThrow();
            long runtime = client.getJobExecutionResult().get().getNetRuntime();
            long elapsed = System.nanoTime() - start;
            String job = client.getJobID().toString();
            long nativeRows = metrics.counterValue(job, "nativePaimonSourceRows");
            if (mode > 0) assertTrue(nativeRows > 0, operation + ": native scan must run");
            else assertEquals(0, nativeRows);
            int validated = validate(table(output), rows, operation);
            int nativeFiles = nativeFiles(table(output));
            assertEquals(mode == 2, nativeFiles > 0, operation + ": encoder evidence");
            var observation = new LinkedHashMap<String, Object>();
            observation.put("operation", operation);
            observation.put("engine", List.of("stock", "previous", "native").get(mode));
            observation.put("trial", trial);
            observation.put("elapsedNanos", elapsed);
            observation.put("netRuntimeMillis", runtime);
            observation.put("sourceRows", nativeRows);
            observation.put("nativeFiles", nativeFiles);
            observation.put("validatedRows", validated);
            observations.add(observation);
            System.out.println("PAIMON_OPERATIONS " + observation);
            env.executeSql("DROP TABLE " + output);
            delete(directory.resolve(output));
          }
        }
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
      var report = new LinkedHashMap<String, Object>();
      report.put("javaVersion", System.getProperty("java.version"));
      report.put("os", System.getProperty("os.name") + " " + System.getProperty("os.arch"));
      report.put("inputRows", rows);
      report.put("parallelism", 2);
      report.put("partitions", 7);
      report.put("nativeBytes", 512L << 20);
      report.put("sourceBatchRows", 8192);
      report.put("rowBatchRows", 4096);
      report.put("rowBatchMemoryThresholdBytes", 8L << 20);
      report.put("warmups", warmups);
      report.put("trials", trials);
      report.put("previous", "Same native source/compute with paimonBatchEncoding disabled");
      report.put(
          "timing", "SQL submission through successful final commit; fixture/validation excluded");
      report.put("observations", observations);
      String destination = System.getProperty("sf.paimon.operations.report");
      if (destination != null)
        new ObjectMapper()
            .writerWithDefaultPrettyPrinter()
            .writeValue(Path.of(destination).toFile(), report);
    }
  }

  private String ddl(String name, boolean key, boolean cluster) {
    return "CREATE TABLE "
        + name
        + " (id INT NOT NULL,k INT NOT NULL,v STRING"
        + (key ? ",PRIMARY KEY(id,k) NOT ENFORCED" : "")
        + ") PARTITIONED BY(k) WITH ("
        + "'connector'='paimon','path'='"
        + directory.resolve(name).toUri()
        + "','bucket'='"
        + (key ? 2 : -1)
        + "','sink.parallelism'='2','file.format'='parquet',"
        + "'file.compression'='snappy','write-only'='true'"
        + (cluster ? ",'sink.clustering.by-columns'='id'" : "")
        + ")";
  }

  private void create(String name, boolean key, boolean cluster) throws Exception {
    var fields =
        List.of(
            new org.apache.paimon.types.DataField(
                0, "id", new org.apache.paimon.types.IntType(false)),
            new org.apache.paimon.types.DataField(
                1, "k", new org.apache.paimon.types.IntType(false)),
            new org.apache.paimon.types.DataField(
                2, "v", org.apache.paimon.types.DataTypes.STRING()));
    var options =
        new HashMap<>(
            Map.of(
                "bucket",
                key ? "2" : "-1",
                "file.format",
                "parquet",
                "file.compression",
                "snappy",
                "write-only",
                "true"));
    if (cluster) options.put("sink.clustering.by-columns", "id");
    var schema =
        new org.apache.paimon.schema.Schema(
            fields, List.of("k"), key ? List.of("id", "k") : List.of(), options, null);
    tech.streamfusion.paimon.PaimonTestCompat.schemaManager(
            LocalFileIO.create(), new org.apache.paimon.fs.Path(directory.resolve(name).toUri()))
        .createTable(schema);
  }

  private FileStoreTable table(String name) {
    return FileStoreTableFactory.create(
        LocalFileIO.create(), new org.apache.paimon.fs.Path(directory.resolve(name).toUri()));
  }

  private static String payload(int id) {
    return "value-" + id % 4096 + "-abcdefghijklmnopqrstuvwxyz-ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  }

  private static void fixture(FileStoreTable table, int rows) throws Exception {
    var builder = table.newStreamWriteBuilder().withCommitUser("fixture");
    try (var writer = builder.newWrite();
        var commit = builder.newCommit()) {
      for (int id = 0; id < rows; id++)
        writer.write(
            GenericRow.of(id, id % 7, id % 17 == 0 ? null : BinaryString.fromString(payload(id))));
      commit.commit(1, writer.prepareCommit(true, 1));
    }
  }

  private static int validate(FileStoreTable table, int rows, String operation) throws Exception {
    BitSet seen = new BitSet(rows);
    var read = table.newReadBuilder();
    try (var reader = read.newRead().createReader(read.newScan().plan())) {
      org.apache.paimon.reader.RecordReader.RecordIterator<org.apache.paimon.data.InternalRow>
          batch;
      while ((batch = reader.readBatch()) != null) {
        try {
          org.apache.paimon.data.InternalRow row;
          while ((row = batch.next()) != null) {
            int id = row.getInt(0);
            assertTrue(id >= 0 && id < rows);
            assertFalse(seen.get(id), "duplicate " + id);
            seen.set(id);
            assertEquals(id % 7, row.getInt(1));
            boolean updated = operation.equals("update") && id >= rows / 4;
            String expected = updated ? "updated" : id % 17 == 0 ? null : payload(id);
            assertEquals(expected, row.isNullAt(2) ? null : row.getString(2).toString());
          }
        } finally {
          batch.releaseBatch();
        }
      }
    }
    int from = operation.equals("update") || operation.equals("delete") ? 0 : rows / 4;
    int end = operation.equals("delete") ? rows / 4 : rows;
    assertEquals(end - from, seen.cardinality());
    assertEquals(end, seen.nextClearBit(from));
    return seen.cardinality();
  }

  private static int nativeFiles(FileStoreTable table) throws Exception {
    int count = 0;
    for (var entry : table.store().newScan().plan().files()) {
      var path =
          new org.apache.paimon.fs.Path(
              table.store().pathFactory().bucketPath(entry.partition(), entry.bucket()),
              entry.file().fileName());
      try (var reader =
          tech.streamfusion.paimon.PaimonTestCompat.parquetReader(
              table.fileIO(), path, entry.file().fileSize())) {
        if (reader.getFooter().getFileMetaData().getCreatedBy().contains("parquet-rs")) count++;
      }
    }
    return count;
  }

  private static void delete(Path path) throws Exception {
    try (var files = java.nio.file.Files.walk(path)) {
      for (var file :
          files.sorted(Comparator.reverseOrder()).collect(java.util.stream.Collectors.toList()))
        java.nio.file.Files.delete(file);
    }
  }
}

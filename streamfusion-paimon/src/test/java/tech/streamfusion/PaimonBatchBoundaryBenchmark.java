package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

/**
 * Complete bounded copies, including source decode, shuffle, encoding and the successful commit.
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class PaimonBatchBoundaryBenchmark {
  @TempDir Path directory;

  @Test
  void compareBoundedConnectorBoundaries() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"), "Use -Pbench");
    int rows = Integer.getInteger("sf.paimon.batch.rows", 250_000);
    int warmups = Integer.getInteger("sf.paimon.batch.warmups", 2);
    int trials = Integer.getInteger("sf.paimon.batch.trials", 5);
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
      var engines = new TableEnvironment[3];
      for (int mode = 0; mode < engines.length; mode++) {
        var options = new Configuration(config);
        options.setString("streamfusion.native.enabled", Boolean.toString(mode != 0));
        if (mode == 1) {
          options.setString("streamfusion.operator.paimonSource.enabled", "false");
          options.setString("streamfusion.operator.paimonSink.enabled", "false");
        }
        engines[mode] = NativePlannerTestEnvironment.createBatch(options);
      }
      for (String format :
          System.getProperty("sf.paimon.batch.formats", "parquet,orc").split(",")) {
        String input = "input_" + format;
        create(input, format, false, -1);
        for (var env : engines) env.executeSql(ddl(input, format, false, -1));
        FileStoreTable inputTable = table(input);
        var builder = inputTable.newStreamWriteBuilder().withCommitUser("fixture");
        try (var writer = builder.newWrite();
            var commit = builder.newCommit()) {
          for (int id = 0; id < rows; id++)
            writer.write(
                GenericRow.of(
                    id, id % 7, id % 17 == 0 ? null : BinaryString.fromString(payload(id))));
          commit.commit(1, writer.prepareCommit(true, 1));
        }
        for (String bucketText :
            System.getProperty("sf.paimon.batch.buckets", "append,2,-1,-2").split(",")) {
          boolean primaryKey = !bucketText.equals("append");
          int bucket = primaryKey ? Integer.parseInt(bucketText) : -1;
          for (int trial = -warmups; trial < trials; trial++) {
            for (int offset = 0; offset < engines.length; offset++) {
              int mode = Math.floorMod(trial + warmups + offset, engines.length);
              var env = engines[mode];
              String output =
                  "result_"
                      + format
                      + "_"
                      + (primaryKey ? "pk" : "append")
                      + "_"
                      + (bucket + 2)
                      + "_"
                      + (trial + warmups)
                      + "_"
                      + mode;
              create(output, format, primaryKey, bucket);
              env.executeSql(ddl(output, format, primaryKey, bucket));
              long start = System.nanoTime();
              var result =
                  env.executeSql(
                      "INSERT INTO "
                          + output
                          + " SELECT * FROM "
                          + input
                          + " WHERE id >= "
                          + rows / 4);
              result.await();
              var client = result.getJobClient().orElseThrow();
              long runtime = client.getJobExecutionResult().get().getNetRuntime();
              long nanos = System.nanoTime() - start;
              String job = client.getJobID().toString();
              long nativeRows = metrics.counterValue(job, "nativePaimonSourceRows");
              long transposes = metrics.counterValue(job, "numInputRows", "RowDataToArrow");
              if (mode == 2) {
                assertTrue(nativeRows > 0);
                assertEquals(0, metrics.counterValue(job, "paimonSourceFallbackRows"));
                assertEquals(0, transposes);
                assertTrue(metrics.recordsIn(job, "native-paimon-bucket-route") > 0);
                if (!primaryKey)
                  assertTrue(metrics.recordsIn(job, "native-paimon-round-robin-route") > 0);
              } else assertEquals(0, nativeRows);
              validate(table(output), rows);
              PaimonBatchSqlTest.assertEncoder(table(output), mode == 2);
              var observation = new LinkedHashMap<String, Object>();
              observation.put("format", format);
              observation.put("tableMode", primaryKey ? "primary-key" : "append");
              observation.put("bucket", bucket);
              observation.put(
                  "engine", List.of("stock", "previous-boundaries", "native").get(mode));
              observation.put("trial", trial);
              observation.put("elapsedNanos", nanos);
              observation.put("netRuntimeMillis", runtime);
              observation.put("sourceRows", nativeRows);
              observation.put("entryTransposeRows", transposes);
              observation.put("validatedRows", rows - rows / 4);
              observation.put("nativeEncoder", mode == 2);
              observation.put(
                  "heapUsedBytes",
                  Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
              observations.add(observation);
              System.out.printf("PAIMON_BATCH %s%n", observation);
              env.executeSql("DROP TABLE " + output);
              delete(tablePath(output));
            }
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
      report.put("batchRows", 8192);
      report.put("warmups", warmups);
      report.put("trials", trials);
      report.put(
          "timing",
          "SQL submission through successful final commit; fixture and exact row validation"
              + " excluded");
      report.put("previous", "Current release with native Paimon source and sink disabled");
      report.put("observations", observations);
      new ObjectMapper()
          .writerWithDefaultPrettyPrinter()
          .writeValue(Path.of(System.getProperty("sf.paimon.batch.report")).toFile(), report);
    }
  }

  private String ddl(String name, String format, boolean primaryKey, int bucket) {
    return "CREATE TABLE "
        + name
        + " (id INT NOT NULL,k INT NOT NULL,v STRING"
        + (primaryKey ? ", PRIMARY KEY(id,k) NOT ENFORCED" : "")
        + ") PARTITIONED BY(k)"
        + " WITH ('connector'='paimon','path'='"
        + tablePath(name).toUri()
        + "','bucket'='"
        + bucket
        + "','sink.parallelism'='2','file.format'='"
        + format
        + "','file.compression'='snappy','write-only'='true')";
  }

  private Path tablePath(String name) {
    return directory.resolve(name);
  }

  private void create(String name, String format, boolean primaryKey, int bucket) throws Exception {
    var fields =
        List.of(
            new org.apache.paimon.types.DataField(
                0, "id", new org.apache.paimon.types.IntType(false)),
            new org.apache.paimon.types.DataField(
                1, "k", new org.apache.paimon.types.IntType(false)),
            new org.apache.paimon.types.DataField(
                2, "v", org.apache.paimon.types.DataTypes.STRING()));
    var schema =
        new org.apache.paimon.schema.Schema(
            fields,
            List.of("k"),
            primaryKey ? List.of("id", "k") : List.of(),
            Map.of(
                "bucket",
                Integer.toString(bucket),
                "file.format",
                format,
                "file.compression",
                "snappy",
                "write-only",
                "true"),
            null);
    tech.streamfusion.paimon.PaimonTestCompat.schemaManager(
            LocalFileIO.create(), new org.apache.paimon.fs.Path(tablePath(name).toUri()))
        .createTable(schema);
  }

  private FileStoreTable table(String name) {
    return FileStoreTableFactory.create(
        LocalFileIO.create(), new org.apache.paimon.fs.Path(tablePath(name).toUri()));
  }

  private static String payload(int id) {
    return "value-" + id % 4096 + "-abcdefghijklmnopqrstuvwxyz-ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  }

  private static void validate(FileStoreTable table, int rows) throws Exception {
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
            assertTrue(id >= rows / 4 && id < rows && !seen.get(id));
            seen.set(id);
            assertEquals(id % 7, row.getInt(1));
            assertEquals(
                id % 17 == 0 ? null : payload(id),
                row.isNullAt(2) ? null : row.getString(2).toString());
          }
        } finally {
          batch.releaseBatch();
        }
      }
    }
    assertEquals(rows - rows / 4, seen.cardinality());
  }

  private static void delete(Path path) throws Exception {
    try (var paths = java.nio.file.Files.walk(path)) {
      for (Path entry :
          paths
              .sorted(java.util.Comparator.reverseOrder())
              .collect(java.util.stream.Collectors.toList())) java.nio.file.Files.delete(entry);
    }
  }
}

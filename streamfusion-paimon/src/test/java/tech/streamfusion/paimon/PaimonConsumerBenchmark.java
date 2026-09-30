package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.Arrays;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

class PaimonConsumerBenchmark {
  @Test
  @EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
  void compareWholeJobs() throws Exception {
    int rows = Integer.getInteger("consumer.rows", 200_000);
    int warmups = Integer.getInteger("consumer.warmups", 2);
    int runs = Integer.getInteger("consumer.runs", 5);
    boolean primaryKey = Boolean.getBoolean("consumer.primaryKey");
    String format = PaimonTestTables.fileFormat();
    var warehouse = Files.createTempDirectory("consumer-benchmark");
    var setup = environment(warehouse);
    setup.executeSql(
        "CREATE TABLE t (id BIGINT NOT NULL, payload STRING, amount BIGINT"
            + (primaryKey ? ", PRIMARY KEY(id) NOT ENFORCED" : "")
            + ") WITH ('bucket'='"
            + (primaryKey ? "1" : "-1")
            + "', 'file.format'='"
            + format
            + "', 'changelog-producer'='"
            + (primaryKey ? "input" : "none")
            + "', 'write-only'='true', 'consumer-id'='benchmark', 'consumer.mode'='at-least-once',"
            + " 'consumer.expiration-time'='1 d', 'scan.bounded.watermark'='0',"
            + " 'continuous.discovery-interval'='10 ms')");
    var table =
        FileStoreTableFactory.create(
            LocalFileIO.create(), new Path(warehouse.resolve("default.db/t").toUri()));
    var builder = table.newStreamWriteBuilder().withCommitUser("benchmark");
    try (var io =
            new org.apache.paimon.disk.IOManagerImpl(
                Files.createTempDirectory("consumer-benchmark-io").toString());
        var writer = builder.newWrite();
        var commit = builder.newCommit()) {
      writer.withIOManager(io);
      for (long i = 0; i < rows; i++) {
        writer.write(
            GenericRow.of(
                i,
                i % 8 == 0
                    ? null
                    : BinaryString.fromString("payload-" + (i % 4096) + "x".repeat(48)),
                i % 7 == 0 ? null : i * 13));
      }
      ((TableCommitImpl) commit)
          .commit(new ManifestCommittable(1, 0L, writer.prepareCommit(true, 1)));
      writer.write(GenericRow.of(-1L, BinaryString.fromString("stop"), -1L));
      ((TableCommitImpl) commit)
          .commit(new ManifestCommittable(2, 1L, writer.prepareCommit(true, 2)));
    }
    var consumers = new ConsumerManager(table.fileIO(), table.location());
    consumers.resetConsumer("benchmark", new Consumer(1));
    var read = table.newReadBuilder();
    long decodedRows = 0;
    int nativeFiles = 0;
    int splitId = 0;
    var input = read.newStreamScan();
    var splits = new java.util.ArrayList<org.apache.paimon.table.source.Split>();
    boolean bounded = false;
    for (int attempt = 0; attempt < 10; attempt++) {
      try {
        splits.addAll(input.plan().splits());
      } catch (org.apache.paimon.table.source.EndOfScanException done) {
        bounded = true;
        break;
      }
    }
    assertTrue(bounded, "fixture must reach its bounded watermark");
    for (var split : splits) {
      try (var reader = new NativePaimonSplitReader(table, read, read.newRead(), 1024, -1)) {
        reader.handleSplitsChanges(
            new org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition<>(
                java.util.List.of(
                    new org.apache.paimon.flink.source.FileStoreSourceSplit(
                        "check-" + splitId++, split))));
        boolean finished = false;
        while (!finished) {
          var fetched = reader.fetch();
          if (fetched.nextSplit() != null) {
            var record = fetched.nextRecordFromSplit();
            if (record != null) {
              try (var root = record.batch().root()) {
                decodedRows += root.getRowCount();
              }
            }
          }
          finished = !fetched.finishedSplits().isEmpty();
          fetched.recycle();
        }
        nativeFiles += reader.nativeFilesRead();
      }
    }
    assertEquals(rows, decodedRows);
    assertTrue(nativeFiles > 0, "fixture must exercise native file decoding");
    var validation = environment(warehouse);
    var validationScan = NativePlanner.install(validation);
    long count = 0;
    long checksum = 0;
    try (var output = validation.executeSql("SELECT id, payload, amount FROM t").collect()) {
      while (output.hasNext()) {
        var row = output.next();
        count++;
        checksum += (Long) row.getField(0);
      }
    }
    assertEquals(rows, count, "bounded source must exclude the stop snapshot");
    assertEquals((long) rows * (rows - 1) / 2, checksum);
    assertEquals(1, validationScan.substitutions(), validationScan.explainSummary());
    var check = environment(warehouse);
    check.executeSql(
        "CREATE TEMPORARY TABLE out_t (id BIGINT, payload STRING, amount BIGINT)"
            + " WITH ('connector'='blackhole')");
    String plan =
        NativePlanner.explain(check, "INSERT INTO out_t SELECT id, payload, amount FROM t");
    if (primaryKey) {
      assertTrue(plan.contains("DropUpdateBefore"), plan);
      assertFalse(plan.contains("NativePaimonSource"), plan);
      plan = NativePlanner.explain(environment(warehouse), "SELECT id, payload, amount FROM t");
    }
    assertTrue(plan.contains("NativePaimonSource"), plan);
    assertTrue(plan.contains("ArrowToRowData"), plan);
    double[][] times = new double[3][runs];
    String[] modes = {"flink", "native", "previous_source"};
    for (int trial = 0; trial < warmups + runs; trial++) {
      for (int turn = 0; turn < modes.length; turn++) {
        int mode = (trial + turn) % modes.length;
        consumers.resetConsumer("benchmark", new Consumer(1));
        var env = executionEnvironment();
        var sql = environment(warehouse, env);
        if (mode == 2)
          sql.getConfig()
              .getConfiguration()
              .setString("streamfusion.operator.paimonSource.enabled", "false");
        sql.executeSql(
            "CREATE TEMPORARY TABLE out_t (id BIGINT, payload STRING, amount BIGINT)"
                + " WITH ('connector'='blackhole')");
        String query = "INSERT INTO out_t SELECT id, payload, amount FROM t";
        var scan = mode == 0 ? null : NativePlanner.install(sql);
        long start = System.nanoTime();
        if (primaryKey) {
          sql.toChangelogStream(sql.sqlQuery("SELECT id, payload, amount FROM t"))
              .sinkTo(new org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink<>());
          env.execute("consumer full-changelog benchmark");
        } else {
          sql.executeSql(query).await();
        }
        double seconds = (System.nanoTime() - start) / 1e9;
        if (mode == 1) assertEquals(1, scan.substitutions(), scan.explainSummary());
        if (mode == 2) assertEquals(0, scan.substitutions(), scan.explainSummary());
        if (trial >= warmups) times[mode][trial - warmups] = seconds;
      }
    }
    for (int mode = 0; mode < modes.length; mode++) {
      var sorted = times[mode].clone();
      Arrays.sort(sorted);
      System.out.printf(
          "[consumer] format=%s primary_key=%s rows=%d mode=%s median=%.9f trials=%s%n",
          format,
          primaryKey,
          rows,
          modes[mode],
          sorted[sorted.length / 2],
          Arrays.toString(times[mode]));
    }
  }

  private static StreamTableEnvironment environment(java.nio.file.Path warehouse) {
    return environment(warehouse, executionEnvironment());
  }

  private static StreamExecutionEnvironment executionEnvironment() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100);
    return env;
  }

  private static StreamTableEnvironment environment(
      java.nio.file.Path warehouse, StreamExecutionEnvironment env) {
    var sql = StreamTableEnvironment.create(env);
    sql.executeSql(
        "CREATE CATALOG p WITH ('type'='paimon', 'warehouse'='" + warehouse.toUri() + "')");
    sql.executeSql("USE CATALOG p");
    return sql;
  }
}

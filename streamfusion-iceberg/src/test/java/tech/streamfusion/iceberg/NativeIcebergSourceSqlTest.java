package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class NativeIcebergSourceSqlTest {
  @TempDir Path temp;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void continuousSqlKeepsDiscoveryFiltersProjectionAndFinalDrain(boolean nativeMode)
      throws Exception {
    var config =
        Configuration.fromMap(
            Map.of(
                "restart-strategy.type",
                "none",
                "taskmanager.memory.task.off-heap.size",
                "256 mb"));
    var env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(2);
    env.setMaxParallelism(16);
    env.enableCheckpointing(100);
    var sql = StreamTableEnvironment.create(env);
    var planner = nativeMode ? NativePlanner.install(sql) : null;
    sql.executeSql(
        "CREATE CATALOG ice WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='"
            + temp.resolve("warehouse").toUri()
            + "')");
    sql.executeSql("CREATE DATABASE ice.db");
    sql.executeSql(
        "CREATE TABLE ice.db.input (id INT, payload STRING) WITH ('format-version'='2',"
            + " 'streaming'='true', 'monitor-interval'='10ms',"
            + " 'starting-strategy'='TABLE_SCAN_THEN_INCREMENTAL')");
    sql.executeSql(
        "CREATE TABLE ice.db.output (id INT, payload STRING) WITH ('format-version'='2',"
            + " 'write.distribution-mode'='none')");
    try (var catalog =
        new HadoopCatalog(
            new org.apache.hadoop.conf.Configuration(),
            temp.resolve("warehouse").toUri().toString())) {
      var input = catalog.loadTable(TableIdentifier.of("db", "input"));
      var output = catalog.loadTable(TableIdentifier.of("db", "output"));
      append(input, 0, 101);
      String query =
          "INSERT INTO ice.db.output SELECT id + 1, payload FROM ice.db.input WHERE id >= 13 AND"
              + " payload IS NOT NULL";
      var explanation = sql.explainSql(query);
      if (nativeMode)
        assertTrue(
            explanation.contains("NativeIcebergSource"),
            explanation + "\n" + planner.fallbackReasons());
      var job = sql.executeSql(query).getJobClient().orElseThrow();
      boolean stopped = false;
      try {
        awaitRows(output, 88);
        append(input, 101, 104);
        awaitRows(output, 91);
        var savepoint =
            job.stopWithSavepoint(
                    true,
                    temp.resolve("savepoints").toUri().toString(),
                    SavepointFormatType.CANONICAL)
                .get(30, TimeUnit.SECONDS);
        assertNotNull(savepoint);
        job.getJobExecutionResult().get(30, TimeUnit.SECONDS);
        stopped = true;
        output.refresh();
        var ids = new java.util.TreeSet<Integer>();
        try (var rows = IcebergGenerics.read(output).build()) {
          for (var row : rows)
            assertTrue(ids.add((Integer) row.getField("id")), "duplicate committed row");
        }
        assertEquals(
            java.util.stream.IntStream.range(14, 105)
                .boxed()
                .collect(java.util.stream.Collectors.toSet()),
            ids);
        if (nativeMode)
          NativeIcebergSqlTest.assertNativeDataFiles(output, planner.fallbackReasons().toString());
      } finally {
        if (!stopped) {
          job.cancel().get(30, TimeUnit.SECONDS);
          job.getJobExecutionResult().handle((result, failure) -> null).get(30, TimeUnit.SECONDS);
        }
      }
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  static void append(Table table, int start, int end) throws Exception {
    var factory =
        new GenericFileWriterFactory.Builder(table).dataFileFormat(FileFormat.PARQUET).build();
    var writer =
        factory.newDataWriter(
            table
                .encryption()
                .encrypt(
                    table.io().newOutputFile(table.location() + "/seed-" + start + ".parquet")),
            table.spec(),
            null);
    try (writer) {
      var record = GenericRecord.create(table.schema());
      for (int id = start; id < end; id++) {
        record.setField("id", id);
        record.setField("payload", "value-" + id);
        writer.write(record);
      }
    }
    table.newAppend().appendFile(writer.toDataFile()).commit();
  }

  private static void awaitRows(Table table, int expected) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (System.nanoTime() < deadline) {
      table.refresh();
      long count = 0;
      try (var rows = IcebergGenerics.read(table).build()) {
        for (var ignored : rows) count++;
      }
      if (count == expected) return;
      if (count > expected) fail("too many committed rows: " + count);
      Thread.sleep(20);
    }
    fail("timed out waiting for " + expected + " committed rows");
  }
}

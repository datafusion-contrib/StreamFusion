package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
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
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.NativePlanner;

class NativeIcebergNestedSqlTest {
  @TempDir Path temp;

  @ParameterizedTest
  @CsvSource({"false,2", "true,2", "false,3", "true,3"})
  void streamingSourceAndSinkKeepNestedValuesAcrossLaterSnapshots(boolean nativeMode, int version)
      throws Exception {
    IcebergNativeMetrics.reset();
    var config =
        Configuration.fromMap(
            Map.of(
                "restart-strategy.type",
                "none",
                "taskmanager.memory.task.off-heap.size",
                "256 mb",
                "metrics.reporters",
                "iceberg",
                "metrics.reporter.iceberg.factory.class",
                IcebergNativeMetrics.class.getName()));
    var env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(2);
    env.enableCheckpointing(100);
    var sql = StreamTableEnvironment.create(env);
    var planner = nativeMode ? NativePlanner.install(sql) : null;
    String warehouse = temp.resolve("warehouse").toUri().toString();
    sql.executeSql(
        "CREATE CATALOG ice WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='"
            + warehouse
            + "')");
    sql.executeSql("CREATE DATABASE ice.db");
    String columns =
        "(id INT, profile ROW<n INT, label STRING, ts TIMESTAMP(6)>, items ARRAY<ROW<name STRING,"
            + " vals ARRAY<DOUBLE>>>, attrs MAP<STRING, ROW<n INT>>)";
    sql.executeSql(
        "CREATE TABLE ice.db.input "
            + columns
            + " WITH ('format-version'='"
            + version
            + "', 'streaming'='true', 'monitor-interval'='10ms',"
            + " 'starting-strategy'='TABLE_SCAN_THEN_INCREMENTAL')");
    sql.executeSql(
        "CREATE TABLE ice.db.output "
            + columns
            + " WITH ('format-version'='"
            + version
            + "', 'write.distribution-mode'='none')");
    try (var catalog = new HadoopCatalog(new org.apache.hadoop.conf.Configuration(), warehouse)) {
      var input = catalog.loadTable(TableIdentifier.of("db", "input"));
      var output = catalog.loadTable(TableIdentifier.of("db", "output"));
      append(input, 0, 4);
      addNestedEqualities(input);
      String query = "INSERT INTO ice.db.output SELECT * FROM ice.db.input WHERE id >= 1";
      var plan = sql.explainSql(query);
      if (nativeMode) {
        assertTrue(plan.contains("NativeIcebergSource"), plan + planner.fallbackReasons());
        assertTrue(plan.contains("NativeIcebergSink"), plan + planner.fallbackReasons());
        assertFalse(plan.contains("RowDataToArrow"), plan);
        assertFalse(plan.contains("ArrowToRowData"), plan);
      }
      var job = sql.executeSql(query).getJobClient().orElseThrow();
      boolean stopped = false;
      try {
        awaitRows(output, 2);
        append(input, 4, 7);
        awaitRows(output, 5);
        job.stopWithSavepoint(
                true, temp.resolve("savepoints").toUri().toString(), SavepointFormatType.CANONICAL)
            .get(30, TimeUnit.SECONDS);
        job.getJobExecutionResult().get(30, TimeUnit.SECONDS);
        stopped = true;
        var expected = records(input);
        expected.remove(0);
        assertEquals(expected, records(output));
        if (nativeMode) {
          assertTrue(IcebergNativeMetrics.count("nativeParquetTasks") >= 2);
          assertEquals(0, IcebergNativeMetrics.count("stockFallbackTasks"));
          NativeIcebergSqlTest.assertNativeDataFiles(output, planner.fallbackReasons().toString());
        }
      } finally {
        if (!stopped) {
          job.cancel().get(30, TimeUnit.SECONDS);
          job.getJobExecutionResult().handle((result, failure) -> null).get(30, TimeUnit.SECONDS);
        }
      }
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
    assertEquals(0, tech.streamfusion.operator.NativeAllocator.SHARED.getAllocatedMemory());
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
      for (int id = start; id < end; id++) {
        var row = GenericRecord.create(table.schema());
        row.setField("id", id);
        var profile = GenericRecord.create(table.schema().findType("profile").asStructType());
        profile.setField("n", id * 10);
        profile.setField("label", id % 2 == 0 ? "é🐍-" + id : null);
        profile.setField(
            "ts", LocalDateTime.parse("1969-12-31T23:59:59.999999").plusNanos(id * 1000L));
        row.setField("profile", id % 3 == 1 ? null : profile);
        var item =
            GenericRecord.create(
                table.schema().findType("items").asListType().elementType().asStructType());
        item.setField("name", "item-" + id);
        item.setField("vals", java.util.Arrays.asList(0.5, null, Double.NaN));
        var items = new ArrayList<>();
        if (id % 3 != 0) {
          items.add(item);
          items.add(null);
        }
        row.setField("items", id % 3 == 2 ? null : items);
        var value =
            GenericRecord.create(
                table.schema().findType("attrs").asMapType().valueType().asStructType());
        value.setField("n", id);
        var attrs = new LinkedHashMap<>();
        attrs.put("a", value);
        attrs.put("b", null);
        row.setField("attrs", id % 2 == 0 ? attrs : Map.of());
        writer.write(row);
      }
    }
    table.newAppend().appendFile(writer.toDataFile()).commit();
  }

  private static void addNestedEqualities(Table table) throws Exception {
    var keys = table.schema().select("profile.n");
    var factory =
        new GenericFileWriterFactory.Builder(table)
            .dataFileFormat(FileFormat.PARQUET)
            .deleteFileFormat(FileFormat.PARQUET)
            .equalityFieldIds(new int[] {table.schema().findField("profile.n").fieldId()})
            .equalityDeleteRowSchema(keys)
            .build();
    var writer =
        factory.newEqualityDeleteWriter(
            table
                .encryption()
                .encrypt(table.io().newOutputFile(table.location() + "/equalities.parquet")),
            table.spec(),
            null);
    try (writer) {
      var key = GenericRecord.create(keys);
      key.setField("profile", null);
      writer.write(key);
    }
    table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
  }

  private static Map<Integer, org.apache.iceberg.data.Record> records(Table table)
      throws Exception {
    table.refresh();
    var records = new TreeMap<Integer, org.apache.iceberg.data.Record>();
    try (var input = IcebergGenerics.read(table).build()) {
      for (var row : input) assertNull(records.put((Integer) row.getField("id"), row));
    }
    return records;
  }

  private static void awaitRows(Table table, int count) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
    while (System.nanoTime() < deadline) {
      if (records(table).size() == count) return;
      Thread.sleep(20);
    }
    fail("Timed out waiting for " + count + " nested rows");
  }
}

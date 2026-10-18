package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.planner.NativePlanner;

class NativeIcebergSqlTest {
  @TempDir Path temp;

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void millisecondSqlTypesMatchStockAndKeepNativeEncoding(boolean nativeMode) throws Exception {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.configure(Configuration.fromMap(java.util.Map.of("restart-strategy.type", "none")));
    env.setParallelism(1);
    var sql = StreamTableEnvironment.create(env);
    var scan = nativeMode ? NativePlanner.install(sql) : null;
    sql.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
    sql.executeSql(
        "CREATE CATALOG ice WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='"
            + temp.toUri()
            + "')");
    sql.executeSql("CREATE DATABASE ice.db");
    sql.executeSql(
        "CREATE TABLE ice.db.t (event_ts TIMESTAMP(3), event_ltz TIMESTAMP_LTZ(3), event_clock"
            + " TIME(3)) WITH ('format-version'='2', 'write.distribution-mode'='none')");
    sql.executeSql(
            "INSERT INTO ice.db.t SELECT TIMESTAMP '1969-12-31 23:59:59.999', "
                + "TO_TIMESTAMP_LTZ(-1, 3), CAST('23:59:59.999' AS TIME(3))")
        .await();
    try (var catalog =
        new HadoopCatalog(new org.apache.hadoop.conf.Configuration(), temp.toUri().toString())) {
      var table = catalog.loadTable(TableIdentifier.of("db", "t"));
      try (var records = IcebergGenerics.read(table).build()) {
        var iterator = records.iterator();
        assertTrue(iterator.hasNext());
        var record = iterator.next();
        assertEquals(
            java.time.LocalDateTime.parse("1969-12-31T23:59:59.999"), record.getField("event_ts"));
        assertEquals(
            java.time.Instant.ofEpochMilli(-1).atOffset(java.time.ZoneOffset.UTC),
            record.getField("event_ltz"));
        // The released Flink Iceberg catalog exposes TIME at precision zero.
        assertEquals(java.time.LocalTime.parse("23:59:59"), record.getField("event_clock"));
        assertFalse(iterator.hasNext());
      }
      if (nativeMode) assertNativeDataFiles(table, scan.fallbackReasons().toString());
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({"false,2", "true,2", "false,3", "true,3"})
  void updatingSqlKeepsJavaDeleteEncoding(boolean upsert, int formatVersion) throws Exception {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.configure(Configuration.fromMap(java.util.Map.of("restart-strategy.type", "none")));
    env.setParallelism(2);
    env.setMaxParallelism(16);
    env.enableCheckpointing(1000);
    var sql = StreamTableEnvironment.create(env);
    var scan = NativePlanner.install(sql);
    sql.executeSql(
        "CREATE CATALOG ice WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='"
            + temp.toUri()
            + "')");
    sql.executeSql("CREATE DATABASE ice.db");
    sql.executeSql(
        "CREATE TABLE ice.db.t (id INT NOT NULL, n BIGINT, PRIMARY KEY (id) NOT ENFORCED) "
            + "PARTITIONED BY (id) WITH ('format-version'='"
            + formatVersion
            + "', 'write.distribution-mode'='hash', "
            + "'write.upsert.enabled'='"
            + upsert
            + "')");
    sql.executeSql(
            "INSERT INTO ice.db.t SELECT id, COUNT(*) FROM (VALUES (1), (2), (1), (1), (2)) AS"
                + " v(id) GROUP BY id")
        .await();
    try (var catalog =
        new HadoopCatalog(new org.apache.hadoop.conf.Configuration(), temp.toUri().toString())) {
      var table = catalog.loadTable(TableIdentifier.of("db", "t"));
      var values = new java.util.TreeMap<Integer, Long>();
      try (var rows = IcebergGenerics.read(table).build()) {
        for (var row : rows) {
          assertNull(values.put((Integer) row.getField("id"), (Long) row.getField("n")));
        }
      }
      assertEquals(java.util.Map.of(1, 3L, 2, 2L), values);
      assertNativeDataFiles(table, scan.fallbackReasons().toString());
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "none,1", "none,2", "none,3", "hash,2", "hash,3", "range,1", "range,2", "range,3"
  })
  void sqlRetainsTheHostCommitterAndUsesTheNativeDataAppender(
      String distribution, int formatVersion) throws Exception {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.configure(Configuration.fromMap(java.util.Map.of("restart-strategy.type", "none")));
    env.setParallelism(distribution.equals("hash") ? 2 : 1);
    env.setMaxParallelism(2);
    env.enableCheckpointing(1000);
    var sql = StreamTableEnvironment.create(env);
    var scan = NativePlanner.install(sql);
    sql.executeSql(
        "CREATE CATALOG ice WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='"
            + temp.toUri()
            + "')");
    sql.executeSql("CREATE DATABASE ice.db");
    sql.executeSql(
        "CREATE TABLE ice.db.t (id INT, payload STRING) "
            + (!distribution.equals("none") ? "PARTITIONED BY (payload) " : "")
            + "WITH ('format-version'='"
            + formatVersion
            + "', 'write.distribution-mode'='"
            + distribution
            + "')");
    sql.executeSql("INSERT INTO ice.db.t VALUES (1, 'first'), (2, CAST(NULL AS STRING))").await();
    try (var catalog =
        new HadoopCatalog(new org.apache.hadoop.conf.Configuration(), temp.toUri().toString())) {
      var table = catalog.loadTable(TableIdentifier.of("db", "t"));
      var values = new java.util.TreeMap<Integer, Object>();
      try (var rows = IcebergGenerics.read(table).build()) {
        for (var row : rows) values.put((Integer) row.getField("id"), row.getField("payload"));
      }
      assertEquals(2, values.size());
      assertEquals("first", values.get(1));
      assertNull(values.get(2));
      assertNativeDataFiles(table, scan.fallbackReasons().toString());
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  static void assertNativeDataFiles(org.apache.iceberg.Table table, String fallbacks)
      throws Exception {
    int files = 0;
    try (var tasks = table.newScan().planFiles()) {
      for (var task : tasks) {
        try (var parquet =
            ParquetFileReader.open(
                org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                    new org.apache.hadoop.fs.Path(task.file().location()),
                    new org.apache.hadoop.conf.Configuration()))) {
          assertTrue(
              parquet.getFooter().getFileMetaData().getCreatedBy().contains("parquet-rs"),
              () -> "Expected native data files; planner fallbacks: " + fallbacks);
        }
        files++;
      }
    }
    assertTrue(files > 0);
  }
}

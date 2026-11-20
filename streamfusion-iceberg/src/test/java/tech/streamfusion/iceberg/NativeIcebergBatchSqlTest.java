package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.OutputFileFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.NativePlannerTestEnvironment;

class NativeIcebergBatchSqlTest {
  @TempDir Path temp;

  @Test
  void groupingSetsKeepOneArrowSchemaForSourceColumnsAndNullLiterals() throws Exception {
    var sql = environment();
    sql.executeSql(
        "CREATE TABLE ice.db.input (id INT, payload STRING) WITH ('format-version'='2')");
    try (var catalog = catalog()) {
      NativeIcebergSourceSqlTest.append(table(catalog, "input"), 0, 3);
    }
    String select =
        "SELECT id, GROUPING(id), COUNT(*) FROM ice.db.input GROUP BY GROUPING SETS ((id), ())";
    var plan = sql.explainSql(select);
    assertTrue(plan.contains("NativeExpand"), plan);
    assertTrue(plan.contains("NativeConnectorSource"), plan);
    var rows = new java.util.HashSet<Row>();
    try (var result = sql.executeSql(select).collect()) {
      while (result.hasNext()) assertTrue(rows.add(result.next()));
    }
    assertEquals(
        java.util.Set.of(
            Row.of(0, 0L, 1L), Row.of(1, 0L, 1L), Row.of(2, 0L, 1L), Row.of(null, 1L, 3L)),
        rows);
    assertTrue(IcebergNativeMetrics.count("nativeParquetTasks") > 0);
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "avro"})
  void stringGroupingUsesNativeComputeWithEitherFileReader(String format) throws Exception {
    var sql = environment();
    sql.executeSql(
        "CREATE TABLE ice.db.input (id INT, payload STRING) WITH ('write.format.default'='"
            + format
            + "')");
    sql.executeSql(
            "INSERT INTO ice.db.input VALUES (1, '寿司'), (2, '寿司'), (3, '😀'),"
                + " (4, CAST(NULL AS STRING))")
        .await(45, TimeUnit.SECONDS);
    IcebergNativeMetrics.reset();
    String select = "SELECT payload, COUNT(*) FROM ice.db.input GROUP BY payload";
    var plan = sql.explainSql(select);
    assertTrue(plan.contains("NativeAggregate"), plan);
    assertTrue(plan.contains("NativeConnectorSource"), plan);
    var rows = new java.util.HashSet<Row>();
    try (var result = sql.executeSql(select).collect()) {
      while (result.hasNext()) assertTrue(rows.add(result.next()));
    }
    assertEquals(java.util.Set.of(Row.of("寿司", 2L), Row.of("😀", 1L), Row.of(null, 1L)), rows);
    String counter = format.equals("parquet") ? "nativeParquetTasks" : "stockFallbackTasks";
    assertTrue(IcebergNativeMetrics.count(counter) > 0);
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  @Test
  void nullSafeCompositeSemiJoinKeepsEveryMatchingProbeRow() throws Exception {
    var sql = environment();
    sql.executeSql("CREATE TABLE ice.db.probe_rows (id INT, payload STRING, seq INT)");
    sql.executeSql("CREATE TABLE ice.db.build_rows (id INT, payload STRING)");
    sql.executeSql(
            "INSERT INTO ice.db.probe_rows VALUES (1, 'a', 1), (1, 'a', 2),"
                + " (1, CAST(NULL AS STRING), 3), (CAST(NULL AS INT), 'a', 4),"
                + " (CAST(NULL AS INT), CAST(NULL AS STRING), 5), (2, 'b', 6),"
                + " (2, CAST(NULL AS STRING), 7)")
        .await(45, TimeUnit.SECONDS);
    sql.executeSql(
            "INSERT INTO ice.db.build_rows VALUES (1, 'a'), (1, 'a'), (1, CAST(NULL AS STRING)),"
                + " (CAST(NULL AS INT), CAST(NULL AS STRING))")
        .await(45, TimeUnit.SECONDS);
    String select =
        "WITH matched_keys AS (SELECT id, payload FROM ice.db.probe_rows INTERSECT"
            + " SELECT id, payload FROM ice.db.build_rows)"
            + " SELECT p.seq FROM ice.db.probe_rows p JOIN matched_keys m"
            + " ON p.id IS NOT DISTINCT FROM m.id AND p.payload IS NOT DISTINCT FROM m.payload"
            + " ORDER BY p.seq";
    var plan = sql.explainSql(select);
    assertTrue(plan.contains("NativeJoin(joinType=[LeftSemiJoin]"), plan);
    var values = new java.util.ArrayList<Integer>();
    try (var rows = sql.executeSql(select).collect()) {
      while (rows.hasNext()) values.add((Integer) rows.next().getField(0));
    }
    assertEquals(java.util.List.of(1, 2, 3, 5), values);
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  @Test
  void sharedJoinReadsEachIcebergInputOnceAcrossTwoAggregates() throws Exception {
    var sql = environment();
    try (var catalog = catalog()) {
      for (var name : java.util.List.of("left_input", "right_input")) {
        sql.executeSql("CREATE TABLE ice.db." + name + " (id INT, payload STRING)");
        NativeIcebergSourceSqlTest.append(table(catalog, name), 0, 3);
      }
    }
    String select =
        "WITH joined AS (SELECT CAST(l.id AS BIGINT) + 1 AS v FROM ice.db.left_input l"
            + " JOIN ice.db.right_input r ON l.id = r.id)"
            + " SELECT SUM(v) FROM joined UNION ALL SELECT MAX(v) FROM joined";
    var plan = sql.explainSql(select);
    assertTrue(plan.contains("NativeJoin"), plan);
    assertTrue(plan.contains("NativeShare"), plan);
    var values = new java.util.ArrayList<Long>();
    try (var rows = sql.executeSql(select).collect()) {
      while (rows.hasNext()) values.add((Long) rows.next().getField(0));
    }
    values.sort(Long::compareTo);
    assertEquals(java.util.List.of(3L, 6L), values);
    assertEquals(2, IcebergNativeMetrics.count("nativeParquetTasks"), plan);
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  @ParameterizedTest
  @CsvSource({"1,none", "2,hash", "3,range"})
  void snapshotCopyKeepsProjectionFiltersRoutingAndEndOfInputCommit(int version, String mode)
      throws Exception {
    var sql = environment();
    sql.executeSql(
        "CREATE TABLE ice.db.input (id INT, payload STRING) WITH ('format-version'='"
            + version
            + "')");
    sql.executeSql(
        "CREATE TABLE ice.db.output (id INT, payload STRING) PARTITIONED BY (payload) "
            + "WITH ('format-version'='"
            + version
            + "', 'write.distribution-mode'='"
            + mode
            + "', 'write-parallelism'='2')");
    try (var catalog = catalog()) {
      var input = table(catalog, "input");
      NativeIcebergSourceSqlTest.append(input, 0, 100);
      if (version == 3) {
        try (var tasks = input.newScan().planFiles()) {
          var file = tasks.iterator().next().file();
          var deletes =
              new BaseDVFileWriter(
                  OutputFileFactory.builderFor(input, 0, 0).format(FileFormat.PUFFIN).build(),
                  ignored -> null);
          try (deletes) {
            deletes.delete(file.location(), 17, input.spec(), null);
          }
          var delta = input.newRowDelta();
          deletes.result().deleteFiles().forEach(delta::addDeletes);
          delta.commit();
        }
      }
      long snapshot = input.currentSnapshot().snapshotId();
      NativeIcebergSourceSqlTest.append(input, 100, 105);
      String insert =
          "INSERT INTO ice.db.output SELECT id + 1,"
              + " CASE MOD(id, 3) WHEN 0 THEN 'a' WHEN 1 THEN 'b' END FROM ice.db.input "
              + "/*+ OPTIONS('snapshot-id'='"
              + snapshot
              + "', 'source.parallelism'='2') */ WHERE id >= 13 AND payload <> 'value-20'";
      sql.executeSql(insert).await(45, TimeUnit.SECONDS);
      var output = table(catalog, "output");
      Map<Integer, String> expected = new HashMap<>();
      IntStream.range(13, 100)
          .filter(id -> id != 20 && (version != 3 || id != 17))
          .forEach(id -> expected.put(id + 1, id % 3 == 0 ? "a" : id % 3 == 1 ? "b" : null));
      assertEquals(expected, rows(output));
      assertTrue(IcebergNativeMetrics.count("nativeParquetTasks") > 0);
      assertEquals(0, IcebergNativeMetrics.count("stockFallbackTasks"));
      NativeIcebergSqlTest.assertNativeDataFiles(output, sql.explainSql(insert));
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3})
  void keyedUpsertUsesJavaDeletesAndNativeDataFiles(int version) throws Exception {
    var sql = environment();
    sql.executeSql(
        "CREATE TABLE ice.db.target (id INT NOT NULL, payload STRING, PRIMARY KEY (id) NOT"
            + " ENFORCED) PARTITIONED BY (id) WITH ('format-version'='"
            + version
            + "', 'write.distribution-mode'='range', 'write.upsert.enabled'='true')");
    sql.executeSql("INSERT INTO ice.db.target VALUES (1, 'old'), (2, 'keep')")
        .await(45, TimeUnit.SECONDS);
    sql.executeSql("INSERT INTO ice.db.target VALUES (1, 'new'), (3, 'added')")
        .await(45, TimeUnit.SECONDS);
    try (var catalog = catalog()) {
      var target = table(catalog, "target");
      assertEquals(Map.of(1, "new", 2, "keep", 3, "added"), rows(target));
      assertTrue(target.currentSnapshot().addedDeleteFiles(target.io()).iterator().hasNext());
      NativeIcebergSqlTest.assertNativeDataFiles(target, "keyed batch upsert");
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  @Test
  void partitionOverwriteReplacesOnlyWrittenPartitions() throws Exception {
    var sql = environment();
    sql.executeSql(
        "CREATE TABLE ice.db.target (id INT, payload STRING) PARTITIONED BY (payload) "
            + "WITH ('format-version'='2', 'write.distribution-mode'='hash')");
    sql.executeSql("INSERT INTO ice.db.target VALUES (1, 'replace'), (2, 'keep')")
        .await(45, TimeUnit.SECONDS);
    sql.executeSql("INSERT OVERWRITE ice.db.target PARTITION (payload='replace') SELECT 3")
        .await(45, TimeUnit.SECONDS);
    try (var catalog = catalog()) {
      var target = table(catalog, "target");
      assertEquals(Map.of(2, "keep", 3, "replace"), rows(target));
      assertEquals("overwrite", target.currentSnapshot().operation());
      NativeIcebergSqlTest.assertNativeDataFiles(target, "batch overwrite");
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  @Test
  void nestedSnapshotCopyAppliesEqualityDeletes() throws Exception {
    var sql = environment();
    String columns =
        "(id INT, profile ROW<n INT, label STRING, ts TIMESTAMP(6)>, items ARRAY<ROW<name STRING,"
            + " vals ARRAY<DOUBLE>>>, attrs MAP<STRING, ROW<n INT>>)";
    for (String name : java.util.List.of("input", "output")) {
      sql.executeSql(
          "CREATE TABLE ice.db." + name + " " + columns + " WITH ('format-version'='2')");
    }
    try (var catalog = catalog()) {
      var input = table(catalog, "input");
      NativeIcebergNestedSqlTest.append(input, 0, 7);
      NativeIcebergNestedSqlTest.addNestedEqualities(input);
      sql.executeSql("INSERT INTO ice.db.output SELECT * FROM ice.db.input WHERE id >= 1")
          .await(45, TimeUnit.SECONDS);
      var expected = NativeIcebergNestedSqlTest.records(input);
      expected.remove(0);
      var output = table(catalog, "output");
      assertEquals(expected, NativeIcebergNestedSqlTest.records(output));
      assertTrue(IcebergNativeMetrics.count("nativeParquetTasks") > 0);
      assertEquals(0, IcebergNativeMetrics.count("stockFallbackTasks"));
      NativeIcebergSqlTest.assertNativeDataFiles(output, "nested bounded copy");
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  @Test
  void countWithZeroColumnProjectionUsesNativeReaderAndPushedLimitStaysStock() throws Exception {
    var sql = environment();
    sql.executeSql(
        "CREATE TABLE ice.db.input (id INT, payload STRING) WITH ('format-version'='2')");
    try (var catalog = catalog()) {
      NativeIcebergSourceSqlTest.append(table(catalog, "input"), 0, 19);
      try (var result = sql.executeSql("SELECT COUNT(*) FROM ice.db.input").collect()) {
        assertEquals(Row.of(19L), result.next());
        assertFalse(result.hasNext());
      }
      assertTrue(IcebergNativeMetrics.count("nativeParquetTasks") > 0);
      IcebergNativeMetrics.reset();
      try (var result = sql.executeSql("SELECT id FROM ice.db.input LIMIT 2").collect()) {
        for (int i = 0; i < 2; i++) assertTrue((Integer) result.next().getField(0) < 19);
        assertFalse(result.hasNext());
      }
      assertEquals(0, IcebergNativeMetrics.count("nativeParquetTasks"));
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  @Test
  void batchReadsAndWritesTheRequestedBranch() throws Exception {
    var sql = environment();
    sql.executeSql(
        "CREATE TABLE ice.db.input (id INT, payload STRING) WITH ('format-version'='2')");
    try (var catalog = catalog()) {
      var input = table(catalog, "input");
      NativeIcebergSourceSqlTest.append(input, 0, 2);
      input.manageSnapshots().createBranch("audit").commit();
      sql.executeSql(
              "INSERT INTO ice.db.input /*+ OPTIONS('branch'='audit') */ VALUES (2, 'audit')")
          .await(45, TimeUnit.SECONDS);
      input.refresh();
      assertEquals(Map.of(0, "value-0", 1, "value-1"), rows(input));
      int branchFiles = 0;
      try (var tasks = input.newScan().useRef("audit").planFiles()) {
        for (var task : tasks) branchFiles++;
      }
      assertEquals(2, branchFiles);
      sql.executeSql("CREATE TABLE ice.db.output (id INT, payload STRING)");
      sql.executeSql(
              "INSERT INTO ice.db.output SELECT * FROM ice.db.input /*+ OPTIONS('branch'='audit')"
                  + " */")
          .await(45, TimeUnit.SECONDS);
      var output = table(catalog, "output");
      assertEquals(Map.of(0, "value-0", 1, "value-1", 2, "audit"), rows(output));
      assertEquals(2, IcebergNativeMetrics.count("nativeParquetTasks"));
      NativeIcebergSqlTest.assertNativeDataFiles(output, "branch copy");
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  @Test
  void requiredColumnErrorPreventsTheTerminalCommit() throws Exception {
    var sql = environment();
    sql.executeSql("CREATE TABLE ice.db.input (id INT, payload STRING)");
    sql.executeSql("CREATE TABLE ice.db.output (id INT, payload STRING NOT NULL)");
    sql.executeSql("INSERT INTO ice.db.input VALUES (1, 'valid'), (2, CAST(NULL AS STRING))")
        .await(45, TimeUnit.SECONDS);
    String insert = "INSERT INTO ice.db.output SELECT * FROM ice.db.input";
    assertTrue(sql.explainSql(insert).contains("NativeConnectorSink"));
    var failure =
        assertThrows(Exception.class, () -> sql.executeSql(insert).await(45, TimeUnit.SECONDS));
    var causes = new StringBuilder();
    for (Throwable cause = failure; cause != null; cause = cause.getCause())
      causes.append(cause.getMessage()).append('\n');
    assertTrue(causes.toString().contains("Column 'payload' is NOT NULL"), causes.toString());
    try (var catalog = catalog()) {
      assertNull(table(catalog, "output").currentSnapshot());
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  private TableEnvironment environment() throws Exception {
    IcebergNativeMetrics.reset();
    var config =
        Configuration.fromMap(
            Map.of(
                "parallelism.default", "2",
                "restart-strategy.type", "none",
                "taskmanager.memory.task.off-heap.size", "256 mb",
                "metrics.reporters", "iceberg",
                "metrics.reporter.iceberg.factory.class", IcebergNativeMetrics.class.getName()));
    var sql = NativePlannerTestEnvironment.createBatch(config);
    sql.executeSql(
        "CREATE CATALOG ice WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='"
            + temp.toUri()
            + "')");
    sql.executeSql("CREATE DATABASE ice.db");
    return sql;
  }

  private HadoopCatalog catalog() {
    return new HadoopCatalog(new org.apache.hadoop.conf.Configuration(), temp.toUri().toString());
  }

  private static Table table(HadoopCatalog catalog, String name) {
    return catalog.loadTable(TableIdentifier.of("db", name));
  }

  private static Map<Integer, String> rows(Table table) throws Exception {
    var result = new TreeMap<Integer, String>();
    try (var records = IcebergGenerics.read(table).build()) {
      for (var row : records) {
        int id = (Integer) row.getField("id");
        assertFalse(result.containsKey(id), "Duplicate committed id " + id);
        result.put(id, (String) row.getField("payload"));
      }
    }
    return result;
  }
}

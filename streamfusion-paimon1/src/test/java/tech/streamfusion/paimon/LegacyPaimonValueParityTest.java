package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.NativePlanner;

class LegacyPaimonValueParityTest {
  @org.junit.jupiter.api.io.TempDir java.nio.file.Path tempDir;

  @ParameterizedTest
  @CsvSource({"parquet,false", "parquet,true", "orc,false", "orc,true"})
  void nanBoundsNeverPruneMatchingRows(String format, boolean primaryKey) throws Exception {
    List<List<String>> twins = new ArrayList<>();
    for (boolean nativeSink : List.of(false, true)) {
      var path = Files.createTempDirectory(tempDir, "paimon1-nan").toUri().toString();
      var env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      var sql = StreamTableEnvironment.create(env);
      catalog(sql, path);
      sql.executeSql(
          "CREATE TABLE t (id INT NOT NULL, f FLOAT, d DOUBLE"
              + (primaryKey ? ", PRIMARY KEY(id) NOT ENFORCED" : "")
              + ") WITH ('bucket'='"
              + (primaryKey ? 1 : -1)
              + "', 'file.format'='"
              + format
              + "')");
      var input =
          env.fromCollection(
              List.of(
                  Row.of(1, Float.NaN, Double.NaN),
                  Row.of(2, Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY),
                  Row.of(3, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY),
                  Row.of(4, -0.0f, -0.0),
                  Row.of(5, 0.0f, 0.0),
                  Row.of(6, null, null)),
              Types.ROW_NAMED(new String[] {"id", "f", "d"}, Types.INT, Types.FLOAT, Types.DOUBLE));
      sql.createTemporaryView(
          "input_rows",
          sql.fromDataStream(
              input,
              Schema.newBuilder()
                  .column("id", DataTypes.INT().notNull())
                  .column("f", DataTypes.FLOAT())
                  .column("d", DataTypes.DOUBLE())
                  .build()));
      if (nativeSink) {
        var plan = NativePlanner.install(sql);
        assertTrue(
            sql.explainSql("INSERT INTO t SELECT * FROM input_rows").contains("NativePaimon"));
      }
      sql.executeSql("INSERT INTO t SELECT * FROM input_rows").await();
      var readerEnv = StreamExecutionEnvironment.getExecutionEnvironment();
      readerEnv.setParallelism(1);
      readerEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);
      var reader = StreamTableEnvironment.create(readerEnv);
      catalog(reader, path);
      List<String> rows = new ArrayList<>();
      for (String condition :
          List.of("TRUE", "d > 0", "f < 0", "d IS NULL", "f = 0", "d = CAST('NaN' AS DOUBLE)")) {
        try (var values = reader.executeSql("SELECT * FROM t WHERE " + condition).collect()) {
          values.forEachRemaining(r -> rows.add(condition + ":" + r));
        }
      }
      rows.sort(String::compareTo);
      twins.add(rows);
    }
    assertEquals(twins.get(0), twins.get(1));
  }

  @ParameterizedTest
  @CsvSource({"parquet", "orc"})
  void partialUpdateAppliesDefaultsAfterMerging(String format) throws Exception {
    List<List<String>> twins = new ArrayList<>();
    for (boolean nativeSink : List.of(false, true)) {
      var path = Files.createTempDirectory(tempDir, "paimon1-defaults").toUri().toString();
      for (int pass = 0; pass < 2; pass++) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        var sql = StreamTableEnvironment.create(env);
        catalog(sql, path);
        sql.executeSql(
            "CREATE TABLE IF NOT EXISTS t (id INT NOT NULL, v INT, PRIMARY KEY(id) NOT ENFORCED) "
                + "WITH ('bucket'='1', 'file.format'='"
                + format
                + "', 'merge-engine'='partial-update', 'changelog-producer'='input',"
                + " 'fields.v.default-value'='42')");
        var input =
            env.fromCollection(
                pass == 0
                    ? List.of(Row.of(1, 7), Row.of(2, null))
                    : List.of(Row.of(1, null), Row.of(2, null)),
                Types.ROW_NAMED(new String[] {"id", "v"}, Types.INT, Types.INT));
        sql.createTemporaryView(
            "input_rows",
            sql.fromDataStream(
                input,
                Schema.newBuilder()
                    .column("id", DataTypes.INT().notNull())
                    .column("v", DataTypes.INT())
                    .build()));
        if (nativeSink) {
          var plan = NativePlanner.install(sql);
          assertTrue(
              sql.explainSql("INSERT INTO t SELECT * FROM input_rows").contains("NativePaimon"));
          assertFalse(sql.explainSql("SELECT * FROM t").contains("NativePaimonSource"));
          assertTrue(
              plan.fallbackReasons().stream().anyMatch(r -> r.contains("applies field defaults")),
              plan.explainSummary());
        }
        sql.executeSql("INSERT INTO t SELECT * FROM input_rows").await();
      }
      var env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      env.setRuntimeMode(RuntimeExecutionMode.BATCH);
      var sql = StreamTableEnvironment.create(env);
      catalog(sql, path);
      List<String> rows = new ArrayList<>();
      try (var result = sql.executeSql("SELECT * FROM t").collect()) {
        result.forEachRemaining(r -> rows.add(r.toString()));
      }
      rows.sort(String::compareTo);
      assertEquals(List.of("+I[1, 7]", "+I[2, 42]"), rows);
      twins.add(rows);
    }
    assertEquals(twins.get(0), twins.get(1));
  }

  @ParameterizedTest
  @CsvSource({"parquet", "orc"})
  void timestampBundlesPreserveHistoricalCalendarAndMicroseconds(String format) throws Exception {
    List<List<String>> twins = new ArrayList<>();
    for (boolean nativeSink : List.of(false, true)) {
      var path = Files.createTempDirectory(tempDir, "paimon1-timestamps").toUri().toString();
      var env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      var sql = StreamTableEnvironment.create(env);
      catalog(sql, path);
      sql.executeSql(
          "CREATE TABLE t (id INT NOT NULL, ts TIMESTAMP(6), ltz TIMESTAMP_LTZ(6)) "
              + "WITH ('bucket'='-1', 'file.format'='"
              + format
              + "')");
      List<Row> inputRows = new ArrayList<>();
      for (int year : List.of(1, 1582, 1899, 1970, 2026, 9999)) {
        var time = java.time.LocalDateTime.of(year, 10, 10, 3, 4, 5, 123456000);
        inputRows.add(Row.of(year, time, time.toInstant(java.time.ZoneOffset.UTC)));
      }
      var input =
          env.fromCollection(
              inputRows,
              Types.ROW_NAMED(
                  new String[] {"id", "ts", "ltz"},
                  Types.INT,
                  Types.LOCAL_DATE_TIME,
                  Types.INSTANT));
      sql.createTemporaryView(
          "input_rows",
          sql.fromDataStream(
              input,
              Schema.newBuilder()
                  .column("id", DataTypes.INT().notNull())
                  .column("ts", DataTypes.TIMESTAMP(6))
                  .column("ltz", DataTypes.TIMESTAMP_LTZ(6))
                  .build()));
      if (nativeSink) {
        var plan = NativePlanner.install(sql);
        assertTrue(
            sql.explainSql("INSERT INTO t SELECT * FROM input_rows").contains("NativePaimon"),
            plan.explainSummary());
      }
      sql.executeSql("INSERT INTO t SELECT * FROM input_rows").await();
      var readerEnv = StreamExecutionEnvironment.getExecutionEnvironment();
      readerEnv.setParallelism(1);
      readerEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);
      var reader = StreamTableEnvironment.create(readerEnv);
      catalog(reader, path);
      List<String> result = new ArrayList<>();
      try (var rows = reader.executeSql("SELECT * FROM t").collect()) {
        rows.forEachRemaining(r -> result.add(r.toString()));
      }
      result.sort(String::compareTo);
      assertEquals(inputRows.size(), result.size());
      var table =
          org.apache.paimon.table.FileStoreTableFactory.create(
              org.apache.paimon.fs.local.LocalFileIO.create(),
              new org.apache.paimon.fs.Path(path + "default.db/t"));
      var read = table.newReadBuilder();
      var type = org.apache.paimon.flink.LogicalTypeConversion.toLogicalType(read.readType());
      List<String> nativeRows = new ArrayList<>();
      for (var split : read.newScan().plan().splits()) {
        try (var nativeReader = new NativePaimonSplitReader(table, read, read.newRead(), 3, -1)) {
          nativeReader.handleSplitsChanges(
              new org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition<>(
                  List.of(new org.apache.paimon.flink.source.FileStoreSourceSplit("s", split))));
          boolean finished = false;
          while (!finished) {
            var fetched = nativeReader.fetch();
            if (fetched.nextSplit() != null) {
              var record = fetched.nextRecordFromSplit();
              while (record != null) {
                try (var root = record.batch().root()) {
                  for (var row :
                      tech.streamfusion.operator.RowDataArrowConverter.read(root, type)) {
                    nativeRows.add(
                        Row.of(
                                row.getInt(0),
                                row.getTimestamp(1, 6).toLocalDateTime(),
                                row.getTimestamp(2, 6).toInstant())
                            .toString());
                  }
                }
                record = fetched.nextRecordFromSplit();
              }
            }
            finished = !fetched.finishedSplits().isEmpty();
            fetched.recycle();
          }
          assertTrue(nativeReader.nativeFilesRead() > 0);
        }
      }
      nativeRows.sort(String::compareTo);
      assertEquals(result, nativeRows);
      twins.add(result);
    }
    assertEquals(twins.get(0), twins.get(1));
  }

  private static void catalog(StreamTableEnvironment sql, String path) {
    sql.executeSql("CREATE CATALOG p WITH ('type'='paimon', 'warehouse'='" + path + "')");
    sql.executeSql("USE CATALOG p");
  }
}

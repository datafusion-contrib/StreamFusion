package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.FileStoreTableFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

/** Compare multiple committed SQL writes against the exact released 1.0.0 reader. */
class LegacyPaimonParityTest {
  @org.junit.jupiter.api.io.TempDir java.nio.file.Path tempDir;

  static Stream<Arguments> modes() {
    List<Arguments> modes = new ArrayList<>();
    for (String format : List.of("parquet", "orc")) {
      for (int buckets : List.of(-1, 2)) {
        modes.add(Arguments.of(format, buckets, "append", "none", false));
        for (String changelog : List.of("none", "input", "lookup", "full-compaction")) {
          modes.add(Arguments.of(format, buckets, "deduplicate", changelog, false));
        }
      }
      modes.add(Arguments.of(format, 2, "deduplicate", "input", true));
      modes.add(Arguments.of(format, 2, "first-row", "lookup", false));
      modes.add(Arguments.of(format, 2, "partial-update", "input", false));
      for (String function :
          List.of(
              "sum", "min", "max", "product", "first_value", "last_value", "last_non_null_value")) {
        modes.add(Arguments.of(format, 2, "aggregation:" + function, "input", false));
      }
    }
    return modes.stream();
  }

  @ParameterizedTest
  @MethodSource("modes")
  void committedRowsMatchStock(
      String format, int buckets, String engine, String changelog, boolean managed)
      throws Exception {
    String aggregate = engine.contains(":") ? engine.substring(engine.indexOf(':') + 1) : "sum";
    engine = engine.split(":")[0];
    List<List<String>> results = new ArrayList<>();
    boolean primaryKey = !engine.equals("append");
    for (boolean accelerated : List.of(false, true)) {
      var warehouse = Files.createTempDirectory(tempDir, "paimon1-parity");
      for (int pass = 0; pass < 2; pass++) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(50);
        var sql = StreamTableEnvironment.create(env);
        sql.getConfig().set("table.exec.sink.upsert-materialize", "NONE");
        sql.executeSql(
            "CREATE CATALOG p WITH ('type'='paimon', 'warehouse'='" + warehouse.toUri() + "')");
        sql.executeSql("USE CATALOG p");
        sql.executeSql(
            "CREATE TABLE IF NOT EXISTS t (id INT NOT NULL, v INT, pt STRING NOT NULL"
                + (primaryKey ? ", PRIMARY KEY (id, pt) NOT ENFORCED" : "")
                + ") PARTITIONED BY (pt) WITH ('bucket'='"
                + buckets
                + "', 'file.format'='"
                + format
                + "', 'sink.parallelism'='2', 'sink.use-managed-memory-allocator'='"
                + managed
                + "', 'write-buffer-size'='1 mb', 'compaction.min.file-num'='2',"
                + " 'compaction.max.file-num'='3', 'changelog-producer'='"
                + changelog
                + "'"
                + (primaryKey ? ", 'merge-engine'='" + engine + "'" : "")
                + (!primaryKey && buckets > 0 ? ", 'bucket-key'='id'" : "")
                + (engine.equals("aggregation")
                    ? ", 'fields.v.aggregate-function'='" + aggregate + "'"
                    : "")
                + (engine.equals("partial-update") ? ", 'partial-update.ignore-delete'='true'" : "")
                + ")");
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
          rows.add(Row.ofKind(RowKind.INSERT, i % 31, i + pass * 1000, "p" + i % 2));
          if (primaryKey && engine.equals("deduplicate") && i % 17 == 0)
            rows.add(Row.ofKind(RowKind.DELETE, i % 31, i + pass * 1000, "p" + i % 2));
        }
        var input =
            env.fromCollection(
                rows,
                Types.ROW_NAMED(
                    new String[] {"id", "v", "pt"}, Types.INT, Types.INT, Types.STRING));
        var schema =
            Schema.newBuilder()
                .column("id", DataTypes.INT().notNull())
                .column("v", DataTypes.INT())
                .column("pt", DataTypes.STRING().notNull())
                .build();
        sql.createTemporaryView(
            "input_rows",
            sql.fromChangelogStream(
                input, schema, primaryKey ? ChangelogMode.all() : ChangelogMode.insertOnly()));
        var plan = accelerated ? NativePlanner.install(sql) : null;
        sql.executeSql("INSERT INTO t SELECT * FROM input_rows").await();
        if (accelerated) {
          assertTrue(plan.substitutions() > 0, plan.explainSummary());
          assertTrue(
              plan.fallbackReasons().stream().noneMatch(r -> r.startsWith("Paimon sink:")),
              plan.explainSummary());
          assertTrue(
              sql.explainSql("INSERT INTO t SELECT * FROM input_rows").contains("NativePaimon"),
              plan.explainSummary());
        }
      }
      var table =
          FileStoreTableFactory.create(
              LocalFileIO.create(), new Path(warehouse.resolve("default.db/t").toUri()));
      List<String> values = new ArrayList<>();
      var read = table.newReadBuilder();
      try (var reader = read.newRead().createReader(read.newScan().plan())) {
        reader.forEachRemaining(
            row ->
                values.add(
                    row.getInt(0)
                        + ":"
                        + (row.isNullAt(1) ? "null" : row.getInt(1))
                        + ":"
                        + row.getString(2)));
      }
      values.sort(String::compareTo);
      results.add(values);
    }
    assertEquals(results.get(0), results.get(1));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void nullablePrimaryKeyInputPreservesNotNullErrors(boolean containsNull) throws Exception {
    java.nio.file.Path warehouse = Files.createTempDirectory("paimon-not-null-error");
    List<String> failures = new ArrayList<>();
    for (boolean nativeSink : new boolean[] {false, true}) {
      String name = nativeSink ? "required_native" : "required_stock";
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
      tableEnv.executeSql(
          "CREATE CATALOG p WITH ('type'='paimon', 'warehouse'='" + warehouse.toUri() + "')");
      tableEnv.executeSql("USE CATALOG p");
      tableEnv.getConfig().set("table.exec.sink.upsert-materialize", "NONE");
      tableEnv.executeSql(
          "CREATE TABLE "
              + name
              + " (id BIGINT, label STRING, PRIMARY KEY (id) NOT ENFORCED) "
              + "WITH ('bucket' = '2', 'local-merge-buffer-size' = '1 mb')");
      var rows =
          tech.streamfusion.compat.FlinkTestSources.fromData(
              env,
              Types.ROW_NAMED(new String[] {"id", "label"}, Types.LONG, Types.STRING),
              Row.of(1L, "first"),
              Row.of(containsNull ? null : 2L, "second"));
      tableEnv.createTemporaryView(
          "required_input",
          tableEnv.fromDataStream(
              rows, Schema.newBuilder().column("id", "BIGINT").column("label", "STRING").build()));
      var scan = nativeSink ? NativePlanner.install(tableEnv) : null;
      String sql = "INSERT INTO " + name + " SELECT id AS renamed, label FROM required_input";
      if (nativeSink) {
        String plan =
            tableEnv.explainSql(sql, org.apache.flink.table.api.ExplainDetail.JSON_EXECUTION_PLAN);
        assertTrue(plan.contains("native-paimon-not-null-enforcer"), () -> plan);
        assertTrue(plan.contains("native-paimon-bucket-route"), () -> plan);
      }
      if (containsNull) {
        Throwable failure = assertThrows(Throwable.class, () -> tableEnv.executeSql(sql).await());
        while (failure.getCause() != null) failure = failure.getCause();
        failures.add(failure.getMessage());
      } else {
        tableEnv.executeSql(sql).await();
        var table =
            FileStoreTableFactory.create(
                LocalFileIO.create(), new Path(warehouse.resolve("default.db/" + name).toUri()));
        var read = table.newReadBuilder();
        List<String> values = new ArrayList<>();
        try (var reader = read.newRead().createReader(read.newScan().plan())) {
          reader.forEachRemaining(row -> values.add(row.getLong(0) + "|" + row.getString(1)));
        }
        values.sort(String::compareTo);
        assertEquals(List.of("1|first", "2|second"), values);
      }
      if (nativeSink) {
        assertTrue(
            scan.fallbackReasons().stream().noneMatch(reason -> reason.startsWith("paimon sink:")),
            scan::explainSummary);
      }
    }
    if (containsNull) {
      assertEquals(failures.get(0), failures.get(1));
      assertTrue(failures.get(0).contains("Column 'id' is NOT NULL"));
    }
  }

}

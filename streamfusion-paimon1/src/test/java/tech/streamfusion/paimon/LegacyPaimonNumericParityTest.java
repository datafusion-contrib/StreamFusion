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
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.NativePlanner;

class LegacyPaimonNumericParityTest {
  @TempDir java.nio.file.Path tempDir;

  @ParameterizedTest
  @CsvSource({"parquet,sum", "parquet,product", "orc,sum", "orc,product"})
  void integerOverflowAndEmptyRetractionsMatchReleasedWriter(String format, String function)
      throws Exception {
    List<List<String>> twins = new ArrayList<>();
    for (boolean accelerated : List.of(false, true)) {
      var warehouse = Files.createTempDirectory(tempDir, "numeric");
      var env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      var sql = StreamTableEnvironment.create(env);
      sql.getConfig().set("table.exec.sink.upsert-materialize", "NONE");
      String catalog =
          "CREATE CATALOG p WITH ('type'='paimon', 'warehouse'='" + warehouse.toUri() + "')";
      sql.executeSql(catalog);
      sql.executeSql("USE CATALOG p");
      sql.executeSql(
          "CREATE TABLE t (id INT NOT NULL, b TINYINT, s SMALLINT, i INT, l BIGINT, "
              + "PRIMARY KEY(id) NOT ENFORCED) WITH ('bucket'='1', 'file.format'='"
              + format
              + "', 'merge-engine'='aggregation', 'changelog-producer'='input', "
              + "'fields.default-aggregate-function'='"
              + function
              + "')");
      int multiply = function.equals("product") ? 2 : 1;
      int divisor = function.equals("product") ? -1 : 1;
      List<Row> rows =
          List.of(
              Row.of(1, Byte.MAX_VALUE, Short.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE),
              Row.of(1, (byte) multiply, (short) multiply, multiply, (long) multiply),
              Row.of(2, Byte.MIN_VALUE, Short.MIN_VALUE, Integer.MIN_VALUE, Long.MIN_VALUE),
              Row.ofKind(
                  RowKind.DELETE, 2, (byte) divisor, (short) divisor, divisor, (long) divisor),
              Row.of(3, null, null, null, null),
              Row.ofKind(RowKind.DELETE, 3, (byte) 5, (short) 5, 5, 5L));
      var input =
          env.fromCollection(
              rows,
              Types.ROW_NAMED(
                  new String[] {"id", "b", "s", "i", "l"},
                  Types.INT,
                  Types.BYTE,
                  Types.SHORT,
                  Types.INT,
                  Types.LONG));
      sql.createTemporaryView(
          "input_rows",
          sql.fromChangelogStream(
              input,
              Schema.newBuilder()
                  .column("id", DataTypes.INT().notNull())
                  .column("b", DataTypes.TINYINT())
                  .column("s", DataTypes.SMALLINT())
                  .column("i", DataTypes.INT())
                  .column("l", DataTypes.BIGINT())
                  .build(),
              ChangelogMode.all()));
      if (accelerated) {
        NativePlanner.install(sql);
        assertTrue(
            sql.explainSql("INSERT INTO t SELECT * FROM input_rows").contains("NativePaimon"));
      }
      sql.executeSql("INSERT INTO t SELECT * FROM input_rows").await();
      var readEnv = StreamExecutionEnvironment.getExecutionEnvironment();
      readEnv.setParallelism(1);
      readEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);
      var read = StreamTableEnvironment.create(readEnv);
      read.executeSql(catalog);
      read.executeSql("USE CATALOG p");
      List<String> result = new ArrayList<>();
      try (var values = read.executeSql("SELECT * FROM t").collect()) {
        values.forEachRemaining(row -> result.add(row.toString()));
      }
      result.sort(String::compareTo);
      assertEquals(3, result.size());
      twins.add(result);
    }
    assertEquals(twins.get(0), twins.get(1));
  }
}

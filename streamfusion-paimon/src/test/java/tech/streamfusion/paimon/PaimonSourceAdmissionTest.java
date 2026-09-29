package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class PaimonSourceAdmissionTest {
  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "'consumer-id'='reader', 'consumer.expiration-time'='1 d'|exactly-once consumer retention",
        "'consumer-id'='reader', 'consumer.mode'='at-least-once', 'consumer.expiration-time'='1 d',"
            + " 'consumer.ignore-progress'='true'|consumer option",
        "'file.format'='avro'|file.format",
        "'source.checkpoint-align.enabled'='true'|checkpoint-align",
        "'scan.ignore-corrupt-file'='true'|ignore-corrupt-file",
        "'data-file.thin-mode'='true'|thin-mode"
      })
  void unsupportedOptionsRetainStockSource(String options, String reason) throws Exception {
    var sql = environment();
    sql.executeSql(
        "CREATE TABLE t (id INT NOT NULL, v STRING, PRIMARY KEY(id) NOT ENFORCED) WITH"
            + " ('bucket'='2', 'changelog-producer'='input', "
            + options
            + ")");
    String plan = NativePlanner.explain(sql, "SELECT * FROM t");
    assertFalse(plan.contains("StreamPhysicalNativePaimonSource"), plan);
    assertTrue(plan.contains(reason), plan);
  }

  @Test
  void missingConsumerExpirationRetainsHostValidation() throws Exception {
    var sql = environment();
    sql.executeSql(
        "CREATE TABLE t (id INT) WITH ('bucket'='-1', 'consumer-id'='reader',"
            + " 'consumer.mode'='at-least-once')");
    var failure =
        assertThrows(
            IllegalArgumentException.class, () -> NativePlanner.explain(sql, "SELECT * FROM t"));
    assertTrue(failure.getMessage().contains("consumer.expiration-time"), failure.getMessage());
  }

  @Test
  void primaryKeyWithoutChangelogAndNanosecondTimestampRemainStock() throws Exception {
    var sql = environment();
    sql.executeSql(
        "CREATE TABLE t (id INT NOT NULL, v STRING, PRIMARY KEY(id) NOT ENFORCED) WITH"
            + " ('bucket'='2')");
    assertTrue(
        NativePlanner.explain(sql, "SELECT * FROM t").contains("requires a changelog producer"));
    sql.executeSql("CREATE TABLE ns (v TIMESTAMP(9))");
    assertFalse(NativePlanner.explain(sql, "SELECT * FROM ns").contains("NativePaimonSource"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "INTERVAL '1' SECOND",
        "INTERVAL '1' MONTH",
        "INTERVAL '1' YEAR",
        "INTERVAL '1-1' YEAR TO MONTH"
      })
  void watermarkProjectionAndFiltersAreAdmitted(String interval) throws Exception {
    var sql = environment();
    sql.executeSql(
        "CREATE TABLE t (id INT, ts TIMESTAMP(3), WATERMARK FOR ts AS ts - " + interval + ")");
    String plan = NativePlanner.explain(sql, "SELECT ts, id FROM t WHERE id > 10");
    assertTrue(plan.contains("NativePaimonSource"), plan);
  }

  @Test
  void nestedPruningStaysOnJava() throws Exception {
    var sql = environment();
    sql.executeSql("CREATE TABLE t (id INT, nested ROW<a INT, b STRING>)");
    String plan = NativePlanner.explain(sql, "SELECT nested.a FROM t");
    assertFalse(plan.contains("NativePaimonSource"), plan);
  }

  private static StreamTableEnvironment environment() throws Exception {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(1000);
    var sql = StreamTableEnvironment.create(env);
    sql.executeSql(
        "CREATE CATALOG p WITH ('type'='paimon', 'warehouse'='"
            + Files.createTempDirectory("paimon-source-plan").toUri()
            + "', 'table-default.file.format'='"
            + PaimonTestTables.fileFormat()
            + "')");
    sql.executeSql("USE CATALOG p");
    return sql;
  }
}

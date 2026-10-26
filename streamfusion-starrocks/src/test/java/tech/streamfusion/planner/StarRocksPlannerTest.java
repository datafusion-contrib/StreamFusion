package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import com.starrocks.connector.flink.table.StarRocksDataType;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

/** Declines that keep the stock StarRocks writer, decided before or instead of contacting it. */
class StarRocksPlannerTest {
  private static final String UNREACHABLE =
      "'connector'='starrocks', 'jdbc-url'='jdbc:mysql://127.0.0.1:1', 'load-url'='127.0.0.1:1',"
          + " 'database-name'='db', 'table-name'='t', 'username'='root', 'password'=''";

  @Test
  void columnTypesWithoutAStockSpellingPortKeepTheStockSink() {
    List<String> reasons = plan("id BIGINT, tags ARRAY<INT>", "", "SELECT id, ARRAY[1] FROM src");
    assertTrue(
        reasons.stream()
            .anyMatch(
                reason ->
                    reason.contains(
                        "starrocks sink: column tags of type ARRAY<INT> is not natively encoded")),
        reasons.toString());
  }

  @Test
  void nonTransactionalWritersKeepTheStockSink() {
    List<String> reasons =
        plan("id BIGINT, name STRING", ", 'sink.version'='V1'", "SELECT id, name FROM src");
    assertTrue(
        reasons.contains(
            "starrocks sink: sink.version=V1 keeps the connector's non-transactional writer"),
        reasons.toString());
  }

  @Test
  void mergeCommitKeepsTheStockSink() {
    List<String> reasons =
        plan(
            "id BIGINT, name STRING",
            ", 'sink.properties.enable_merge_commit'='true',"
                + " 'sink.properties.merge_commit_interval_ms'='1000'",
            "SELECT id, name FROM src");
    assertTrue(
        reasons.contains(
            "starrocks sink: merge commit (sink.properties.enable_merge_commit) is not natively"
                + " verified"),
        reasons.toString());
  }

  @Test
  void multiTableTransactionsKeepTheStockSink() {
    List<String> reasons =
        plan(
            "id BIGINT, name STRING",
            ", 'sink.transaction.multi-table.enabled'='true'",
            "SELECT id, name FROM src");
    assertTrue(
        reasons.contains(
            "starrocks sink: sink.transaction.multi-table.enabled needs the connector's"
                + " partition-aware writer"),
        reasons.toString());
  }

  @Test
  void lengthEnforcementKeepsTheStockSink() {
    var environment = environment();
    environment
        .getConfig()
        .set(
            ExecutionConfigOptions.TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER,
            ExecutionConfigOptions.TypeLengthEnforcer.TRIM_PAD);
    List<String> reasons =
        plan(environment, "id BIGINT, code VARCHAR(3)", "", "SELECT id, name FROM src");
    assertTrue(
        reasons.stream()
            .anyMatch(
                reason -> reason.startsWith("starrocks sink: target column code uses VARCHAR(3)")),
        reasons.toString());
  }

  @Test
  void unreachableTablesKeepTheStockSinkAndItsOwnError() {
    List<String> reasons = plan("id BIGINT, name STRING", "", "SELECT id, name FROM src");
    assertTrue(
        reasons.stream()
            .anyMatch(
                reason ->
                    reason.startsWith("starrocks sink: the connector could not prepare the table")),
        reasons.toString());
  }

  @Test
  void dateSpellingProbeRejectsZonesThatSkippedADay() {
    TimeZone previous = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("Pacific/Apia"));
    try {
      List<String> reasons = plan("id BIGINT, d DATE", "", "SELECT id, CURRENT_DATE FROM src");
      assertTrue(
          reasons.contains(
              "starrocks sink: DATE spelling differs from this JVM's SimpleDateFormat (default"
                  + " locale, calendar or time zone)"),
          reasons.toString());
    } finally {
      TimeZone.setDefault(previous);
    }
  }

  @Test
  void disablingTheAdapterKeepsTheStockSinkWithoutAReason() {
    System.setProperty("streamfusion.starrocks.enabled", "false");
    try {
      List<String> reasons = plan("id BIGINT, name STRING", "", "SELECT id, name FROM src");
      assertTrue(
          reasons.stream().noneMatch(reason -> reason.contains("starrocks")), reasons.toString());
    } finally {
      System.clearProperty("streamfusion.starrocks.enabled");
    }
  }

  @Test
  void textTargetsThatTheConnectorReparsesAsJsonKeepTheStockSink() {
    StarRocksSinkOptions options = options();
    RowType type =
        RowType.of(
            new LogicalType[] {new IntType(), new VarCharType(VarCharType.MAX_LENGTH)},
            new String[] {"id", "doc"});
    String[] names = {"id", "doc"};
    assertNull(
        StarRocksTables.targetReason(
            options,
            names,
            type,
            Map.of("id", StarRocksDataType.INT, "doc", StarRocksDataType.STRING)));
    assertEquals(
        "text column doc targets a StarRocks JSON column, which the connector re-serializes"
            + " through fastjson",
        StarRocksTables.targetReason(
            options,
            names,
            type,
            Map.of("id", StarRocksDataType.INT, "doc", StarRocksDataType.JSON)));
    assertEquals(
        "text column doc targets a StarRocks unrecognized or differently cased column, which the"
            + " connector re-serializes through fastjson",
        StarRocksTables.targetReason(
            options,
            names,
            type,
            Map.of("id", StarRocksDataType.INT, "DOC", StarRocksDataType.STRING)));
  }

  @Test
  void anOpColumnCollidesWithTheUpsertFlagOnlyForPrimaryKeyTables() {
    StarRocksSinkOptions options = options();
    RowType type = RowType.of(new LogicalType[] {new IntType()}, new String[] {"__op"});
    Map<String, StarRocksDataType> targets = Map.of("__op", StarRocksDataType.INT);
    assertNull(StarRocksTables.targetReason(options, new String[] {"__op"}, type, targets));
    options.enableUpsertDelete();
    assertEquals(
        "column __op collides with the connector's upsert flag",
        StarRocksTables.targetReason(options, new String[] {"__op"}, type, targets));
  }

  private static List<String> plan(String sinkSchema, String extraOptions, String select) {
    return plan(environment(), sinkSchema, extraOptions, select);
  }

  private static List<String> plan(
      StreamTableEnvironment tables, String sinkSchema, String extraOptions, String select) {
    var scan = NativePlanner.install(tables);
    tables.executeSql(
        "CREATE TABLE src (id BIGINT, name STRING) WITH ('connector'='datagen',"
            + " 'number-of-rows'='1')");
    tables.executeSql(
        "CREATE TABLE dst (" + sinkSchema + ") WITH (" + UNREACHABLE + extraOptions + ")");
    // Translating the kept stock writer validates the table over JDBC, which fails here.
    var stockTranslation =
        assertThrows(
            IllegalArgumentException.class, () -> tables.explainSql("INSERT INTO dst " + select));
    assertTrue(
        stockTranslation.getMessage().contains("Failed to get table schema info from StarRocks"),
        stockTranslation.toString());
    return scan.fallbackReasons();
  }

  private static StreamTableEnvironment environment() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    return StreamTableEnvironment.create(env);
  }

  private static StarRocksSinkOptions options() {
    return StarRocksSinkOptions.builder()
        .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9030")
        .withProperty("load-url", "127.0.0.1:8030")
        .withProperty("database-name", "db")
        .withProperty("table-name", "t")
        .withProperty("username", "root")
        .withProperty("password", "")
        .build();
  }
}

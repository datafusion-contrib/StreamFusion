package tech.streamfusion.starrocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

/**
 * Complete-job comparison against a real StarRocks server: a rowwise datagen source, a projection
 * and the StarRocks sink, in stock Flink, in StreamFusion with the stock writer (the previous
 * behavior, which converts Arrow back to rows before the writer) and with the native encoder. Every
 * perimeter conversion and the server's ingestion stay inside the measured job.
 */
@EnabledIfEnvironmentVariable(named = "SF_STARROCKS_BENCH", matches = "true")
class StarRocksConnectorBenchmark {
  private static final String INSERT =
      "INSERT INTO dst SELECT id, CAST(MOD(id, 1000) AS INT), CONCAT('customer-', CAST(id AS"
          + " STRING)), CAST(MOD(id, 100000) AS DECIMAL(18, 2)), CAST(id AS DOUBLE) / 3, event_day,"
          + " event_time, MOD(id, 2) = 0, note FROM src";

  @Test
  @Timeout(7200)
  void compareStockPreviousAndNativeWriters() throws Exception {
    assertEquals(
        "release",
        System.getProperty("sf.starrocks.benchmark.profile"),
        "Use the bench Maven profile");
    int warmups = Integer.getInteger("sf.starrocks.benchmark.warmups", 1);
    int trials = Integer.getInteger("sf.starrocks.benchmark.trials", 3);
    int parallelism = Integer.getInteger("sf.starrocks.benchmark.parallelism", 2);
    String sizes = System.getProperty("sf.starrocks.benchmark.rows", "1000000,5000000");
    String formats = System.getProperty("sf.starrocks.benchmark.formats", "csv,json");
    Path output =
        Path.of(System.getProperty("sf.starrocks.benchmark.output", "target/starrocks-benchmark"));
    Files.createDirectories(output.resolve("plans"));
    String database = "sf_bench_" + UUID.randomUUID().toString().replace("-", "");
    var lines = new ArrayList<String>();
    lines.add("rows,format,parallelism,trial,mode,milliseconds");
    try (Connection connection =
        DriverManager.getConnection("jdbc:mysql://127.0.0.1:9030/?useSSL=false", "root", "")) {
      execute(connection, "CREATE DATABASE " + database);
      try {
        for (String size : sizes.split(","))
          for (String format : formats.split(",")) {
            long rows = Long.parseLong(size);
            for (int trial = -warmups; trial < trials; trial++) {
              var modes = new ArrayList<>(List.of("stock", "previous", "native"));
              Collections.rotate(modes, trial + warmups);
              for (String mode : modes) {
                String table = "output_" + UUID.randomUUID().toString().replace("-", "");
                execute(
                    connection,
                    "CREATE TABLE "
                        + database
                        + "."
                        + table
                        + " (id BIGINT, quantity INT, name STRING, amount DECIMAL(18, 2), score"
                        + " DOUBLE, event_day DATE, event_time DATETIME, active BOOLEAN, note STRING)"
                        + " DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4"
                        + " PROPERTIES('replication_num'='1')");
                System.setProperty("streamfusion.starrocks.enabled", mode.equals("native") + "");
                var environment = StreamExecutionEnvironment.getExecutionEnvironment();
                environment.setParallelism(parallelism);
                var tables = StreamTableEnvironment.create(environment);
                if (!mode.equals("stock")) NativePlanner.install(tables);
                tables.executeSql(
                    "CREATE TABLE src (id BIGINT, event_day DATE, event_time TIMESTAMP(3), note"
                        + " STRING) WITH ('connector'='datagen', 'rows-per-second'='1000000000',"
                        + " 'fields.id.kind'='sequence', 'fields.id.start'='1', 'fields.id.end'='"
                        + rows
                        + "', 'fields.note.length'='48')");
                tables.executeSql(
                    "CREATE TABLE dst (id BIGINT, quantity INT, name STRING, amount DECIMAL(18, 2),"
                        + " score DOUBLE, event_day DATE, event_time TIMESTAMP(3), active BOOLEAN,"
                        + " note STRING) WITH ('connector'='starrocks',"
                        + " 'jdbc-url'='jdbc:mysql://127.0.0.1:9030', 'load-url'='127.0.0.1:8030',"
                        + " 'database-name'='"
                        + database
                        + "', 'table-name'='"
                        + table
                        + "', 'username'='root', 'password'='', 'sink.properties.format'='"
                        + format
                        + "')");
                String plan = tables.explainSql(INSERT);
                Files.writeString(output.resolve("plans/" + mode + "-" + format + ".txt"), plan);
                assertEquals(mode.equals("native"), plan.contains("NativeStarRocksSink"), plan);
                assertEquals(mode.equals("previous"), plan.contains("ArrowToRowData"), plan);
                if (!mode.equals("stock")) assertTrue(plan.contains("NativeCalc"), plan);
                else assertFalse(plan.contains("Native"), plan);

                long started = System.nanoTime();
                tables.executeSql(INSERT).await();
                long millis = (System.nanoTime() - started) / 1_000_000;

                try (var statement = connection.createStatement();
                    var result =
                        statement.executeQuery(
                            "SELECT COUNT(*), SUM(id), SUM(quantity), SUM(active) FROM "
                                + database
                                + "."
                                + table)) {
                  assertTrue(result.next());
                  assertEquals(rows, result.getLong(1));
                  assertEquals(rows * (rows + 1) / 2, result.getLong(2));
                  long quantities = 0;
                  for (long id = 1; id <= rows; id++) quantities += id % 1000;
                  assertEquals(quantities, result.getLong(3));
                  assertEquals(rows / 2, result.getLong(4));
                }
                execute(connection, "DROP TABLE " + database + "." + table + " FORCE");
                if (trial >= 0)
                  lines.add(
                      rows
                          + ","
                          + format
                          + ","
                          + parallelism
                          + ","
                          + trial
                          + ","
                          + mode
                          + ","
                          + millis);
                System.out.println(
                    "StarRocks benchmark "
                        + rows
                        + " "
                        + format
                        + " trial "
                        + trial
                        + " "
                        + mode
                        + ": "
                        + millis
                        + " ms");
              }
            }
          }
      } finally {
        System.clearProperty("streamfusion.starrocks.enabled");
        execute(connection, "DROP DATABASE " + database + " FORCE");
      }
    }
    Files.write(output.resolve("connector.csv"), lines);
    lines.forEach(System.out::println);
  }

  private static void execute(Connection connection, String sql) throws Exception {
    try (var statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }
}

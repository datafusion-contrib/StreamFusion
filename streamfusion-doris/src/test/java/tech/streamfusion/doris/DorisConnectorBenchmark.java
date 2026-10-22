package tech.streamfusion.doris;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

/**
 * Complete Doris reads, SQL execution, Stream Load and visibility, including all perimeter costs.
 */
@EnabledIfEnvironmentVariable(named = "SF_DORIS_BENCH", matches = "true")
class DorisConnectorBenchmark {
  private final Path directory =
      Path.of(System.getProperty("sf.doris.benchmark.output", "target/doris-benchmark"));
  private Connection connection;
  private final boolean complex = Boolean.getBoolean("sf.doris.benchmark.complex");
  private final boolean maps = Boolean.getBoolean("sf.doris.benchmark.maps");
  private final boolean flight =
      Boolean.parseBoolean(System.getProperty("sf.doris.benchmark.flight", "true"));
  private final boolean legacy = Boolean.getBoolean("sf.doris.benchmark.legacy");
  private String database;

  @Test
  @Timeout(1800)
  void compareStockPreviousAndArrowTransport() throws Exception {
    assertEquals(
        "release", System.getProperty("sf.doris.benchmark.profile"), "Use the bench Maven profile");
    int warmups = Integer.getInteger("sf.doris.benchmark.warmups", 1);
    int trials = Integer.getInteger("sf.doris.benchmark.trials", 3);
    String sizes = System.getProperty("sf.doris.benchmark.rows", "100000,1000000");
    int parallelism = Integer.getInteger("sf.doris.benchmark.parallelism", 2);
    Files.createDirectories(directory.resolve("plans"));
    String previous = System.getProperty("streamfusion.doris.enabled");
    database = "sf_bench_" + UUID.randomUUID().toString().replace("-", "");
    try (Connection opened =
        DriverManager.getConnection("jdbc:mysql://127.0.0.1:9030/?useSSL=false", "root", "")) {
      connection = opened;
      sql("CREATE DATABASE " + database);
      try {
        var measurements = new ArrayList<String>();
        measurements.add("rows,parallelism,trial,mode,milliseconds");
        for (String size : sizes.split(",")) {
          int rows = Integer.parseInt(size);
          assertTrue(rows > 0 && rows % 1000 == 0);
          sql("CREATE TABLE " + database + ".input " + dorisSchema());
          sql(
              "INSERT INTO "
                  + database
                  + ".input SELECT number, CAST(number % 1000 AS INT),"
                  + " IF(number % 7 = 0, NULL, CONCAT('payload-☃-',CAST(number AS STRING))),"
                  + " IF(number % 11 = 0, NULL, CAST(number / 100.0 AS DECIMAL(18,2)))"
                  + (complex
                      ? ", CAST('2020-07-01 12:34:56.123456' AS DATETIMEV2(6)),"
                          + "ARRAY(CAST(number % 10 AS INT),NULL),"
                          + (maps ? "MAP('a',CAST(number % 5 AS INT),'b',NULL)," : "")
                          + "NAMED_STRUCT('x',CAST(number % 3 AS INT),'t','r-☃')"
                      : "")
                  + " FROM numbers('number'='"
                  + rows
                  + "')");
          List<String> expected =
              result(
                  "SELECT COUNT(*), SUM(id+1), SUM(n+1), COUNT(name), SUM(amount),"
                      + " SUM(CRC32(COALESCE(name,'')))"
                      + (complex
                          ? ",SUM(UNIX_TIMESTAMP(CONVERT_TZ(ts,'UTC','"
                              + java.time.ZoneId.systemDefault().getId()
                              + "'))),SUM(a[1]),COUNT(a[2]),"
                              + (maps ? "SUM(m['a']),COUNT(m['b'])," : "")
                              + "SUM(r.x),COUNT(r.t)"
                          : "")
                      + " FROM "
                      + database
                      + ".input WHERE id % 2 = 0");
          for (int trial = -warmups; trial < trials; trial++) {
            var modes = new ArrayList<>(List.of("stock", "previous", "arrow"));
            java.util.Collections.rotate(modes, trial + warmups);
            for (String mode : modes) {
              System.setProperty(
                  "streamfusion.doris.enabled", Boolean.toString(mode.equals("arrow")));
              String output = "output_" + UUID.randomUUID().toString().replace("-", "");
              sql("CREATE TABLE " + database + "." + output + " " + dorisSchema());
              var env = StreamExecutionEnvironment.getExecutionEnvironment();
              env.setParallelism(parallelism);
              var tables = StreamTableEnvironment.create(env);
              if (!mode.equals("stock")) NativePlanner.install(tables);
              tables.executeSql(ddl("src", "input", ""));
              tables.executeSql(
                  ddl(
                      "dst",
                      output,
                      ", 'sink.properties.format'='arrow', 'sink.properties.compress_type'='',"
                          + " 'sink.enable-delete'='false', 'sink.enable-2pc'='true',"
                          + " 'sink.label-prefix'='"
                          + UUID.randomUUID()
                          + "'"));
              String insert =
                  "INSERT INTO dst SELECT id + 1, n + 1, name, amount"
                      + (complex ? ",ts,a" + (maps ? ",m" : "") + ",r" : "")
                      + " FROM src WHERE MOD(id,2)=0";
              String plan = tables.explainSql(insert);
              boolean arrow = mode.equals("arrow");
              assertEquals(arrow, plan.contains("NativeDorisSource"), plan);
              assertEquals(arrow, plan.contains("NativeDorisSink"), plan);
              assertEquals(!mode.equals("stock"), plan.contains("NativeCalc"), plan);
              if (arrow) {
                assertFalse(plan.contains("RowDataToArrow"), plan);
                assertFalse(plan.contains("ArrowToRowData"), plan);
              } else if (mode.equals("previous")) {
                assertTrue(plan.contains("RowDataToArrow"), plan);
                assertTrue(plan.contains("ArrowToRowData"), plan);
              }
              Files.writeString(
                  directory.resolve("plans/" + rows + "-" + trial + "-" + mode + ".txt"), plan);
              long started = System.nanoTime();
              tables.executeSql(insert).await();
              long deadline = System.nanoTime() + java.time.Duration.ofSeconds(30).toNanos();
              while (!result("SELECT COUNT(*) FROM " + database + "." + output)
                  .equals(List.of(Integer.toString(rows / 2)))) {
                assertTrue(System.nanoTime() < deadline, "Doris output did not become visible");
                Thread.sleep(10);
              }
              double milliseconds = (System.nanoTime() - started) / 1_000_000.0;
              assertEquals(
                  expected,
                  result(
                      "SELECT COUNT(*), SUM(id), SUM(n), COUNT(name), SUM(amount),"
                          + " SUM(CRC32(COALESCE(name,'')))"
                          + (complex
                              ? ",SUM(UNIX_TIMESTAMP(ts)),SUM(a[1]),COUNT(a[2]),"
                                  + (maps ? "SUM(m['a']),COUNT(m['b'])," : "")
                                  + "SUM(r.x),COUNT(r.t)"
                              : "")
                          + " FROM "
                          + database
                          + "."
                          + output));
              String measured =
                  rows + "," + parallelism + "," + trial + "," + mode + "," + milliseconds;
              System.out.println("DORIS_BENCH " + measured);
              measurements.add(measured);
              Files.write(directory.resolve("trials.csv"), measurements);
              sql("DROP TABLE " + database + "." + output + " FORCE");
            }
          }
          sql("DROP TABLE " + database + ".input FORCE");
        }
        Files.writeString(
            directory.resolve("configuration.txt"),
            "Doris="
                + result("SELECT @@version_comment").get(0)
                + "\nconnector=26.3.0\nflink=2.2.1\n"
                + "complex="
                + complex
                + "\nmaps="
                + maps
                + "\nflight="
                + flight
                + "\nlegacy="
                + legacy
                + "\n"
                + "java="
                + System.getProperty("java.version")
                + "\nos="
                + System.getProperty("os.name")
                + " "
                + System.getProperty("os.arch")
                + "\nclient-timezone="
                + java.time.ZoneId.systemDefault()
                + "\nprocessors="
                + Runtime.getRuntime().availableProcessors()
                + "\nheap-bytes="
                + Runtime.getRuntime().maxMemory()
                + "\nnative-version="
                + tech.streamfusion.Native.version()
                + "\nwarmups="
                + warmups
                + "\ntrials="
                + trials
                + "\nrows="
                + sizes
                + "\nparallelism="
                + parallelism
                + "\n"
                + "replication=1\n"
                + "buckets=4\n"
                + "format=arrow\n"
                + "http-compression=none\n"
                + "logging=WARN; HTTP wire debug disabled for all modes\n"
                + "row-batch-null-patterns=name%7,amount%11\n"
                + "boundary=SQL submission through queryable Doris output; includes planning, JNI,"
                + " codecs, I/O, startup and publish wait\n");
      } finally {
        sql("DROP DATABASE " + database + " FORCE");
      }
    } finally {
      if (previous == null) System.clearProperty("streamfusion.doris.enabled");
      else System.setProperty("streamfusion.doris.enabled", previous);
    }
  }

  private String dorisSchema() {
    return "(id BIGINT, n INT, name STRING, amount DECIMAL(18,2)"
        + (complex
            ? ", ts DATETIMEV2(6), a ARRAY<INT>"
                + (maps ? ", m MAP<STRING,INT>" : "")
                + ", r STRUCT<x:INT,t:STRING>"
            : "")
        + ") DUPLICATE KEY(id)"
        + " DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES('replication_num'='1')";
  }

  private String ddl(String name, String table, String extra) {
    return "CREATE TABLE "
        + name
        + " (id BIGINT, n INT, name STRING, amount DECIMAL(18,2)"
        + (complex
            ? ", ts TIMESTAMP(6), a ARRAY<INT>"
                + (maps ? ", m MAP<STRING NOT NULL,INT>" : "")
                + ", r ROW<x INT,t STRING>"
            : "")
        + ") WITH ("
        + "'connector'='doris','fenodes'='127.0.0.1:8030','table.identifier'='"
        + database
        + "."
        + table
        + "','username'='root','password'='','source.use-flight-sql'='"
        + flight
        + "','source.use-old-api'='"
        + legacy
        + "','source.flight-sql-port'='9611'"
        + extra
        + ")";
  }

  private List<String> result(String query) throws Exception {
    try (var statement = connection.createStatement();
        var result = statement.executeQuery(query)) {
      assertTrue(result.next());
      var values = new ArrayList<String>();
      for (int i = 1; i <= result.getMetaData().getColumnCount(); i++)
        values.add(result.getString(i));
      return values;
    }
  }

  private void sql(String query) throws Exception {
    try (var statement = connection.createStatement()) {
      statement.execute(query);
    }
  }
}

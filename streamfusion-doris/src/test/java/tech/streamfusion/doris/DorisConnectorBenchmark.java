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
import org.apache.flink.table.api.config.ExecutionConfigOptions;
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
  private final boolean materialize = Boolean.getBoolean("sf.doris.benchmark.materialize");
  private final ExecutionConfigOptions.SinkUpsertMaterializeStrategy strategy =
      ExecutionConfigOptions.SinkUpsertMaterializeStrategy.valueOf(
          System.getProperty("sf.doris.benchmark.strategy", "LEGACY"));
  private final int cardinality = Integer.getInteger("sf.doris.benchmark.cardinality", 1000);
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
    assertFalse(materialize && maps, "Materialization benchmark uses scalar or array/row payloads");
    int warmups = Integer.getInteger("sf.doris.benchmark.warmups", 1);
    int trials = Integer.getInteger("sf.doris.benchmark.trials", 3);
    String sizes = System.getProperty("sf.doris.benchmark.rows", "100000,1000000");
    int parallelism = Integer.getInteger("sf.doris.benchmark.parallelism", 2);
    Files.createDirectories(directory.resolve("plans"));
    String previous = System.getProperty("streamfusion.doris.enabled");
    String previousSink = System.getProperty("streamfusion.operator.dorisSink.enabled");
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
          sql("CREATE TABLE " + database + ".input " + dorisSchema(false));
          sql(
              "INSERT INTO "
                  + database
                  + ".input SELECT "
                  + (materialize ? "number % " + cardinality : "number")
                  + ", CAST(number % 1000 AS INT),"
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
          if (materialize)
            expected =
                materializedResult(
                    "(SELECT "
                        + materializedProjection(false)
                        + " FROM "
                        + database
                        + ".input GROUP BY id) expected");
          for (int trial = -warmups; trial < trials; trial++) {
            var modes = new ArrayList<>(List.of("stock", "previous", "arrow"));
            java.util.Collections.rotate(modes, trial + warmups);
            for (String mode : modes) {
              try (var metrics = new DorisExecutionMetrics()) {
                System.setProperty(
                    "streamfusion.doris.enabled",
                    Boolean.toString(materialize || mode.equals("arrow")));
                if (materialize)
                  System.setProperty(
                      "streamfusion.operator.dorisSink.enabled",
                      Boolean.toString(mode.equals("arrow")));
                String output = "output_" + UUID.randomUUID().toString().replace("-", "");
                sql("CREATE TABLE " + database + "." + output + " " + dorisSchema(true));
                var env =
                    materialize
                        ? metrics.environment(parallelism)
                        : StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(parallelism);
                var tables = StreamTableEnvironment.create(env);
                if (materialize) {
                  tables.getConfig().set("table.exec.sink.upsert-materialize", "FORCE");
                  tables
                      .getConfig()
                      .set(
                          ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE_STRATEGY,
                          strategy);
                }
                var scan = mode.equals("stock") ? null : NativePlanner.install(tables);
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
                if (materialize)
                  insert =
                      "INSERT INTO dst SELECT "
                          + materializedProjection(true)
                          + " FROM src GROUP BY id";
                String plan = tables.explainSql(insert);
                if (scan != null)
                  Files.writeString(directory.resolve("native-summary.txt"), scan.explainSummary());
                if (materialize) assertTrue(plan.contains("upsertMaterialize=[true]"), plan);
                boolean arrow = mode.equals("arrow");
                assertEquals(
                    arrow || materialize && mode.equals("previous"),
                    plan.contains("NativeDorisSource"),
                    plan);
                assertEquals(arrow, plan.contains("NativeDorisSink"), plan);
                assertEquals(!mode.equals("stock"), plan.contains("NativeCalc"), plan);
                if (arrow) {
                  assertFalse(plan.contains("RowDataToArrow"), plan);
                  assertFalse(plan.contains("ArrowToRowData"), plan);
                } else if (mode.equals("previous")) {
                  assertEquals(!materialize, plan.contains("RowDataToArrow"), plan);
                  assertTrue(plan.contains("ArrowToRowData"), plan);
                }
                Files.writeString(
                    directory.resolve("plans/" + rows + "-" + trial + "-" + mode + ".txt"), plan);
                long started = System.nanoTime();
                var execution = tables.executeSql(insert);
                execution.await();
                long deadline = System.nanoTime() + java.time.Duration.ofSeconds(30).toNanos();
                while (!result("SELECT COUNT(*) FROM " + database + "." + output)
                    .equals(List.of(Integer.toString(materialize ? cardinality : rows / 2)))) {
                  assertTrue(System.nanoTime() < deadline, "Doris output did not become visible");
                  Thread.sleep(10);
                }
                double milliseconds = (System.nanoTime() - started) / 1_000_000.0;
                if (materialize && arrow) {
                  String job = execution.getJobClient().orElseThrow().getJobID().toString();
                  assertTrue(
                      metrics.recordsIn(job, "native-sink-upsert-materialize") >= cardinality,
                      "Native materializer must process aggregate updates");
                }
                if (materialize)
                  assertEquals(expected, materializedResult(database + "." + output));
                else
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
          }
          sql("DROP TABLE " + database + ".input FORCE");
        }
        Files.writeString(
            directory.resolve("configuration.txt"),
            "Doris="
                + result("SELECT @@version_comment").get(0)
                + "\nconnector=26.3.0\nflink=2.2.1\n"
                + "materialize="
                + materialize
                + "\nstrategy="
                + strategy
                + "\ncardinality="
                + cardinality
                + "\n"
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
                + (materialize
                    ? "materialized-payload=id,count,string(id),decimal(id); nested adds"
                        + " timestamp,array,null-element,row\n"
                        + "cluster=one fresh metric-reporting MiniCluster per job; cluster boot"
                        + " outside timing; JVM reused\n"
                    : "row-batch-null-patterns=name%7,amount%11\n")
                + "boundary=SQL submission through queryable Doris output; includes planning, JNI,"
                + " codecs, I/O, startup and publish wait\n");
      } finally {
        sql("DROP DATABASE " + database + " FORCE");
      }
    } finally {
      if (previousSink == null) System.clearProperty("streamfusion.operator.dorisSink.enabled");
      else System.setProperty("streamfusion.operator.dorisSink.enabled", previousSink);
      if (previous == null) System.clearProperty("streamfusion.doris.enabled");
      else System.setProperty("streamfusion.doris.enabled", previous);
    }
  }

  private String materializedProjection(boolean flink) {
    return "id, CAST(COUNT(*) AS INT)"
        + (flink ? "" : " AS n")
        + ", CAST(id AS STRING)"
        + (flink ? "" : " AS name")
        + ", CAST(id AS DECIMAL(18,2))"
        + (flink ? "" : " AS amount")
        + (complex
            ? ", CAST('2020-07-01 12:34:56.123456' AS "
                + (flink ? "TIMESTAMP(6)" : "DATETIMEV2(6)")
                + ")"
                + (flink ? "" : " AS ts")
                + (flink
                    ? ",ARRAY[CAST(id % 10 AS INT),CAST(NULL AS INT)]"
                    : ",ARRAY(CAST(id % 10 AS INT),NULL)")
                + (flink ? "" : " AS a")
                + ","
                + (flink
                    ? "ROW(CAST(id % 3 AS INT),'r-☃')"
                    : "NAMED_STRUCT('x',CAST(id % 3 AS INT),'t','r-☃') AS r")
            : "");
  }

  private List<String> materializedResult(String table) throws Exception {
    return result(
        "SELECT COUNT(*),SUM(id),SUM(n),SUM(CRC32(name)),SUM(amount)"
            + (complex ? ",SUM(UNIX_TIMESTAMP(ts)),SUM(a[1]),COUNT(a[2]),SUM(r.x),COUNT(r.t)" : "")
            + " FROM "
            + table);
  }

  private String dorisSchema(boolean output) {
    return "(id BIGINT, n INT, name STRING, amount DECIMAL(18,2)"
        + (complex
            ? ", ts DATETIMEV2(6), a ARRAY<INT>"
                + (maps ? ", m MAP<STRING,INT>" : "")
                + ", r STRUCT<x:INT,t:STRING>"
            : "")
        + ") "
        + (materialize && output ? "UNIQUE" : "DUPLICATE")
        + " KEY(id)"
        + " DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES('replication_num'='1'"
        + (materialize && output ? ",'enable_unique_key_merge_on_write'='true'" : "")
        + ")";
  }

  private String ddl(String name, String table, String extra) {
    return "CREATE TABLE "
        + name
        + " (id BIGINT"
        + (materialize ? " NOT NULL" : "")
        + ", n INT, name STRING, amount DECIMAL(18,2)"
        + (complex
            ? ", ts TIMESTAMP(6), a ARRAY<INT>"
                + (maps ? ", m MAP<STRING NOT NULL,INT>" : "")
                + ", r ROW<x INT,t STRING>"
            : "")
        + (materialize && table.startsWith("output_") ? ", PRIMARY KEY(id) NOT ENFORCED" : "")
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

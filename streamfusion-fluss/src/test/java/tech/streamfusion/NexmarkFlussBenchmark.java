package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.fluss.FlussTestCluster;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

/** Same matrix queries and event corpus, with Fluss at both transport boundaries. */
@EnabledIfEnvironmentVariable(named = "SF_FLUSS_BENCH", matches = "true")
class NexmarkFlussBenchmark {
  private static final int PARALLELISM = 4;
  private static final long ROWS =
      Long.parseLong(System.getenv().getOrDefault("SF_ROWS", "1000000"));
  private static final int WARMUPS = Integer.getInteger("nexmark.warmups", 1);
  private static final int RUNS = Integer.getInteger("nexmark.runs", 3);
  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  void flussToFlussMatrix() throws Exception {
    assertTrue(
        System.getProperty("java.library.path").contains("release"),
        "Use -Pbench for measurements");
    String previous = System.getProperty("streamfusion.fluss.enabled");
    System.setProperty("streamfusion.fluss.enabled", "true");
    try (FlussTestCluster cluster = new FlussTestCluster()) {
      cluster.start();
      var config = cluster.config();
      config.set(
          ConfigOptions.CLIENT_WRITER_BUCKET_NO_KEY_ASSIGNER,
          ConfigOptions.NoKeyAssigner.ROUND_ROBIN);
      try (Connection connection = ConnectionFactory.createConnection(config)) {
        connection.getAdmin().createDatabase("nexmark", DatabaseDescriptor.EMPTY, true).get();
        seed(cluster, connection);
        for (var query : NexmarkMatrixBenchmark.selectQueries()) {
          for (int warmup = 0; warmup < WARMUPS; warmup++) {
            try (Result baseline = run(cluster, connection, query, false);
                Result accelerated = run(cluster, connection, query, true)) {
              if (!query.label.equals("q12"))
                baseline.rows.assertSame(accelerated.rows, query.label);
            }
          }
          List<Double> stock = new ArrayList<>();
          List<Double> nativeTimes = new ArrayList<>();
          for (int run = 0; run < RUNS; run++) {
            try (Result baseline = run(cluster, connection, query, false);
                Result accelerated = run(cluster, connection, query, true)) {
              if (!query.label.equals("q12"))
                baseline.rows.assertSame(accelerated.rows, query.label);
              stock.add(baseline.seconds);
              nativeTimes.add(accelerated.seconds);
            }
          }
          System.out.printf(
              "[fluss] %s rows=%d parallelism=%d stock=%s native=%s primaryKeySink=%s%n",
              query.label,
              ROWS,
              PARALLELISM,
              stock,
              nativeTimes,
              NexmarkMatrixBenchmark.UPSERT_KEYS.containsKey(query.label));
        }
      }
    } finally {
      if (previous == null) System.clearProperty("streamfusion.fluss.enabled");
      else System.setProperty("streamfusion.fluss.enabled", previous);
    }
  }

  private static void seed(FlussTestCluster cluster, Connection connection) throws Exception {
    RowType type = FlinkConversions.toFlussRowType(NexmarkKafkaBenchmark.nexmarkRowType());
    StreamTableEnvironment tables = environment(cluster);
    tables.executeSql(
        "CREATE TABLE fluss.nexmark.events ("
            + NexmarkKafkaBenchmark.SCHEMA
            + ", rowtime AS TO_TIMESTAMP_LTZ(`dateTime`, 3),"
            + " WATERMARK FOR rowtime AS rowtime - INTERVAL '4' SECOND)"
            + " WITH ('bucket.num'='4', 'table.log.format'='ARROW',"
            + " 'scan.startup.mode'='earliest', 'scan.bounded.mode'='latest-offset')");
    tables.getCatalog("fluss").orElseThrow().close();
    TablePath path = TablePath.of("nexmark", "events");
    assertFalse(
        connection.getAdmin().getTableInfo(path).get().hasPrimaryKey(),
        "The shared Nexmark input must be append-only");
    var writer = connection.getTable(path).newAppend().createWriter();
    for (long i = 0; i < ROWS; i++) {
      writer.append(row(JSON.readTree(NexmarkKafkaBenchmark.event(i)), type));
      if (i % 8192 == 8191) writer.flush();
    }
    writer.flush();
  }

  private static GenericRow row(JsonNode value, RowType type) {
    GenericRow row = new GenericRow(type.getFieldCount());
    for (int i = 0; i < type.getFieldCount(); i++) {
      JsonNode field = value.get(type.getFields().get(i).getName());
      Object decoded =
          field == null || field.isNull()
              ? null
              : switch (type.getTypeAt(i).getTypeRoot()) {
                case INTEGER -> field.intValue();
                case BIGINT -> field.longValue();
                case STRING -> BinaryString.fromString(field.textValue());
                case ROW -> row(field, (RowType) type.getTypeAt(i));
                default -> throw new IllegalArgumentException("Unexpected Nexmark physical field");
              };
      row.setField(i, decoded);
    }
    return row;
  }

  private static StreamTableEnvironment environment(FlussTestCluster cluster) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(PARALLELISM);
    env.enableCheckpointing(1000);
    env.getConfig().enableObjectReuse();
    StreamTableEnvironment tables = StreamTableEnvironment.create(env);
    tables.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    tables.executeSql(
        "CREATE CATALOG fluss WITH ('type'='fluss', 'bootstrap.servers'='"
            + cluster.config().get(ConfigOptions.BOOTSTRAP_SERVERS).get(0)
            + "')");
    return tables;
  }

  private record Result(double seconds, SortedOutput rows) implements AutoCloseable {
    @Override
    public void close() throws java.io.IOException {
      rows.close();
    }
  }

  private static Result run(
      FlussTestCluster cluster,
      Connection connection,
      NexmarkMatrixBenchmark.Query query,
      boolean nativeRun)
      throws Exception {
    String name = "output_" + UUID.randomUUID().toString().replace("-", "");
    StreamTableEnvironment tables = environment(cluster);
    tables.executeSql("CREATE TEMPORARY VIEW src AS SELECT * FROM fluss.nexmark.events");
    NexmarkMatrixBenchmark.registerEventViews(tables);
    NexmarkMatrixBenchmark.runSetup(tables, query);
    String ddl = query.sinkDdl.replace("%TS%", "TIMESTAMP_LTZ(3)").replace("%WTS%", "TIMESTAMP(3)");
    String key = NexmarkMatrixBenchmark.UPSERT_KEYS.get(query.label);
    if (key != null) ddl = ddl.replace(") WITH", ", PRIMARY KEY (" + key + ") NOT ENFORCED) WITH");
    ddl =
        ddl.replace("CREATE TABLE sink", "CREATE TABLE fluss.nexmark." + name)
            .replace(
                "WITH ('connector' = 'blackhole')",
                "WITH ('bucket.num'='4', 'table.log.format'='ARROW')");
    tables.executeSql(ddl);
    String insert =
        query.insertSql.replace("INSERT INTO sink", "INSERT INTO fluss.nexmark." + name);
    PhysicalPlanScan scan = nativeRun ? NativePlanner.install(tables) : null;
    String plan = tables.explainSql(insert, ExplainDetail.JSON_EXECUTION_PLAN);
    java.nio.file.Path plans = java.nio.file.Path.of("target", "fluss-plans");
    java.nio.file.Files.createDirectories(plans);
    java.nio.file.Files.writeString(
        plans.resolve(query.label + (nativeRun ? "-native" : "-stock") + ".txt"), plan);
    if (nativeRun) {
      assertTrue(plan.contains("native-fluss-source"), scan.explainSummary());
      if (key == null) assertTrue(plan.contains("native-fluss-append-sink"), scan.explainSummary());
      else
        assertFalse(
            plan.contains("native-fluss-append-sink"), "Primary-key production must stay stock");
      assertFalse(plan.contains("RowDataToArrow"), scan.explainSummary());
      if (key == null) assertFalse(plan.contains("ArrowToRowData"), scan.explainSummary());
      int physical = plan.indexOf("== Physical Execution Plan ==");
      JsonNode execution = JSON.readTree(plan.substring(plan.indexOf('{', physical)));
      FlussPlanAssertions.assertNativeInterior(execution, key != null, query.label);
      if (query.label.equals("q3")) {
        assertTrue(plan.contains("NativeShare(consumers=[2])"), scan.explainSummary());
        assertEquals(
            1,
            plan.split("\"contents\" : \"Source: native-fluss-source\"", -1).length - 1,
            "q3 must share one columnar source between both join branches");
      }
    }
    long start = System.nanoTime();
    tables.executeSql(insert).await();
    double seconds = (System.nanoTime() - start) / 1e9;
    TablePath output = TablePath.of("nexmark", name);
    try {
      var info = connection.getAdmin().getTableInfo(output).get();
      assertEquals(key != null, info.hasPrimaryKey(), query.label + " output table mode");
      var getters = InternalRow.createFieldGetters(info.getRowType());
      SortedOutput result = new SortedOutput();
      var source = connection.getTable(output).newScan();
      if (!info.hasPrimaryKey()) source = source.limit(Integer.MAX_VALUE);
      try {
        try (var scanner = source.createBatchScanner()) {
          for (; ; ) {
            var batch = scanner.pollBatch(Duration.ofSeconds(30));
            if (batch == null) break;
            try (batch) {
              while (batch.hasNext()) {
                InternalRow row = batch.next();
                List<String> fields = new ArrayList<>();
                for (var getter : getters) fields.add(String.valueOf(getter.getFieldOrNull(row)));
                result.add(fields.toString());
              }
            }
          }
        }
        return new Result(seconds, result);
      } catch (Throwable failure) {
        try {
          result.close();
        } catch (Exception closing) {
          failure.addSuppressed(closing);
        }
        throw failure;
      }
    } finally {
      try {
        connection.getAdmin().dropTable(output, false).get();
      } finally {
        tables.getCatalog("fluss").orElseThrow().close();
      }
    }
  }
}

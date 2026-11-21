package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.tpcds.Results;
import io.trino.tpcds.Session;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import jdk.jfr.Configuration;
import jdk.jfr.Recording;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.RecoveryExecutionMetrics;
import tech.streamfusion.compat.FlinkTestContexts;

/** Matched stock and native SQL over the same four-partition Iceberg fixture. */
@EnabledIfEnvironmentVariable(named = "SF_TPCDS", matches = "true")
class IcebergTpcdsBenchmark {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String RESOURCES = "/tpcds/flink-2.2.1/";
  private static final int PARTITIONS = 4;
  private static final Namespace NAMESPACE = Namespace.of("tpcds");
  private static final String PARTITION = "sf_result_partition";
  private static final Map<String, String> FILE_OPTIONS =
      Map.of(
          "format-version",
          "2",
          "write.format.default",
          "parquet",
          "write.parquet.compression-codec",
          "snappy",
          "write.distribution-mode",
          "hash",
          "write-parallelism",
          "4",
          "source.parallelism",
          "4");
  private static final Map<String, String> FACT_KEYS =
      Map.of(
          "catalog_sales",
          "cs_order_number",
          "catalog_returns",
          "cr_order_number",
          "store_sales",
          "ss_ticket_number",
          "store_returns",
          "sr_ticket_number",
          "web_sales",
          "ws_order_number",
          "web_returns",
          "wr_order_number");

  @Test
  void compare() throws Exception {
    String directory = System.getProperty("sf.tpcds.directory");
    assertNotNull(directory, "Set -Dsf.tpcds.directory to a benchmark scratch directory");
    Path root = Path.of(directory).toAbsolutePath();
    Files.createDirectories(root);
    var manifest = JSON.readTree(resource("manifest.json")).get("queries");
    assertEquals(103, manifest.size());
    var fixture = root.resolve("fixture.json");
    try (var catalog =
        new HadoopCatalog(
            new org.apache.hadoop.conf.Configuration(),
            root.resolve("warehouse").toUri().toString())) {
      if (!Files.exists(fixture)) prepare(catalog, fixture);
      verifyFixture(catalog, fixture);
      if (!Boolean.getBoolean("sf.tpcds.prepareOnly")) compare(root, catalog, manifest);
    }
  }

  private void compare(
      Path root, HadoopCatalog catalog, com.fasterxml.jackson.databind.JsonNode manifest)
      throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"), "Use -Pbench");
    int warmups = Integer.getInteger("sf.tpcds.warmups", 1);
    int trials = Integer.getInteger("sf.tpcds.trials", 3);
    int offHeapMiB = Integer.getInteger("sf.tpcds.offHeapMiB", 512);
    assertTrue(warmups >= 0 && trials > 0);
    assertTrue(offHeapMiB > 0);
    boolean profile = Boolean.getBoolean("sf.tpcds.profile");
    boolean stockCompute = Boolean.getBoolean("sf.tpcds.stockCompute");
    boolean stockSource = Boolean.getBoolean("sf.tpcds.stockSource");
    boolean stockSink = Boolean.getBoolean("sf.tpcds.stockSink");
    String selection = System.getProperty("sf.tpcds.queries", "");
    String reportName = System.getProperty("sf.tpcds.report", "trials");
    Path report = root.resolve(reportName);
    Files.createDirectory(report);
    var config =
        org.apache.flink.configuration.Configuration.fromMap(
            Map.of(
                "parallelism.default",
                "4",
                "table.exec.resource.default-parallelism",
                "4",
                "execution.batch.adaptive.auto-parallelism.enabled",
                "false",
                "table.optimizer.range-sort.enabled",
                "false",
                "table.optimizer.join-reorder-enabled",
                "true",
                "table.optimizer.join.broadcast-threshold",
                "10485760",
                "taskmanager.memory.managed.size",
                "1024m",
                "taskmanager.memory.task.off-heap.size",
                offHeapMiB + "m",
                "restart-strategy.type",
                "none",
                "streamfusion.transpose.batchRows",
                "8192"));
    var nativeConfig = new org.apache.flink.configuration.Configuration(config);
    if (stockCompute) nativeConfig.setString("streamfusion.operator.calc.enabled", "false");
    if (stockSource) nativeConfig.setString("streamfusion.operator.icebergSource.enabled", "false");
    if (stockSink) nativeConfig.setString("streamfusion.operator.icebergSink.enabled", "false");
    var failures = new ArrayList<String>();
    int selected = 0;
    try (var metrics = RecoveryExecutionMetrics.countersOnly();
        var writer = Files.newBufferedWriter(report.resolve("trials.jsonl"))) {
      metrics.setAsContext(config, PARTITIONS, ((long) offHeapMiB) << 20);
      var stock = environment(root, config, false);
      var nativeEnv = environment(root, nativeConfig, true);
      for (var query : manifest) {
        String id = query.get("id").asText();
        if (!selection.isBlank() && !List.of(selection.split(",")).contains(id)) continue;
        selected++;
        var sql = resource(query.get("file").asText());
        assertEquals(query.get("sha256").asText(), hash(sql));
        sql = sql.stripTrailing().replaceFirst(";\\s*$", "");
        var columns = stock.sqlQuery(sql).getResolvedSchema().getColumnDataTypes();
        assertEquals(columns, nativeEnv.sqlQuery(sql).getResolvedSchema().getColumnDataTypes());
        int hashField = 0;
        var fields = new ArrayList<RowType.RowField>();
        for (int i = 0; i < columns.size(); i++) {
          var type = columns.get(i).getLogicalType();
          fields.add(new RowType.RowField("c" + i, type));
          if (!type.isNullable() && columns.get(hashField).getLogicalType().isNullable())
            hashField = i;
        }
        fields.add(new RowType.RowField(PARTITION, new IntType()));
        var schema = icebergSchema(new RowType(fields));
        var spec = PartitionSpec.builderFor(schema).identity(PARTITION).build();
        var aliases =
            IntStream.range(0, columns.size())
                .mapToObj(i -> "c" + i)
                .collect(Collectors.joining(","));
        var queryWithPartition =
            "SELECT q.*, MOD(MOD(HASH_CODE(COALESCE(CAST(c"
                + hashField
                + " AS STRING), '')), 4) + 4, 4) FROM ("
                + sql
                + "\n) q("
                + aliases
                + ")";
        Output expected = null;
        try {
          for (int trial = -warmups; trial < trials; trial++) {
            for (int position = 0; position < 2; position++) {
              boolean nativeRun = Math.floorMod(selected + trial + position, 2) == 0;
              var env = nativeRun ? nativeEnv : stock;
              var engine = nativeRun ? "streamfusion" : "stock";
              var name = "out_" + id + "_" + engine + "_" + (trial + warmups);
              var ident = TableIdentifier.of(NAMESPACE, name);
              catalog.createTable(ident, schema, spec, FILE_OPTIONS);
              try {
                var insert = "INSERT INTO " + name + " " + queryWithPartition;
                if (trial == -warmups) {
                  Files.writeString(
                      report.resolve("q" + id + "-" + engine + ".plan"),
                      env.explainSql(insert, ExplainDetail.JSON_EXECUTION_PLAN));
                }
                var measurement =
                    measure(
                        env,
                        insert,
                        profile && trial >= 0
                            ? report.resolve("q" + id + "-" + engine + "-" + trial + ".jfr")
                            : null);
                var output = read(catalog.loadTable(ident));
                if (expected == null) expected = output;
                else {
                  assertTrue(
                      expected.rows().equals(output.rows()),
                      "Committed result mismatch for Q" + id);
                  assertEquals(expected.partitions(), output.partitions(), "Q" + id);
                }
                var work =
                    metrics.nativeOperators().stream()
                        .filter(row -> measurement.jobId().equals(row.get("jobId")))
                        .toList();
                if (!nativeRun) assertTrue(work.isEmpty(), "Q" + id + " stock plan");
                if (stockCompute)
                  assertTrue(
                      work.stream()
                          .allMatch(row -> row.get("operator").toString().startsWith("NativeShare")),
                      "Q" + id + " stock compute");
                long nativeTasks = metrics.counterValue(measurement.jobId(), "nativeParquetTasks");
                long fallbacks = metrics.counterValue(measurement.jobId(), "stockFallbackTasks");
                if (nativeRun && !stockSource)
                  assertTrue(nativeTasks > 0, "Q" + id + " native scan");
                else assertEquals(0, nativeTasks, "Q" + id + " stock scan");
                assertEquals(0, fallbacks, "Q" + id + " Java file task fallback");
                assertEquals(
                    nativeRun && !stockSink ? output.files() : 0, output.nativeFiles(), "Q" + id);
                var observation = new LinkedHashMap<String, Object>();
                observation.put("query", id);
                observation.put("engine", engine);
                observation.put("trial", trial);
                observation.put("taskOffHeapMiB", offHeapMiB);
                observation.put("elapsedNanos", measurement.elapsedNanos());
                observation.put("resultRows", output.rows().size());
                observation.put("resultSha256", hash(String.join("\n", output.rows())));
                observation.put("partitions", output.partitions());
                observation.put("nativeTasks", nativeTasks);
                observation.put("nativeFiles", output.nativeFiles());
                observation.put("files", output.files());
                observation.put("work", work);
                observation.put(
                    "entryTransposeRows",
                    metrics.counterValue(measurement.jobId(), "numInputRows", "RowDataToArrow"));
                observation.put(
                    "nativeLz4Bytes", metrics.counterValue(measurement.jobId(), "nativeLz4Bytes"));
                String line = JSON.writeValueAsString(observation);
                writer.write(line);
                writer.newLine();
                writer.flush();
                System.out.printf(
                    Locale.ROOT,
                    "TPCDS q=%s engine=%s trial=%d ms=%.3f rows=%d reads=%d files=%d%n",
                    id,
                    engine,
                    trial,
                    measurement.elapsedNanos() / 1e6,
                    output.rows().size(),
                    nativeTasks,
                    output.nativeFiles());
              } finally {
                catalog.dropTable(ident, true);
              }
            }
          }
        } catch (Exception | AssertionError failure) {
          failure.printStackTrace();
          var causes = new ArrayList<String>();
          for (Throwable cause = failure; cause != null; cause = cause.getCause())
            causes.add(cause.toString());
          writer.write(JSON.writeValueAsString(Map.of("query", id, "failure", causes)));
          writer.newLine();
          writer.flush();
          failures.add(id);
        }
      }
    } finally {
      FlinkTestContexts.clearForRemoteSubmission();
    }
    assertTrue(selected > 0);
    assertTrue(failures.isEmpty(), "Failed variants " + failures);
  }

  private record Measurement(long elapsedNanos, String jobId) {}

  private static Measurement measure(TableEnvironment env, String insert, Path flightRecording)
      throws Exception {
    try (var recording =
        flightRecording == null ? null : new Recording(Configuration.getConfiguration("profile"))) {
      if (recording != null) {
        recording.setDestination(flightRecording);
        recording.start();
        System.out.println(
            "TPCDS_PROFILE pid=" + ProcessHandle.current().pid() + " file=" + flightRecording);
        System.out.flush();
      }
      long start = System.nanoTime();
      var result = env.executeSql(insert);
      result.await(180, TimeUnit.SECONDS);
      long elapsed = System.nanoTime() - start;
      if (recording != null) recording.stop();
      return new Measurement(elapsed, result.getJobClient().orElseThrow().getJobID().toString());
    }
  }

  private static TableEnvironment environment(
      Path root, org.apache.flink.configuration.Configuration config, boolean nativeRun)
      throws Exception {
    var options = new org.apache.flink.configuration.Configuration(config);
    options.setString("streamfusion.native.enabled", Boolean.toString(nativeRun));
    var env = NativePlannerTestEnvironment.createBatch(options);
    env.executeSql(
        "CREATE CATALOG ice WITH ('type'='iceberg','catalog-type'='hadoop','warehouse'='"
            + root.resolve("warehouse").toUri()
            + "')");
    env.executeSql("USE CATALOG ice");
    env.executeSql("USE tpcds");
    var columns =
        new ArrayList<>(env.from("warehouse_partitioned").getResolvedSchema().getColumnNames());
    assertTrue(columns.remove("sf_input_partition"));
    env.executeSql(
        "CREATE TEMPORARY VIEW warehouse AS SELECT "
            + String.join(",", columns)
            + " FROM warehouse_partitioned");
    return env;
  }

  private void prepare(HadoopCatalog catalog, Path fixture) throws Exception {
    assertFalse(
        catalog.namespaceExists(NAMESPACE), "Select a new directory for an incomplete fixture");
    catalog.createNamespace(NAMESPACE);
    var inputs = JSON.readTree(resource("schema.json")).get("tables");
    assertEquals(24, inputs.size());
    var rows = new LinkedHashMap<String, Long>();
    for (var def : inputs) {
      String name = def.get("name").asText();
      var generator = io.trino.tpcds.Table.getTable(name);
      var fields = new ArrayList<RowType.RowField>();
      assertEquals(generator.getColumns().length, def.get("columns").size(), name);
      for (int i = 0; i < generator.getColumns().length; i++) {
        var column = def.get("columns").get(i);
        var generated = generator.getColumns()[i].getName().toLowerCase(Locale.ROOT);
        if (generated.equals("p_response_targe")) generated = "p_response_target";
        assertEquals(generated, column.get("name").asText(), name);
        fields.add(
            new RowType.RowField(
                generated,
                LogicalTypeParser.parse(column.get("type").asText(), getClass().getClassLoader())));
      }
      boolean warehouse = name.equals("warehouse");
      if (warehouse) fields.add(new RowType.RowField("sf_input_partition", new IntType(false)));
      var type = new RowType(fields);
      var schema = icebergSchema(type);
      var spec =
          warehouse
              ? PartitionSpec.builderFor(schema).identity("sf_input_partition").build()
              : PartitionSpec.builderFor(schema)
                  .bucket(FACT_KEYS.getOrDefault(name, fields.get(0).getName()), PARTITIONS)
                  .build();
      var ident = TableIdentifier.of(NAMESPACE, warehouse ? "warehouse_partitioned" : name);
      var table = catalog.createTable(ident, schema, spec, FILE_OPTIONS);
      var factory =
          new GenericFileWriterFactory.Builder(table).dataFileFormat(FileFormat.PARQUET).build();
      var writers = new HashMap<Integer, DataWriter<org.apache.iceberg.data.Record>>();
      var partition = new PartitionKey(spec, schema);
      var wrapper = new InternalRecordWrapper(schema.asStruct());
      var record = GenericRecord.create(schema);
      long count = 0;
      try {
        var session = Session.getDefaultSession().withScale(1).withTable(generator);
        for (var batch : Results.constructResults(generator, session)) {
          for (var values : batch) {
            for (int i = 0; i < values.size(); i++)
              record.set(i, value(values.get(i), type.getTypeAt(i)));
            if (warehouse) record.set(values.size(), (int) (count % PARTITIONS));
            partition.partition(wrapper.wrap(record));
            var bucket = partition.get(0, Integer.class);
            assertNotNull(bucket, name);
            var writer = writers.get(bucket);
            if (writer == null) {
              writer =
                  factory.newDataWriter(
                      table
                          .encryption()
                          .encrypt(
                              table
                                  .io()
                                  .newOutputFile(
                                      table.location() + "/input-" + bucket + ".parquet")),
                      spec,
                      partition.copy());
              writers.put(bucket, writer);
            }
            writer.write(record);
            count++;
          }
        }
      } finally {
        for (var writer : writers.values()) writer.close();
      }
      assertEquals(Set.of(0, 1, 2, 3), writers.keySet(), name);
      var append = table.newAppend();
      for (var writer : writers.values()) append.appendFile(writer.toDataFile());
      append.commit();
      rows.put(ident.name(), count);
      System.out.println("TPCDS_FIXTURE " + name + " rows=" + count);
    }
    JSON.writeValue(fixture.toFile(), rows);
  }

  private static void verifyFixture(HadoopCatalog catalog, Path fixture) throws Exception {
    var tables = JSON.readTree(fixture.toFile());
    assertEquals(24, tables.size());
    long totalRows = 0;
    for (var entries = tables.fields(); entries.hasNext(); ) {
      var entry = entries.next();
      var table = catalog.loadTable(TableIdentifier.of(NAMESPACE, entry.getKey()));
      var buckets = new TreeSet<Integer>();
      int files = 0;
      long rows = 0;
      try (var tasks = table.newScan().planFiles()) {
        for (var task : tasks) {
          files++;
          rows += task.file().recordCount();
          buckets.add(task.file().partition().get(0, Integer.class));
        }
      }
      assertEquals(entry.getValue().asLong(), rows);
      assertEquals(PARTITIONS, files);
      assertEquals(Set.of(0, 1, 2, 3), buckets);
      totalRows += rows;
    }
    assertEquals(19_557_335, totalRows);
  }

  private static Schema icebergSchema(RowType type) {
    return new Schema(FlinkSchemaUtil.convert(type).asStructType().fields());
  }

  private static Object value(String raw, LogicalType type) {
    if (raw == null || raw.isEmpty()) return null;
    return switch (type.getTypeRoot()) {
      case BIGINT -> Long.parseLong(raw);
      case INTEGER -> Integer.parseInt(raw);
      case DATE -> LocalDate.parse(raw);
      case DECIMAL -> new BigDecimal(raw).setScale(((DecimalType) type).getScale());
      case VARCHAR, CHAR -> raw;
      default -> throw new IllegalArgumentException(type.asSummaryString());
    };
  }

  private record Output(List<String> rows, int files, int nativeFiles, List<Integer> partitions) {}

  private static Output read(Table table) throws Exception {
    var rows = new ArrayList<String>();
    var hadoop = new org.apache.hadoop.conf.Configuration();
    int nativeFiles = 0;
    int files = 0;
    long fileRows = 0;
    var partitions = new TreeSet<Integer>();
    try (var tasks = table.newScan().planFiles()) {
      for (var task : tasks) {
        var file = task.file();
        files++;
        fileRows += file.recordCount();
        int partition = file.partition().get(0, Integer.class);
        assertTrue(partition >= 0 && partition < PARTITIONS);
        partitions.add(partition);
        try (var reader =
            ParquetFileReader.open(
                HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(file.location()), hadoop))) {
          if (reader.getFooter().getFileMetaData().getCreatedBy().contains("parquet-rs"))
            nativeFiles++;
        }
      }
    }
    try (var data = IcebergGenerics.read(table).build()) {
      for (var row : data) {
        var values = new ArrayList<String>();
        for (int i = 0; i < row.size(); i++) {
          var v = row.get(i, Object.class);
          values.add(
              v == null
                  ? null
                  : v instanceof BigDecimal decimal
                      ? decimal.toPlainString()
                      : v instanceof Double d
                          ? Long.toHexString(Double.doubleToLongBits(d))
                          : v instanceof Float f
                              ? Integer.toHexString(Float.floatToIntBits(f))
                              : v.toString());
        }
        rows.add(JSON.writeValueAsString(values));
      }
    }
    assertEquals(fileRows, rows.size());
    rows.sort(String::compareTo);
    return new Output(rows, files, nativeFiles, List.copyOf(partitions));
  }

  private String resource(String name) throws Exception {
    try (var stream = getClass().getResourceAsStream(RESOURCES + name)) {
      assertNotNull(stream, name);
      return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private static String hash(String value) throws Exception {
    return HexFormat.of()
        .formatHex(
            MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
  }
}

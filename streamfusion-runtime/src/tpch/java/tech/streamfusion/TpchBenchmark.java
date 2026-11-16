package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

/**
 * Complete jobs compare identical files and SQL, including each engine's source and collecting
 * sink.
 */
@EnabledIfEnvironmentVariable(named = "SF_TPCH", matches = "true")
class TpchBenchmark {
  private final ObjectMapper json = new ObjectMapper();

  @Test
  void runTheCompleteWorkload() throws Exception {
    Path root = Path.of(System.getProperty("sf.tpch.directory")).toAbsolutePath();
    Files.createDirectories(root);
    double scale = Double.parseDouble(System.getProperty("sf.tpch.scale", "0.01"));
    String format = inputFormat();
    String mode = System.getProperty("sf.tpch.mode", "audit");
    assertTrue(mode.equals("audit") || mode.equals("verify") || mode.equals("compare"), mode);
    if (mode.equals("compare"))
      assertEquals(
          "false", System.getProperty("streamfusion.exchange.zeroCopyLocal"), "Use -Pbench");
    boolean profile = Boolean.getBoolean("sf.tpch.profile");
    assertFalse(profile && mode.equals("compare"), "Profiling belongs in verification mode");
    int warmups = Integer.getInteger("sf.tpch.warmups", 3);
    int trials = Integer.getInteger("sf.tpch.trials", 7);
    assertTrue(warmups >= 0 && trials > 0, "Warmups must be nonnegative and trials positive");
    int parallelism = Integer.getInteger("sf.tpch.parallelism", 2);
    long cap = Long.getLong("sf.tpch.nativeBytes", 512L << 20);
    var tables = generate(root.resolve("data-sf-" + scale), scale);
    var report = new LinkedHashMap<String, Object>();
    report.put("mode", mode);
    report.put("format", format);
    report.put(
        "resultOrderValidation",
        "Exact rows and multiplicities, preserving ORDER BY key order; row order within equal-key"
            + " groups is immaterial");
    report.put("profile", profile);
    report.put("scale", scale);
    report.put("javaVersion", System.getProperty("java.version"));
    report.put("javaVm", System.getProperty("java.vm.name"));
    report.put("os", System.getProperty("os.name") + " " + System.getProperty("os.arch"));
    report.put("maxHeapBytes", Runtime.getRuntime().maxMemory());
    report.put("generator", "io.airlift.tpch:tpch:0.10");
    report.put(
        "schema",
        "TPC-H identifiers BIGINT, integer INT, numeric DECIMAL(15,2), date DATE, text STRING");
    report.put("parallelism", parallelism);
    report.put("nativeBytes", cap);
    report.put("nativeBatchRows", Integer.getInteger("streamfusion.transpose.batchRows", 8192));
    report.put(
        "fileSource", Boolean.parseBoolean(System.getProperty("sf.tpch.fileSource", "true")));
    report.put("warmups", mode.equals("compare") ? warmups : 0);
    report.put("trials", mode.equals("compare") ? trials : 1);
    report.put("tables", tables);
    List<Map<String, Object>> queries = new ArrayList<>();
    report.put("queries", queries);
    Configuration config = new Configuration();
    config.setString(
        "streamfusion.operator.fileSource.enabled",
        System.getProperty("sf.tpch.fileSource", "true"));
    config.setString("taskmanager.memory.managed.size", "1024m");
    config.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
    config.setString("table.exec.resource.default-parallelism", Integer.toString(parallelism));
    config.setString("table.optimizer.range-sort.enabled", "false");
    List<String> failures = new ArrayList<>();
    try (var metrics = RecoveryExecutionMetrics.countersOnly()) {
      metrics.setAsContext(config, parallelism, cap);
      Path directory =
          prepareInput(root, root.resolve("data-sf-" + scale), config, parallelism, format, report);
      var stock = environment(false, config, parallelism, directory, format);
      var nativeEnv = environment(true, config, parallelism, directory, format);
      String selection = "," + System.getProperty("sf.tpch.queries", "") + ",";
      report.put("completeWorkload", selection.equals(",,"));
      for (int number = 1; number <= 22; number++) {
        if (!selection.equals(",,") && !selection.contains("," + number + ",")) continue;
        var query = new LinkedHashMap<String, Object>();
        query.put("query", number);
        queries.add(query);
        String sql = query(number);
        query.put("sqlSha256", hash(sql));
        query.put("sql", sql);
        try {
          query.put("stockPlan", stock.explainSql(sql));
          String nativePlan = nativeEnv.explainSql(sql);
          query.put("nativePlan", nativePlan);
          query.put("admission", NativePlanner.explain(nativeEnv, sql));
          boolean planned =
              nativePlan.contains("NativeAggregate")
                  || nativePlan.contains("NativeCalc")
                  || nativePlan.contains("NativeSort");
          query.put("nativePlanned", planned);
          var stockTimes = new ArrayList<Long>();
          var nativeTimes = new ArrayList<Long>();
          var stockRuntime = new ArrayList<Long>();
          var nativeRuntime = new ArrayList<Long>();
          var runs = new ArrayList<Map<String, Object>>();
          query.put("runs", runs);
          List<String> expected = null;
          String expectedSchema = null;
          int first = mode.equals("compare") ? -warmups : 0;
          int last = mode.equals("compare") ? trials : 1;
          for (int trial = first; trial < last; trial++) {
            if (!mode.equals("audit")) assertTrue(planned, "Q" + number + " has no native plan");
            boolean[] order =
                mode.equals("audit")
                    ? new boolean[] {false}
                    : trial % 2 == 0 ? new boolean[] {false, true} : new boolean[] {true, false};
            for (boolean nativeRun : order) {
              try (var recording =
                  profile
                      ? new jdk.jfr.Recording(jdk.jfr.Configuration.getConfiguration("profile"))
                      : null) {
                if (recording != null) recording.start();
                QueryRun measured =
                    executeQuery(
                        nativeRun ? nativeEnv : stock, sql, root, number, trial, nativeRun);
                String schema = measured.schema;
                if (expectedSchema == null) expectedSchema = schema;
                else
                  assertEquals(
                      expectedSchema, schema, "Q" + number + " declared result types differ");
                var client = measured.client;
                long runtime = measured.runtime;
                long nanos = measured.nanos;
                var rows = measured.rows;
                long work =
                    metrics.nativeOperators().stream()
                        .filter(item -> client.getJobID().toString().equals(item.get("jobId")))
                        .mapToLong(item -> ((Number) item.get("recordsIn")).longValue())
                        .sum();
                var observation = new LinkedHashMap<String, Object>();
                observation.put("engine", nativeRun ? "streamfusion" : "stock");
                observation.put("trial", trial);
                observation.put("jobId", client.getJobID().toString());
                long sourceRows = nativeSourceRows(metrics, client.getJobID().toString());
                observation.put("sourceArrowRows", sourceRows);
                long entryRows =
                    metrics.counterValue(
                        client.getJobID().toString(), "numInputRows", "RowDataToArrow");
                observation.put("entryTransposeRows", entryRows);
                if (nativeRun
                    && Boolean.parseBoolean(
                        System.getProperty(
                            "sf.tpch.requireNativeSource",
                            Boolean.toString(
                                (format.equals("parquet") || format.equals("paimon"))
                                    && Boolean.parseBoolean(
                                        System.getProperty("sf.tpch.fileSource", "true")))))) {
                  assertTrue(
                      sourceRows > 0,
                      "Q" + number + " native " + format + " source did not execute");
                  assertEquals(0, entryRows, "Q" + number + " retained a row-to-Arrow entry");
                }
                observation.put(
                    "nativeWork",
                    metrics.nativeOperators().stream()
                        .filter(item -> client.getJobID().toString().equals(item.get("jobId")))
                        .collect(java.util.stream.Collectors.toList()));
                observation.putAll(measured.details);
                runs.add(observation);
                if (nativeRun) assertTrue(work > 0, "Q" + number + " consumed no native rows");
                else assertEquals(0, work, "stock run entered native execution");
                if (expected == null) expected = rows;
                else assertEquals(expected, rows, "Q" + number + " results differ");
                if (trial >= 0) {
                  (nativeRun ? nativeTimes : stockTimes).add(nanos);
                  (nativeRun ? nativeRuntime : stockRuntime).add(runtime);
                }
                System.out.printf(
                    "[tpch-trial] q=%d trial=%d engine=%s elapsedMs=%.3f rows=%d nativeRows=%d%n",
                    number,
                    trial,
                    nativeRun ? "streamfusion" : "stock",
                    nanos / 1_000_000.0,
                    rows.size(),
                    work);
                if (recording != null) {
                  recording.stop();
                  recording.dump(
                      root.resolve("q" + number + (nativeRun ? "-native.jfr" : "-stock.jfr")));
                }
              }
            }
          }
          query.put("resultType", expectedSchema);
          query.put("rows", expected.size());
          query.put("resultSha256", hash(String.join("\n", expected)));
          query.put("stockNanos", stockTimes);
          query.put("nativeNanos", nativeTimes);
          query.put("stockRuntimeMs", stockRuntime);
          query.put("nativeRuntimeMs", nativeRuntime);
          query.put("status", "passed");
          System.out.printf(
              "[tpch] q=%d rows=%d nativePlanned=%s stockNanos=%s nativeNanos=%s%n",
              number, expected.size(), planned, stockTimes, nativeTimes);
        } catch (Exception | AssertionError failure) {
          query.put("status", "failed");
          query.put("error", failure.toString());
          List<String> causes = new ArrayList<>();
          for (Throwable cause = failure; cause != null; cause = cause.getCause())
            causes.add(cause.toString());
          query.put("causes", causes);
          failure.printStackTrace();
          failures.add("Q" + number + ": " + failure);
          System.out.printf("[tpch] q=%d FAILED %s%n", number, failure);
        } finally {
          json.writerWithDefaultPrettyPrinter()
              .writeValue(
                  root.resolve(
                          "report-" + (format.equals("csv") ? "" : format + "-") + mode + ".json")
                      .toFile(),
                  report);
        }
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
    assertFalse(queries.isEmpty(), "No TPC-H queries selected");
    if (System.getProperty("sf.tpch.queries", "").isEmpty()) assertEquals(22, queries.size());
    assertTrue(failures.isEmpty(), failures.toString());
  }

  String inputFormat() {
    String format = System.getProperty("sf.tpch.format", "csv");
    assertTrue(format.equals("csv") || format.equals("parquet"), "Unsupported TPC-H format");
    return format;
  }

  long nativeSourceRows(RecoveryExecutionMetrics metrics, String job) {
    return metrics.counterValue(job, "sourceArrowRows");
  }

  Path prepareInput(
      Path root,
      Path csv,
      Configuration config,
      int parallelism,
      String format,
      Map<String, Object> report)
      throws Exception {
    if (!format.equals("parquet")) return csv;
    Path directory =
        root.resolve(csv.getFileName().toString().replace("data-sf-", "data-parquet-sf-"));
    report.put("parquet", parquet(csv, directory, config, parallelism));
    return directory;
  }

  static final class QueryRun {
    final String schema;
    final List<String> rows;
    final org.apache.flink.core.execution.JobClient client;
    final long runtime;
    final long nanos;
    final Map<String, Object> details;

    QueryRun(
        String schema,
        List<String> rows,
        org.apache.flink.core.execution.JobClient client,
        long runtime,
        long nanos,
        Map<String, Object> details) {
      this.schema = schema;
      this.rows = rows;
      this.client = client;
      this.runtime = runtime;
      this.nanos = nanos;
      this.details = details;
    }
  }

  QueryRun executeQuery(
      TableEnvironment environment, String sql, Path root, int number, int trial, boolean nativeRun)
      throws Exception {
    long start = System.nanoTime();
    var result = environment.executeSql(sql);
    String schema = result.getResolvedSchema().toPhysicalRowDataType().toString();
    int[] orderFields = orderFields(sql, result.getResolvedSchema().getColumnNames());
    List<String> rows = new ArrayList<>();
    List<List<Object>> keys = new ArrayList<>();
    try (var iterator = result.collect()) {
      while (iterator.hasNext()) {
        Row row = iterator.next();
        rows.add(row.toString());
        var key = new ArrayList<Object>();
        for (int field : orderFields) key.add(row.getField(field));
        keys.add(key);
      }
    }
    var client = result.getJobClient().orElseThrow();
    long runtime = client.getJobExecutionResult().get().getNetRuntime();
    long nanos = System.nanoTime() - start;
    rows = canonicalRows(rows, keys);
    return new QueryRun(schema, rows, client, runtime, nanos, Map.of());
  }

  TableEnvironment environment(
      boolean nativeRun, Configuration options, int parallelism, Path directory, String format)
      throws Exception {
    Configuration config = new Configuration(options);
    config.setString("streamfusion.native.enabled", Boolean.toString(nativeRun));
    config.setString("parallelism.default", Integer.toString(parallelism));
    var table = NativePlannerTestEnvironment.createBatch(config);
    for (TpchTable<?> source : TpchTable.getTables()) {
      table.executeSql(
          "CREATE TABLE "
              + source.getTableName()
              + " ("
              + columns(source)
              + ") WITH ('connector'='filesystem','format'='"
              + format
              + "',"
              + (format.equals("csv") ? "'csv.field-delimiter'='|'," : "")
              + "'path'='"
              + directory
                  .resolve(source.getTableName() + (format.equals("csv") ? ".csv" : ""))
                  .toUri()
                  .toString()
                  .replace("'", "''")
              + "')");
    }
    return table;
  }

  static String columns(TpchTable<?> source) {
    List<String> fields = new ArrayList<>();
    for (TpchColumn<?> column : source.getColumns()) {
      String type;
      switch (column.getType().getBase()) {
        case IDENTIFIER:
          type = "BIGINT";
          break;
        case INTEGER:
          type = "INT";
          break;
        case DATE:
          type = "DATE";
          break;
        case DOUBLE:
          type = "DECIMAL(15,2)";
          break;
        default:
          type = "STRING";
      }
      fields.add(column.getColumnName() + " " + type);
    }
    return String.join(",", fields);
  }

  private Map<String, Object> parquet(
      Path csv, Path directory, Configuration config, int parallelism) throws Exception {
    Files.createDirectories(directory);
    Path manifest = directory.resolve("manifest.json");
    String source = hashFile(csv.resolve("manifest.json"));
    if (Files.exists(manifest)) {
      Map<String, Object> cached = json.readValue(manifest.toFile(), LinkedHashMap.class);
      assertEquals(source, cached.get("sourceCsvManifestSha256"), "Parquet source data changed");
      for (Map<?, ?> file : (List<Map<?, ?>>) cached.get("files")) {
        Path path = directory.resolve((String) file.get("path"));
        assertEquals(((Number) file.get("bytes")).longValue(), Files.size(path));
        assertEquals(file.get("sha256"), hashFile(path));
      }
      return cached;
    }
    Path owner = directory.resolve(".streamfusion-tpch-owner");
    if (!Files.exists(owner)) {
      try (var existing = Files.list(directory)) {
        assertFalse(
            existing.findAny().isPresent(),
            "Refusing to replace a nonempty unowned Parquet directory");
      }
      Files.writeString(owner, "StreamFusion TPC-H generated data\n");
    }
    var created = new ArrayList<Path>();
    boolean complete = false;
    try {
      // Stock Flink writes each logical table once, outside all query timers.
      var writer = environment(false, config, parallelism, csv, "csv");
      var jobs = new ArrayList<String>();
      for (TpchTable<?> table : TpchTable.getTables()) {
        Path destination = directory.resolve(table.getTableName());
        if (Files.exists(destination)) removeGenerated(destination);
        created.add(destination);
        writer.executeSql(
            "CREATE TABLE prepared_"
                + table.getTableName()
                + " ("
                + columns(table)
                + ") WITH"
                + " ('connector'='filesystem','format'='parquet','parquet.compression'='SNAPPY','path'='"
                + destination.toUri().toString().replace("'", "''")
                + "')");
        var result =
            writer.executeSql(
                "INSERT INTO prepared_"
                    + table.getTableName()
                    + " SELECT * FROM "
                    + table.getTableName());
        result.await();
        var client = result.getJobClient().orElseThrow();
        client.getJobExecutionResult().get();
        jobs.add(client.getJobID().toString());
      }
      var files = new ArrayList<Map<String, Object>>();
      try (var paths = Files.walk(directory)) {
        for (Path path :
            paths
                .filter(Files::isRegularFile)
                .filter(path -> !path.getFileName().toString().startsWith("."))
                .sorted()
                .collect(java.util.stream.Collectors.toList())) {
          files.add(
              Map.of(
                  "path",
                  directory.relativize(path).toString(),
                  "bytes",
                  Files.size(path),
                  "sha256",
                  hashFile(path)));
        }
      }
      Map<String, Object> prepared = new LinkedHashMap<>();
      prepared.put("writer", "stock Flink filesystem/Parquet, SNAPPY, outside query timing");
      prepared.put("writerParallelism", parallelism);
      prepared.put("sourceCsvManifestSha256", source);
      prepared.put("writerJobIds", jobs);
      prepared.put("files", files);
      json.writerWithDefaultPrettyPrinter().writeValue(manifest.toFile(), prepared);
      complete = true;
      return prepared;
    } finally {
      if (!complete) for (Path path : created) removeGenerated(path);
    }
  }

  static void removeGenerated(Path directory) throws Exception {
    if (!Files.exists(directory)) return;
    try (var paths = Files.walk(directory)) {
      for (Path path :
          paths
              .sorted(java.util.Comparator.reverseOrder())
              .collect(java.util.stream.Collectors.toList())) Files.delete(path);
    }
  }

  private Map<String, Object> generate(Path directory, double scale) throws Exception {
    Files.createDirectories(directory);
    Path manifest = directory.resolve("manifest.json");
    if (Files.exists(manifest)) {
      Map<String, Object> cached = json.readValue(manifest.toFile(), LinkedHashMap.class);
      for (var entry : cached.entrySet()) {
        Map<?, ?> value = (Map<?, ?>) entry.getValue();
        Path path = directory.resolve(entry.getKey() + ".csv");
        assertEquals(
            ((Number) value.get("bytes")).longValue(), Files.size(path), "Data file changed");
        assertEquals(value.get("sha256"), hashFile(path), "Data file changed");
      }
      return cached;
    }
    Map<String, Object> tables = new LinkedHashMap<>();
    for (TpchTable<?> table : TpchTable.getTables()) {
      Path path = directory.resolve(table.getTableName() + ".csv");
      long count = 0;
      try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
        for (TpchEntity entity : table.createGenerator(scale, 1, 1)) {
          String line = entity.toLine();
          writer.write(line.endsWith("|") ? line.substring(0, line.length() - 1) : line);
          writer.newLine();
          count++;
        }
      }
      tables.put(
          table.getTableName(),
          Map.of("rows", count, "bytes", Files.size(path), "sha256", hashFile(path)));
    }
    json.writerWithDefaultPrettyPrinter().writeValue(manifest.toFile(), tables);
    return tables;
  }

  static int[] orderFields(String sql, List<String> columns) {
    sql = sql.replaceAll("(?m)^\\s*--[^\\r\\n]*", "");
    var matcher = java.util.regex.Pattern.compile("(?i)\\border\\s+by\\b").matcher(sql);
    int end = -1;
    while (matcher.find()) end = matcher.end();
    if (end < 0) return new int[0];
    String clause =
        sql.substring(end).split("(?i)\\blimit\\b", 2)[0].trim().replaceFirst(";\\s*$", "");
    return java.util.Arrays.stream(clause.split(","))
        .mapToInt(
            term -> {
              String field = term.trim().split("\\s+")[0].replace("`", "");
              if (field.contains(".")) field = field.substring(field.lastIndexOf('.') + 1);
              for (int index = 0; index < columns.size(); index++)
                if (columns.get(index).equalsIgnoreCase(field)) return index;
              throw new IllegalArgumentException(
                  "TPC-H ORDER BY must name an output column: " + term);
            })
        .toArray();
  }

  static List<String> canonicalRows(List<String> rows, List<List<Object>> keys) {
    var result = new ArrayList<>(rows);
    for (int start = 0; start < rows.size(); ) {
      int end = start + 1;
      while (end < rows.size() && keys.get(start).equals(keys.get(end))) end++;
      result.subList(start, end).sort(String::compareTo);
      start = end;
    }
    return result;
  }

  private static String query(int number) throws Exception {
    var adapted = TpchBenchmark.class.getResourceAsStream("/tpch/flink-2.2.1/q" + number + ".sql");
    try (var input =
        adapted != null
            ? adapted
            : TpchTable.class.getResourceAsStream("/io/airlift/tpch/queries/q" + number + ".sql")) {
      return new String(input.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  static String hashFile(Path path) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    try (var input = Files.newInputStream(path)) {
      byte[] buffer = new byte[65536];
      for (int read; (read = input.read(buffer)) != -1; ) digest.update(buffer, 0, read);
    }
    return hex(digest.digest());
  }

  static String hash(String value) throws Exception {
    byte[] bytes =
        MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
    return hex(bytes);
  }

  private static String hex(byte[] bytes) {
    StringBuilder result = new StringBuilder();
    for (byte item : bytes) result.append(String.format("%02x", item & 255));
    return result.toString();
  }
}

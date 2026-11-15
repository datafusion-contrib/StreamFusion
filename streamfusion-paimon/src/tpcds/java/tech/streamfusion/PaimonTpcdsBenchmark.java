package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.tpcds.Results;
import io.trino.tpcds.Session;
import io.trino.tpcds.Table;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.stats.*;
import org.apache.flink.table.types.logical.RowType;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.paimon.PaimonTestCompat;
import tech.streamfusion.planner.NativePlanner;

/** Unchanged released Flink TPC-DS variants over identical Paimon inputs and committed outputs. */
@EnabledIfEnvironmentVariable(named = "SF_TPCDS", matches = "true")
class PaimonTpcdsBenchmark {
  private static final String RESOURCES = "/tpcds/flink-2.2.1/";
  private final ObjectMapper json = new ObjectMapper();

  public static void main(String[] args) throws Exception {
    if (!"true".equals(System.getenv("SF_TPCDS")))
      throw new IllegalStateException("Set SF_TPCDS=true to run the benchmark");
    new PaimonTpcdsBenchmark().fullSuite();
  }

  @Test
  void fullSuite() throws Exception {
    Path root = Path.of(System.getProperty("sf.tpcds.directory")).toAbsolutePath();
    Files.createDirectories(root);
    String mode = System.getProperty("sf.tpcds.mode", "audit");
    String computeMode = System.getProperty("sf.tpcds.compute", "native");
    boolean comparePreviousShuffle = Boolean.getBoolean("sf.tpcds.comparePreviousShuffle");
    assertTrue(
        !comparePreviousShuffle || (mode.equals("compare") && computeMode.equals("native")),
        "Previous-shuffle comparison requires compare mode and native computation");
    String shuffleProperty = "streamfusion.shuffle.nativeLz4.enabled";
    String originalShuffle = System.getProperty(shuffleProperty);
    long nativeLz4Bytes = 0;
    assertTrue(List.of("native", "stock").contains(computeMode));
    assertTrue(List.of("prepare", "plan", "audit", "verify", "compare").contains(mode));
    if (mode.equals("compare"))
      assertEquals(
          "false", System.getProperty("streamfusion.exchange.zeroCopyLocal"), "Use -Pbench");
    double scale = Double.parseDouble(System.getProperty("sf.tpcds.scale", "1"));
    int parallelism = Integer.getInteger("sf.tpcds.parallelism", 2);
    int warmups = mode.equals("compare") ? Integer.getInteger("sf.tpcds.warmups", 3) : 0;
    int trials = mode.equals("compare") ? Integer.getInteger("sf.tpcds.trials", 7) : 1;
    long nativeBytes = Long.getLong("sf.tpcds.nativeBytes", 512L << 20);
    boolean shuffleCompression =
        Boolean.parseBoolean(System.getProperty("sf.tpcds.shuffleCompression", "true"));
    var manifest = resourceJson("manifest.json");
    var schema = resourceJson("schema.json");
    assertEquals(103, manifest.get("queries").size(), "Full upstream TPC-DS variant inventory");
    assertEquals(24, schema.get("tables").size());
    var report = new LinkedHashMap<String, Object>();
    report.put("mode", mode);
    report.put("computeMode", computeMode);
    report.put("comparePreviousShuffle", comparePreviousShuffle);
    report.put(
        "engineOrder",
        comparePreviousShuffle
            ? "Rotate stock, streamfusion_java_lz4, streamfusion by trial"
            : "Alternate stock and StreamFusion by trial");
    var lz4Artifact =
        Path.of(
            net.jpountz.lz4.LZ4Factory.class
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .toURI());
    report.put("lz4JavaArtifact", lz4Artifact.getFileName().toString());
    if (Files.isRegularFile(lz4Artifact)) report.put("lz4JavaSha256", hashFile(lz4Artifact));
    report.put("scale", scale);
    report.put("generator", "io.trino.tpcds:tpcds:1.4");
    report.put("queryProvenance", manifest);
    report.put("schemaProvenance", schema);
    report.put("parallelism", parallelism);
    report.put("nativeBytes", nativeBytes);
    report.put("shuffleCompression", shuffleCompression);
    report.put("warmups", warmups);
    report.put("trials", trials);
    report.put("javaVersion", System.getProperty("java.version"));
    report.put("flinkVersion", org.apache.flink.runtime.util.EnvironmentInformation.getVersion());
    report.put(
        "paimonArtifact",
        Path.of(
                org.apache.paimon.CoreOptions.class
                    .getProtectionDomain()
                    .getCodeSource()
                    .getLocation()
                    .toURI())
            .getFileName()
            .toString());
    var libraries = new LinkedHashMap<String, Object>();
    for (String library :
        List.of(
            "streamfusion", "streamfusion_paimon", "streamfusion_parquet", "streamfusion_orc")) {
      for (String directory :
          System.getProperty("java.library.path").split(java.io.File.pathSeparator)) {
        Path file = Path.of(directory, System.mapLibraryName(library));
        if (Files.isRegularFile(file)) {
          libraries.put(
              library, Map.of("file", file.getFileName().toString(), "sha256", hashFile(file)));
          break;
        }
      }
    }
    report.put("nativeLibrariesOnExplicitPath", libraries);
    report.put("javaAssertionsEnabled", getClass().desiredAssertionStatus());
    report.put("os", System.getProperty("os.name") + " " + System.getProperty("os.arch"));
    report.put("maxHeapBytes", Runtime.getRuntime().maxMemory());
    report.put(
        "timing",
        "SQL submission through successful Paimon final commit; fixture and stock readback"
            + " excluded");
    var queries = new ArrayList<Map<String, Object>>();
    report.put("queries", queries);
    var config = new Configuration();
    config.setString("parallelism.default", Integer.toString(parallelism));
    config.setString("table.exec.resource.default-parallelism", Integer.toString(parallelism));
    config.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
    config.setString("table.optimizer.range-sort.enabled", "false");
    config.setString("table.optimizer.join-reorder-enabled", "true");
    config.setString("table.optimizer.join.broadcast-threshold", "10485760");
    config.setString("taskmanager.memory.managed.size", "1024m");
    config.setString(
        "taskmanager.network.blocking-shuffle.compression.enabled",
        Boolean.toString(shuffleCompression));
    config.setString("taskmanager.memory.task.off-heap.size", nativeBytes + "b");
    config.setString("streamfusion.transpose.batchRows", "8192");
    var failures = new ArrayList<String>();
    try (var metrics =
        Boolean.getBoolean("sf.tpcds.retainGauges")
            ? new RecoveryExecutionMetrics()
            : RecoveryExecutionMetrics.countersOnly()) {
      metrics.setAsContext(config, parallelism, nativeBytes);
      var tables = prepare(root.resolve("input-sf-" + scale), schema, scale);
      report.put("tables", tables);
      if (mode.equals("prepare")) {
        json.writerWithDefaultPrettyPrinter()
            .writeValue(root.resolve("report-prepare.json").toFile(), report);
        return;
      }
      var stock = environment(config, false);
      var nativeEnv = environment(config, true);
      for (var tableSchema : schema.get("tables")) {
        String name = tableSchema.get("name").asText();
        String create =
            ddl(name, columns(tableSchema), root.resolve("input-sf-" + scale).resolve(name));
        stock.executeSql(create);
        nativeEnv.executeSql(create);
        registerStatistics(
            stock,
            name,
            tableSchema,
            json.valueToTree(((Map<?, ?>) tables.get("tables")).get(name)));
        registerStatistics(
            nativeEnv,
            name,
            tableSchema,
            json.valueToTree(((Map<?, ?>) tables.get("tables")).get(name)));
      }
      String selection = "," + System.getProperty("sf.tpcds.queries", "") + ",";
      report.put("completeWorkload", selection.equals(",,"));
      for (var query : manifest.get("queries")) {
        String id = query.get("id").asText();
        if (!selection.equals(",,") && !selection.contains("," + id + ",")) continue;
        var entry = new LinkedHashMap<String, Object>();
        entry.put("query", id);
        queries.add(entry);
        try {
          String sql = resource(query.get("file").asText());
          assertEquals(query.get("sha256").asText(), hash(sql));
          String stockPlan = stock.explainSql(sql);
          String nativePlan = nativeEnv.explainSql(sql);
          boolean nativeCompute =
              nativePlan.contains("NativeAggregate")
                  || nativePlan.contains("NativeJoin")
                  || nativePlan.contains("NativeCalc")
                  || nativePlan.contains("NativeSort")
                  || nativePlan.contains("NativeWindow")
                  || nativePlan.contains("NativeExpand")
                  || nativePlan.contains("NativeLimit");
          entry.put("sqlSha256", hash(sql));
          entry.put("stockPlan", stockPlan);
          entry.put("nativePlan", nativePlan);
          entry.put("admission", NativePlanner.explain(nativeEnv, sql));
          entry.put("nativeComputePlanned", nativeCompute);
          json.writerWithDefaultPrettyPrinter()
              .writeValue(root.resolve("report-" + mode + ".json").toFile(), report);
          if (mode.equals("plan")) {
            assertEquals(
                computeMode.equals("native"), nativeCompute, "Q" + id + " planned compute route");
            assertTrue(nativePlan.contains("NativeConnectorSource"), "Q" + id + " source fallback");
          }
          if (!mode.equals("plan")) {
            var runs = new ArrayList<Map<String, Object>>();
            entry.put("runs", runs);
            List<String> expected = null;
            String expectedTypes = null;
            for (int trial = -warmups; trial < trials; trial++) {
              int engines = comparePreviousShuffle ? 3 : 2;
              for (int position = 0; position < engines; position++) {
                int engine =
                    comparePreviousShuffle
                        ? Math.floorMod(trial + warmups + position, 3)
                        : (trial % 2 == 0 ? position : 1 - position);
                boolean nativeRun = engine != 0;
                if (comparePreviousShuffle)
                  System.setProperty(shuffleProperty, Boolean.toString(engine != 1));
                var observed =
                    execute(nativeRun ? nativeEnv : stock, stock, sql, root, id, trial, nativeRun);
                if (comparePreviousShuffle && engine == 1)
                  observed.put("engine", "streamfusion_java_lz4");
                boolean nativeLz4 =
                    nativeRun && Boolean.parseBoolean(System.getProperty(shuffleProperty, "true"));
                observed.put("nativeLz4Enabled", nativeLz4);
                String job = (String) observed.get("jobId");
                long sourceRows = metrics.counterValue(job, "nativePaimonSourceRows");
                var work =
                    metrics.nativeOperators().stream()
                        .filter(item -> job.equals(item.get("jobId")))
                        .collect(java.util.stream.Collectors.toList());
                observed.put("nativeWork", work);
                observed.put("sourceRows", sourceRows);
                for (String metric :
                    List.of(
                        "nativeLz4InputGates",
                        "nativeLz4Blocks",
                        "nativeLz4Bytes",
                        "nativeLz4FallbackGates"))
                  observed.put(metric, metrics.counterValue(job, metric));
                if (nativeLz4)
                  assertEquals(0L, observed.get("nativeLz4FallbackGates"), "Native LZ4 fallback");
                else assertEquals(0L, observed.get("nativeLz4Blocks"), "Native LZ4 disabled");
                if (nativeLz4)
                  nativeLz4Bytes += ((Number) observed.get("nativeLz4Bytes")).longValue();
                observed.put(
                    "sourceFallbackRows", metrics.counterValue(job, "paimonSourceFallbackRows"));
                observed.put(
                    "entryTransposeRows",
                    metrics.counterValue(job, "numInputRows", "RowDataToArrow"));
                @SuppressWarnings("unchecked")
                List<String> rows = (List<String>) observed.remove("result");
                String types = (String) observed.get("resultType");
                if (expected == null) {
                  expected = rows;
                  expectedTypes = types;
                } else {
                  assertEquals(expectedTypes, types, "Q" + id + " types");
                  assertEquals(expected, rows, "Q" + id + " rows");
                }
                if (nativeRun) {
                  if (mode.equals("verify") || mode.equals("compare")) {
                    assertEquals(
                        computeMode.equals("native"), nativeCompute, "Q" + id + " compute route");
                    assertTrue(sourceRows > 0, "Q" + id + " native source missing");
                    assertEquals(
                        0L, observed.get("sourceFallbackRows"), "Q" + id + " source fallback");
                    if (computeMode.equals("native"))
                      assertEquals(0L, observed.get("entryTransposeRows"), "Q" + id + " row entry");
                    boolean computeWork =
                        work.stream()
                            .filter(
                                item ->
                                    List.of(
                                            "NativeCalc",
                                            "NativeAggregate",
                                            "NativeJoin",
                                            "NativeSort",
                                            "NativeWindow",
                                            "NativeExpand",
                                            "NativeLimit")
                                        .stream()
                                        .anyMatch(
                                            prefix ->
                                                item.get("operator").toString().startsWith(prefix)))
                            .anyMatch(item -> ((Number) item.get("recordsIn")).longValue() > 0);
                    assertEquals(
                        computeMode.equals("native"),
                        computeWork,
                        "Q" + id + " actual compute route");
                  }
                } else {
                  assertEquals(0, sourceRows);
                  assertTrue(work.isEmpty(), "Stock must not enter native compute");
                  assertEquals(
                      0L, observed.get("nativeLz4Blocks"), "Stock shuffle must stay stock");
                }
                runs.add(observed);
                System.out.printf(
                    "TPCDS_TRIAL q=%s engine=%s trial=%d elapsedMs=%.3f rows=%d sourceRows=%d"
                        + " nativeComputePlanned=%s%n",
                    id,
                    observed.get("engine"),
                    trial,
                    ((Number) observed.get("elapsedNanos")).longValue() / 1_000_000.0,
                    rows.size(),
                    sourceRows,
                    nativeCompute);
              }
            }
          }
          entry.put("status", "passed");
        } catch (Exception | AssertionError failure) {
          entry.put("status", "failed");
          entry.put("error", failure.toString());
          var causes = new ArrayList<String>();
          for (Throwable cause = failure; cause != null; cause = cause.getCause())
            causes.add(cause.toString());
          entry.put("causes", causes);
          failures.add(id + ": " + failure);
          failure.printStackTrace();
        } finally {
          json.writerWithDefaultPrettyPrinter()
              .writeValue(root.resolve("report-" + mode + ".json").toFile(), report);
        }
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
      if (originalShuffle == null) System.clearProperty(shuffleProperty);
      else System.setProperty(shuffleProperty, originalShuffle);
    }
    assertFalse(queries.isEmpty(), "No query variants selected");
    assertTrue(failures.isEmpty(), String.join("\n", failures));
    if (comparePreviousShuffle && shuffleCompression)
      assertTrue(nativeLz4Bytes > 0, "The comparison must execute native LZ4 decoding");
  }

  private TableEnvironment environment(Configuration config, boolean nativeRun) throws Exception {
    var options = new Configuration(config);
    options.setString("streamfusion.native.enabled", Boolean.toString(nativeRun));
    if (nativeRun && System.getProperty("sf.tpcds.compute", "native").equals("stock"))
      options.setString("streamfusion.operator.calc.enabled", "false");
    return NativePlannerTestEnvironment.createBatch(options);
  }

  private Map<String, Object> prepare(Path path, JsonNode schema, double scale) throws Exception {
    Path marker = path.resolve(".streamfusion-tpcds-owner");
    Path manifest = path.resolve("manifest.json");
    if (Files.exists(manifest)
        && json.readTree(manifest.toFile()).path("statisticsVersion").asInt() == 1) {
      assertTrue(Files.exists(marker), "Refusing to reuse unowned fixture data");
      var cached = json.readValue(manifest.toFile(), LinkedHashMap.class);
      assertEquals(scale, ((Number) cached.get("scale")).doubleValue());
      assertEquals(hash(schema.toString()), cached.get("schemaSha256"));
      assertEquals("io.trino.tpcds:tpcds:1.4", cached.get("generator"));
      for (var item : (List<Map<String, Object>>) cached.get("files")) {
        Path file = path.resolve((String) item.get("path"));
        assertEquals(((Number) item.get("bytes")).longValue(), Files.size(file));
        assertEquals(item.get("sha256"), hashFile(file));
      }
      return cached;
    }
    if (Files.exists(marker)) delete(path);
    Files.createDirectories(path);
    try (var files = Files.list(path)) {
      assertTrue(files.findAny().isEmpty(), "Refusing to replace unowned data");
    }
    Files.writeString(marker, "StreamFusion TPC-DS fixture\n");
    var prepared = new LinkedHashMap<String, Object>();
    var tables = new LinkedHashMap<String, Object>();
    var ndv = resourceJson("ndv.json").get("ndv");
    for (var def : schema.get("tables")) {
      String name = def.get("name").asText();
      var table = Table.getTable(name);
      assertEquals(table.getColumns().length, def.get("columns").size(), name);
      for (int i = 0; i < table.getColumns().length; i++) {
        String generated = table.getColumns()[i].getName().toLowerCase(Locale.ROOT);
        if (generated.equals("p_response_targe")) generated = "p_response_target";
        assertEquals(generated, def.get("columns").get(i).get("name").asText());
      }
      var rowFields = new ArrayList<RowType.RowField>();
      for (var c : def.get("columns"))
        rowFields.add(
            new RowType.RowField(
                c.get("name").asText(),
                org.apache.flink.table.types.logical.utils.LogicalTypeParser.parse(
                    c.get("type").asText(), getClass().getClassLoader())));
      create(path.resolve(name), new RowType(rowFields));
      var paimon = table(path.resolve(name));
      var builder = paimon.newStreamWriteBuilder().withCommitUser("tpcds-fixture");
      long count = 0;
      var statistics = new ArrayList<ObservedColumn>();
      for (var c : def.get("columns")) statistics.add(new ObservedColumn(c.get("type").asText()));
      try (var write = builder.newWrite();
          var commit = builder.newCommit()) {
        var session = Session.getDefaultSession().withScale(scale).withTable(table);
        for (var batch : Results.constructResults(table, session))
          for (var fields : batch) {
            assertEquals(def.get("columns").size(), fields.size(), name);
            var row = new GenericRow(fields.size());
            for (int i = 0; i < fields.size(); i++) {
              statistics.get(i).add(fields.get(i));
              row.setField(i, value(fields.get(i), def.get("columns").get(i).get("type").asText()));
            }
            write.write(row);
            count++;
          }
        commit.commit(1, write.prepareCommit(true, 1));
      }
      var info = inspect(path.resolve(name), false);
      assertEquals(count, ((Number) info.get("rows")).longValue());
      var columnStats = new LinkedHashMap<String, Object>();
      for (int i = 0; i < statistics.size(); i++) {
        String column = def.get("columns").get(i).get("name").asText();
        columnStats.put(column, statistics.get(i).summary(count, ndv.get(column).asLong()));
      }
      info.put("columnStatistics", columnStats);
      tables.put(name, info);
      System.out.println("TPCDS_FIXTURE " + name + " rows=" + count);
    }
    prepared.put("statisticsVersion", 1);
    prepared.put(
        "statistics",
        "Observed row/null counts, integral/date bounds and string lengths; released Flink NDV"
            + " estimates capped by non-null rows");
    prepared.put("ndvProvenance", resourceJson("ndv.json"));
    prepared.put("scale", scale);
    prepared.put("schemaSha256", hash(schema.toString()));
    prepared.put("tables", tables);
    prepared.put("generator", "io.trino.tpcds:tpcds:1.4");
    prepared.put("generatorColumnNameAliases", Map.of("p_response_targe", "p_response_target"));
    var files = new ArrayList<Map<String, Object>>();
    try (var paths = Files.walk(path)) {
      for (var file :
          paths
              .filter(Files::isRegularFile)
              .filter(f -> !f.getFileName().toString().startsWith("."))
              .sorted()
              .collect(java.util.stream.Collectors.toList()))
        files.add(
            Map.of(
                "path",
                path.relativize(file).toString(),
                "bytes",
                Files.size(file),
                "sha256",
                hashFile(file)));
    }
    prepared.put("files", files);
    prepared.put("writer", "released Java Paimon SDK, Snappy Parquet, outside timing");
    json.writerWithDefaultPrettyPrinter().writeValue(manifest.toFile(), prepared);
    return prepared;
  }

  private static final class ObservedColumn {
    private final String type;
    private long nulls, maxLength, totalLength;
    private Long min, max;

    private ObservedColumn(String type) {
      this.type = type;
    }

    private void add(String raw) {
      if (raw == null || raw.isEmpty()) {
        nulls++;
        return;
      }
      if (type.equals("STRING")) {
        maxLength = Math.max(maxLength, raw.length());
        totalLength += raw.length();
      } else if (type.equals("BIGINT") || type.equals("INT") || type.equals("DATE")) {
        long v = type.equals("DATE") ? LocalDate.parse(raw).toEpochDay() : Long.parseLong(raw);
        min = min == null ? v : Math.min(min, v);
        max = max == null ? v : Math.max(max, v);
      }
    }

    private Map<String, Object> summary(long rows, long estimatedNdv) {
      var info = new LinkedHashMap<String, Object>();
      info.put("nulls", nulls);
      info.put("ndv", Math.min(estimatedNdv, rows - nulls));
      info.put("min", min);
      info.put("max", max);
      info.put("maxLength", maxLength);
      info.put("avgLength", rows == nulls ? 0.0 : totalLength / (double) (rows - nulls));
      return info;
    }
  }

  private void registerStatistics(
      TableEnvironment env, String name, JsonNode schema, JsonNode observed) throws Exception {
    var catalog = env.getCatalog(env.getCurrentCatalog()).orElseThrow();
    var path = new ObjectPath(env.getCurrentDatabase(), name);
    catalog.alterTableStatistics(
        path,
        new CatalogTableStatistics(
            observed.get("rows").asLong(), observed.get("files").asInt(), -1, -1),
        false);
    var columns = new LinkedHashMap<String, CatalogColumnStatisticsDataBase>();
    for (var column : schema.get("columns")) {
      String key = column.get("name").asText(), type = column.get("type").asText();
      var info = observed.get("columnStatistics").get(key);
      long ndv = info.get("ndv").asLong(), nulls = info.get("nulls").asLong();
      Long min = info.get("min").isNull() ? null : info.get("min").asLong();
      Long max = info.get("max").isNull() ? null : info.get("max").asLong();
      CatalogColumnStatisticsDataBase stats;
      if (type.equals("STRING"))
        stats =
            new CatalogColumnStatisticsDataString(
                info.get("maxLength").asLong(), info.get("avgLength").asDouble(), ndv, nulls);
      else if (type.equals("DATE"))
        stats =
            new CatalogColumnStatisticsDataDate(
                min == null ? null : new org.apache.flink.table.catalog.stats.Date(min),
                max == null ? null : new org.apache.flink.table.catalog.stats.Date(max),
                ndv,
                nulls);
      else if (type.startsWith("DECIMAL"))
        stats = new CatalogColumnStatisticsDataDouble(null, null, ndv, nulls);
      else stats = new CatalogColumnStatisticsDataLong(min, max, ndv, nulls);
      columns.put(key, stats);
    }
    catalog.alterTableColumnStatistics(path, new CatalogColumnStatistics(columns), false);
  }

  private Object value(String raw, String type) {
    if (raw == null || raw.isEmpty()) return null;
    if (type.equals("BIGINT")) return Long.parseLong(raw);
    if (type.equals("INT")) return Integer.parseInt(raw);
    if (type.equals("DATE")) return Math.toIntExact(LocalDate.parse(raw).toEpochDay());
    if (type.startsWith("DECIMAL")) {
      String[] args = type.substring(8, type.length() - 1).split(",");
      return java.util.Objects.requireNonNull(
          Decimal.fromBigDecimal(
              new BigDecimal(raw), Integer.parseInt(args[0]), Integer.parseInt(args[1])),
          "Decimal overflow " + raw);
    }
    assertEquals("STRING", type);
    return BinaryString.fromString(raw);
  }

  private Map<String, Object> execute(
      TableEnvironment env,
      TableEnvironment verifier,
      String sql,
      Path root,
      String id,
      int trial,
      boolean nativeRun)
      throws Exception {
    var schema = env.sqlQuery(sql).getResolvedSchema();
    var fields = new ArrayList<RowType.RowField>();
    for (int i = 0; i < schema.getColumnCount(); i++)
      fields.add(
          new RowType.RowField("c" + i, schema.getColumnDataTypes().get(i).getLogicalType()));
    String name =
        "tpcds_result_q" + id + "_" + (nativeRun ? "native" : "stock") + "_" + (trial + 100);
    Path path = root.resolve("outputs").resolve(name);
    assertFalse(Files.exists(path), "Refusing to replace an existing query output: " + path);
    create(path, new RowType(fields));
    String create =
        ddl(
            name,
            java.util.stream.IntStream.range(0, fields.size())
                .mapToObj(i -> "c" + i + " " + fields.get(i).getType().asSerializableString())
                .collect(java.util.stream.Collectors.joining(",")),
            path);
    env.executeSql(create);
    if (env != verifier) verifier.executeSql(create);
    try {
      String insert = "INSERT INTO " + name + " " + sql;
      String outputPlan = env.explainSql(insert);
      boolean nativeSink = outputPlan.contains("NativeConnectorSink");
      assertEquals(nativeRun, nativeSink, outputPlan);
      long start = System.nanoTime();
      var result = env.executeSql(insert);
      result.await();
      var client = result.getJobClient().orElseThrow();
      long runtime = client.getJobExecutionResult().get().getNetRuntime();
      long elapsed = System.nanoTime() - start;
      var readback = verifier.executeSql("SELECT * FROM " + name);
      assertEquals(schema.getColumnDataTypes(), readback.getResolvedSchema().getColumnDataTypes());
      var rows = new ArrayList<String>();
      try (var iterator = readback.collect()) {
        while (iterator.hasNext()) {
          var row = iterator.next();
          var values = new ArrayList<String>();
          for (int i = 0; i < row.getArity(); i++) {
            var value = row.getField(i);
            values.add(
                value == null
                    ? null
                    : value instanceof BigDecimal
                        ? ((BigDecimal) value).toPlainString()
                        : value instanceof Double
                            ? Long.toHexString(Double.doubleToLongBits((Double) value))
                            : value instanceof Float
                                ? Integer.toHexString(Float.floatToIntBits((Float) value))
                                : value.toString());
          }
          rows.add(json.writeValueAsString(values));
        }
      }
      readback.getJobClient().orElseThrow().getJobExecutionResult().get();
      rows.sort(String::compareTo);
      var observed = inspect(path, nativeRun);
      assertEquals((long) rows.size(), ((Number) observed.get("rows")).longValue());
      observed.put(
          "engine",
          nativeRun
              ? System.getProperty("sf.tpcds.compute", "native").equals("stock")
                  ? "native_io_stock_compute"
                  : "streamfusion"
              : "stock");
      observed.put("trial", trial);
      observed.put("elapsedNanos", elapsed);
      observed.put("netRuntimeMillis", runtime);
      observed.put("jobId", client.getJobID().toString());
      observed.put("nativeSinkPlanned", nativeSink);
      observed.put("outputPlanSha256", hash(outputPlan));
      observed.put("resultType", schema.toPhysicalRowDataType().toString());
      observed.put("resultSha256", hash(String.join("\n", rows)));
      observed.put("result", rows);
      return observed;
    } finally {
      if (env != verifier) verifier.executeSql("DROP TABLE " + name);
      env.executeSql("DROP TABLE " + name);
      delete(path);
    }
  }

  private static void create(Path path, RowType type) throws Exception {
    var paimon =
        (org.apache.paimon.types.RowType)
            org.apache.paimon.flink.LogicalTypeConversion.toDataType(type);
    PaimonTestCompat.schemaManager(
            LocalFileIO.create(), new org.apache.paimon.fs.Path(path.toUri()))
        .createTable(
            new org.apache.paimon.schema.Schema(
                paimon.getFields(),
                List.of(),
                List.of(),
                Map.of(
                    "bucket",
                    "-1",
                    "file.format",
                    "parquet",
                    "file.compression",
                    "snappy",
                    "write-only",
                    "true"),
                null));
  }

  private static String ddl(String name, String fields, Path path) {
    return "CREATE TABLE "
        + name
        + " ("
        + fields
        + ") WITH ('connector'='paimon','path'='"
        + path.toUri().toString().replace("'", "''")
        + "','bucket'='-1','file.format'='parquet','file.compression'='snappy','write-only'='true','sink.parallelism'='"
        + Integer.getInteger("sf.tpcds.parallelism", 2)
        + "')";
  }

  private String columns(JsonNode def) {
    var values = new ArrayList<String>();
    for (var c : def.get("columns"))
      values.add(c.get("name").asText() + " " + c.get("type").asText());
    return String.join(",", values);
  }

  private static FileStoreTable table(Path path) {
    return FileStoreTableFactory.create(
        LocalFileIO.create(), new org.apache.paimon.fs.Path(path.toUri()));
  }

  private Map<String, Object> inspect(Path path, boolean nativeWriter) throws Exception {
    var table = table(path);
    long rows = 0;
    int files = 0;
    int nativeFiles = 0;
    var partitions = new java.util.HashSet<org.apache.paimon.data.BinaryRow>();
    var buckets = new java.util.TreeSet<Integer>();
    for (var entry : table.store().newScan().plan().files()) {
      partitions.add(entry.partition());
      buckets.add(entry.bucket());
      var file = entry.file();
      var filePath =
          new org.apache.paimon.fs.Path(
              table.store().pathFactory().bucketPath(entry.partition(), entry.bucket()),
              file.fileName());
      try (var reader = PaimonTestCompat.parquetReader(table.fileIO(), filePath, file.fileSize())) {
        if (reader.getFooter().getFileMetaData().getCreatedBy().contains("parquet-rs"))
          nativeFiles++;
      }
      rows += file.rowCount();
      files++;
    }
    assertEquals(nativeWriter ? files : 0, nativeFiles, path.toString());
    var info = new LinkedHashMap<String, Object>();
    info.put("rows", rows);
    info.put("files", files);
    info.put("nativeFiles", nativeFiles);
    info.put("partitionKeys", table.partitionKeys());
    info.put("partitionCount", partitions.size());
    info.put("buckets", buckets);
    info.put("scanSplits", table.newReadBuilder().newScan().plan().splits().size());
    info.put("bucketMode", table.bucketMode().name());
    info.put("snapshot", table.snapshotManager().latestSnapshotId());
    return info;
  }

  private JsonNode resourceJson(String file) throws Exception {
    return json.readTree(resource(file));
  }

  private String resource(String file) throws Exception {
    try (var input = getClass().getResourceAsStream(RESOURCES + file)) {
      assertNotNull(input, file);
      return new String(input.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private static String hash(String value) throws Exception {
    byte[] bytes =
        MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
    var text = new StringBuilder();
    for (byte b : bytes) text.append(String.format("%02x", b));
    return text.toString();
  }

  private static String hashFile(Path path) throws Exception {
    var digest = MessageDigest.getInstance("SHA-256");
    try (var input = Files.newInputStream(path)) {
      byte[] bytes = new byte[1 << 20];
      int size;
      while ((size = input.read(bytes)) >= 0) digest.update(bytes, 0, size);
    }
    var text = new StringBuilder();
    for (byte b : digest.digest()) text.append(String.format("%02x", b));
    return text.toString();
  }

  private static void delete(Path path) throws Exception {
    if (!Files.exists(path)) return;
    try (var paths = Files.walk(path)) {
      for (var item :
          paths.sorted(Comparator.reverseOrder()).collect(java.util.stream.Collectors.toList()))
        Files.delete(item);
    }
  }
}

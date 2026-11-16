package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.tpch.TpchTable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.DataSplit;
import tech.streamfusion.paimon.PaimonTestCompat;

/** The unchanged TPC-H queries, with stock-created Paimon inputs and committed Paimon outputs. */
class PaimonTpchBenchmark extends TpchBenchmark {
  private TableEnvironment verifier;

  @Override
  String inputFormat() {
    return "paimon";
  }

  @Override
  long nativeSourceRows(RecoveryExecutionMetrics metrics, String job) {
    assertEquals(0, metrics.counterValue(job, "paimonSourceFallbackRows"));
    return metrics.counterValue(job, "nativePaimonSourceRows");
  }

  @Override
  Path prepareInput(
      Path root,
      Path csv,
      Configuration config,
      int parallelism,
      String format,
      Map<String, Object> report)
      throws Exception {
    Path directory =
        root.resolve(csv.getFileName().toString().replace("data-sf-", "data-paimon-sf-"));
    Files.createDirectories(directory);
    Path manifest = directory.resolve("manifest.json");
    var mapper = new ObjectMapper();
    var prepared = new LinkedHashMap<String, Object>();
    String source = hashFile(csv.resolve("manifest.json"));
    if (Files.exists(manifest)) {
      prepared.putAll(mapper.readValue(manifest.toFile(), LinkedHashMap.class));
      assertEquals(source, prepared.get("sourceCsvManifestSha256"));
      for (var file : (List<Map<String, Object>>) prepared.get("files")) {
        Path path = directory.resolve((String) file.get("path"));
        assertEquals(((Number) file.get("bytes")).longValue(), Files.size(path));
        assertEquals(file.get("sha256"), hashFile(path));
      }
    } else {
      if (Files.exists(directory.resolve(".streamfusion-tpch-owner"))) {
        removeGenerated(directory);
        Files.createDirectories(directory);
      }
      try (var paths = Files.list(directory)) {
        assertTrue(paths.findAny().isEmpty(), "Refusing to replace unowned Paimon data");
      }
      Files.writeString(
          directory.resolve(".streamfusion-tpch-owner"), "StreamFusion TPC-H generated data\n");
      var loader = super.environment(false, config, parallelism, csv, "csv");
      var tables = new LinkedHashMap<String, Object>();
      for (TpchTable<?> table : TpchTable.getTables()) {
        String name = "load_" + table.getTableName();
        Path destination = directory.resolve(table.getTableName());
        createPhysicalTable(
            destination,
            (org.apache.flink.table.types.logical.RowType)
                loader
                    .from(table.getTableName())
                    .getResolvedSchema()
                    .toPhysicalRowDataType()
                    .getLogicalType());
        loader.executeSql(ddl(name, columns(table), destination));
        var result =
            loader.executeSql("INSERT INTO " + name + " SELECT * FROM " + table.getTableName());
        result.await();
        var client = result.getJobClient().orElseThrow();
        client.getJobExecutionResult().get();
        var inspection = inspect(destination, false);
        inspection.put("writerJobId", client.getJobID().toString());
        tables.put(table.getTableName(), inspection);
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
      prepared.put(
          "writer", "stock Flink Paimon connector, append tables, SNAPPY Parquet, outside timing");
      prepared.put("sourceCsvManifestSha256", source);
      prepared.put("tables", tables);
      prepared.put("files", files);
      mapper.writerWithDefaultPrettyPrinter().writeValue(manifest.toFile(), prepared);
    }
    report.put("paimon", prepared);
    report.put(
        "sink",
        "Paimon append table; timing ends after final commit; stock readback outside timing");
    report.put(
        "resultSemantics",
        "Declared SQL types and complete row multiset; Paimon tables are unordered");
    report.put("previousBoundaries", Boolean.getBoolean("sf.tpch.paimon.previous"));
    return directory;
  }

  @Override
  TableEnvironment environment(
      boolean nativeRun, Configuration config, int parallelism, Path directory, String format)
      throws Exception {
    if (!format.equals("paimon"))
      return super.environment(nativeRun, config, parallelism, directory, format);
    var options = new Configuration(config);
    options.setString("streamfusion.native.enabled", Boolean.toString(nativeRun));
    options.setString("parallelism.default", Integer.toString(parallelism));
    if (Boolean.getBoolean("sf.tpch.paimon.previous")) {
      options.setString("streamfusion.operator.paimonSource.enabled", "false");
      options.setString("streamfusion.operator.paimonSink.enabled", "false");
    }
    var env = NativePlannerTestEnvironment.createBatch(options);
    for (TpchTable<?> table : TpchTable.getTables())
      env.executeSql(
          ddl(table.getTableName(), columns(table), directory.resolve(table.getTableName())));
    if (!nativeRun) verifier = env;
    return env;
  }

  @Override
  QueryRun executeQuery(
      TableEnvironment environment, String sql, Path root, int number, int trial, boolean nativeRun)
      throws Exception {
    var schema = environment.sqlQuery(sql).getResolvedSchema();
    var fields = new ArrayList<String>();
    for (int index = 0; index < schema.getColumnCount(); index++)
      fields.add(
          "c"
              + index
              + " "
              + schema.getColumnDataTypes().get(index).getLogicalType().asSerializableString());
    String name =
        "tpch_output_q" + number + "_" + (nativeRun ? "native" : "stock") + "_" + (trial + 100);
    Path path = root.resolve("paimon-results").resolve(name);
    var sinkFields = new ArrayList<org.apache.flink.table.types.logical.RowType.RowField>();
    for (int index = 0; index < schema.getColumnCount(); index++)
      sinkFields.add(
          new org.apache.flink.table.types.logical.RowType.RowField(
              "c" + index, schema.getColumnDataTypes().get(index).getLogicalType()));
    createPhysicalTable(path, new org.apache.flink.table.types.logical.RowType(sinkFields));
    String create = ddl(name, String.join(",", fields), path);
    environment.executeSql(create);
    if (environment != verifier) verifier.executeSql(create);
    try {
      long started = System.nanoTime();
      var result = environment.executeSql("INSERT INTO " + name + " " + sql);
      result.await();
      var client = result.getJobClient().orElseThrow();
      long runtime = client.getJobExecutionResult().get().getNetRuntime();
      long nanos = System.nanoTime() - started;
      var readback = verifier.executeSql("SELECT * FROM " + name);
      assertEquals(schema.getColumnDataTypes(), readback.getResolvedSchema().getColumnDataTypes());
      var rows = new ArrayList<String>();
      try (var iterator = readback.collect()) {
        while (iterator.hasNext()) rows.add(iterator.next().toString());
      }
      readback.getJobClient().orElseThrow().getJobExecutionResult().get();
      rows.sort(String::compareTo);
      boolean nativeWriter = nativeRun && !Boolean.getBoolean("sf.tpch.paimon.previous");
      var details = inspect(path, nativeWriter);
      assertEquals((long) rows.size(), ((Number) details.get("committedRows")).longValue());
      details.put("sinkCommitIncluded", true);
      details.put("readback", "stock Flink Paimon connector");
      return new QueryRun(
          schema.toPhysicalRowDataType().toString(), rows, client, runtime, nanos, details);
    } finally {
      if (environment != verifier) verifier.executeSql("DROP TABLE " + name);
      environment.executeSql("DROP TABLE " + name);
      removeGenerated(path);
    }
  }

  private static void createPhysicalTable(
      Path path, org.apache.flink.table.types.logical.RowType flinkType) throws Exception {
    var type =
        (org.apache.paimon.types.RowType)
            org.apache.paimon.flink.LogicalTypeConversion.toDataType(flinkType);
    var schema =
        new org.apache.paimon.schema.Schema(
            type.getFields(),
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
            null);
    PaimonTestCompat.schemaManager(
            LocalFileIO.create(), new org.apache.paimon.fs.Path(path.toUri()))
        .createTable(schema);
  }

  private static String ddl(String name, String fields, Path path) {
    return "CREATE TABLE "
        + name
        + " ("
        + fields
        + ") WITH ('connector'='paimon','path'='"
        + path.toUri().toString().replace("'", "''")
        + "','bucket'='-1','file.format'='parquet','file.compression'='snappy','write-only'='true')";
  }

  private static LinkedHashMap<String, Object> inspect(Path path, boolean nativeWriter)
      throws Exception {
    var table =
        FileStoreTableFactory.create(
            LocalFileIO.create(), new org.apache.paimon.fs.Path(path.toUri()));
    long rows = 0;
    int files = 0;
    var partitions = new java.util.HashSet<org.apache.paimon.data.BinaryRow>();
    var buckets = new java.util.TreeSet<Integer>();
    for (var split : table.newReadBuilder().newScan().plan().splits()) {
      var data = (DataSplit) split;
      partitions.add(data.partition());
      buckets.add(data.bucket());
      for (var file : data.dataFiles()) {
        try (var reader =
            PaimonTestCompat.parquetReader(
                table.fileIO(),
                new org.apache.paimon.fs.Path(data.bucketPath(), file.fileName()),
                file.fileSize())) {
          assertEquals(
              nativeWriter,
              reader.getFooter().getFileMetaData().getCreatedBy().contains("parquet-rs"));
        }
        rows += file.rowCount();
        files++;
      }
    }
    var result = new LinkedHashMap<String, Object>();
    result.put("committedRows", rows);
    result.put("files", files);
    result.put("nativeWriter", nativeWriter);
    result.put("snapshot", table.snapshotManager().latestSnapshotId());
    result.put("partitionKeys", table.partitionKeys());
    result.put("partitionCount", partitions.size());
    result.put("bucketMode", table.bucketMode().name());
    result.put("buckets", buckets);
    return result;
  }
}

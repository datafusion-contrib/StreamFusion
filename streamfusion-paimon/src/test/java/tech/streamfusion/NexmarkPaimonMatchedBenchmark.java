package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.paimon.format.parquet.ParquetUtil;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import tech.streamfusion.compat.ListCollectors;

/** Repeated, alternating stock/native pairs using the unchanged Kafka and Paimon SQL fixtures. */
@EnabledIfEnvironmentVariable(named = "SF_PAIMON_MATCHED", matches = "true")
class NexmarkPaimonMatchedBenchmark {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final long EVENTS =
      Long.parseLong(System.getenv().getOrDefault("SF_ROWS", "2000000"));
  private static final int WARMUPS =
      Integer.parseInt(System.getenv().getOrDefault("SF_WARMUP", "1"));
  private static final int RUNS = Integer.parseInt(System.getenv().getOrDefault("SF_RUNS", "3"));
  private Path artifacts;

  @Test
  void compare() throws Exception {
    assertFalse(Boolean.getBoolean("streamfusion.exchange.zeroCopyLocal"));
    assertEquals("disk", System.getenv("SF_LAKE_STATE_BACKEND"));
    assertEquals("true", System.getenv("SF_PAIMON_RELEASE_CONFIRMED"));
    assertEquals("4", System.getenv("SF_PARALLELISM"));
    assertEquals("4", System.getenv("SF_KAFKA_PARTITIONS"));
    artifacts = Path.of(System.getenv("SF_PAIMON_ARTIFACTS")).toAbsolutePath();
    Files.createDirectories(artifacts);
    assertFalse(
        Files.exists(artifacts.resolve("observations.jsonl")), "use a fresh evidence directory");
    var queries = NexmarkMatrixBenchmark.selectQueries();
    var expected = new ArrayList<List<String>>();
    for (var q : queries) {
      expected.add(List.of(q.label, "exact"));
      if (variants() && q.nativeVariantProps != null)
        expected.add(List.of(q.label, q.nativeVariantLabel));
    }
    var method = new LinkedHashMap<String, Object>();
    method.put("events", EVENTS);
    var defaults = new org.apache.paimon.CoreOptions(new org.apache.paimon.options.Options());
    method.put("defaultFileCompression", defaults.fileCompression());
    method.put(
        "defaultZstdLevel",
        org.apache.paimon.CoreOptions.FILE_COMPRESSION_ZSTD_LEVEL.defaultValue());
    method.put("defaultAppendTargetBytes", defaults.targetFileSize(false));
    method.put("defaultPrimaryKeyTargetBytes", defaults.targetFileSize(true));
    method.put("defaultWriteBufferBytes", defaults.writeBufferSize());
    method.put("parallelism", 4);
    method.put("kafkaPartitions", 4);
    method.put("outputBuckets", 4);
    method.put("stateBackend", "RocksDB");
    method.put("miniBatch", false);
    method.put("managedMemory", System.getenv("SF_BENCH_MANAGED_MEMORY"));
    method.put("clusterSlots", 4);
    method.put("maxJavaHeapBytes", Runtime.getRuntime().maxMemory());
    method.put("runtimeMode", "STREAMING");
    method.put("sessionTimeZone", "UTC");
    method.put("checkpointIntervalMillis", 1000);
    method.put("snapshotCompression", false);
    method.put("objectReuse", true);
    method.put("maxPollRecords", 8192);
    method.put("warmups", WARMUPS);
    method.put("runs", RUNS);
    method.put(
        "engineOrder",
        "native first on odd trial indices, stock first on even; warmup indices negative");
    method.put("expectedCells", expected);
    method.put(
        "fileSettings",
        "Paimon released defaults; parquet format; append bucket-key first output column; real"
            + " primary keys for updating queries; deduplicate; in-job compaction enabled");
    method.put(
        "timing",
        "executeSql through final streaming end-of-input commit; production, explain and stock"
            + " readback untimed");
    write(artifacts.resolve("method.json"), method);
    try (var kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      NexmarkKafkaBenchmark.produce(kafka.getBootstrapServers(), "nexmark", "json", EVENTS, 4);
      for (var q : queries) {
        var sink =
            NexmarkMatrixBenchmark.UPSERT_KEYS.containsKey(q.label)
                ? NexmarkMatrixBenchmark.paimonPrimaryKeySink()
                : NexmarkMatrixBenchmark.paimonAppendSink(true);
        var options = new LinkedHashMap<String, Map<String, String>>();
        options.put("exact", Map.of());
        if (variants() && q.nativeVariantProps != null)
          options.put(q.nativeVariantLabel, q.nativeVariantProps);
        for (var variant : options.entrySet()) {
          for (int trial = -WARMUPS; trial < RUNS; trial++) {
            var pair = new LinkedHashMap<Boolean, Fingerprint>();
            for (boolean nativeRun : trial % 2 == 0 ? List.of(false, true) : List.of(true, false)) {
              pair.put(
                  nativeRun,
                  run(
                      kafka.getBootstrapServers(),
                      q,
                      sink,
                      variant.getKey(),
                      variant.getValue(),
                      trial,
                      nativeRun));
            }
            if (!q.label.equals("q12"))
              assertEquals(pair.get(false), pair.get(true), q.label + " " + variant.getKey());
            append(
                Map.of(
                    "kind",
                    "parity",
                    "query",
                    q.label,
                    "variant",
                    variant.getKey(),
                    "trial",
                    trial,
                    "matched",
                    true,
                    "validation",
                    q.label.equals("q12")
                        ? "per-bidder emitted counts bounded by corpus; unfinished processing-time"
                            + " windows excluded"
                        : "full-value multiset"));
          }
        }
      }
    }
  }

  private static boolean variants() {
    return !"false".equals(System.getenv("SF_PAIMON_VARIANTS"));
  }

  private Fingerprint run(
      String brokers,
      NexmarkMatrixBenchmark.Query q,
      NexmarkMatrixBenchmark.LakeSink sink,
      String variant,
      Map<String, String> extra,
      int trial,
      boolean nativeRun)
      throws Exception {
    var properties = new LinkedHashMap<String, String>();
    properties.put("streamfusion.native.enabled", Boolean.toString(nativeRun));
    properties.put("streamfusion.expression.allowIncompatible", "false");
    properties.put("streamfusion.expression.decimalArithmetic.approximate", "false");
    if (nativeRun) properties.putAll(extra);
    var previous = new LinkedHashMap<String, String>();
    properties.forEach((key, value) -> previous.put(key, System.getProperty(key)));
    properties.forEach(System::setProperty);
    String cell = q.label + (variant.equals("exact") ? "-exact" : "-variant");
    Path evidence =
        artifacts
            .resolve("trials")
            .resolve(cell)
            .resolve("run-" + trial + "-" + (nativeRun ? "native" : "stock"));
    Path output = evidence.resolve("table");
    Files.createDirectories(evidence);
    System.out.printf(
        "PAIMON_TRIAL %s %s %d %s%n", q.label, variant, trial, nativeRun ? "native" : "stock");
    try {
      double seconds =
          NexmarkMatrixBenchmark.runLakeSinkOnce(brokers, output, q, nativeRun, sink, evidence);
      // Readback must use the unaccelerated connector, independently of the timed writer.
      System.setProperty("streamfusion.native.enabled", "false");
      var fingerprint = fingerprint(sink.ddl(q, output), q.label);
      var files = footers(output);
      long nativeFiles =
          files.stream()
              .filter(file -> file.get("createdBy").toString().contains("parquet-rs"))
              .count();
      if (nativeRun && fingerprint.count() > 0)
        assertTrue(nativeFiles > 0, "no Rust Parquet files: " + evidence);
      if (!nativeRun) assertEquals(0, nativeFiles, "stock writer must stay stock");
      write(evidence.resolve("files.json"), files);
      var observation = new LinkedHashMap<String, Object>();
      observation.put("kind", "measurement");
      observation.put("query", q.label);
      observation.put("variant", variant);
      observation.put("trial", trial);
      observation.put("warmup", trial < 0);
      observation.put("engine", nativeRun ? "native" : "stock");
      observation.put("seconds", seconds);
      observation.put("sink", sink.id());
      observation.put("fingerprint", fingerprint);
      observation.put("rustParquetFiles", nativeFiles);
      observation.put("parquetFiles", files.size());
      observation.put(
          "parquetBytes",
          files.stream().mapToLong(file -> ((Number) file.get("bytes")).longValue()).sum());
      observation.put("evidence", artifacts.relativize(evidence).toString());
      append(observation);
      System.out.printf(
          "PAIMON_RESULT %s %s %d %s %.9f rows=%d%n",
          q.label, variant, trial, nativeRun ? "native" : "stock", seconds, fingerprint.count());
      return fingerprint;
    } finally {
      previous.forEach(
          (key, value) -> {
            if (value == null) System.clearProperty(key);
            else System.setProperty(key, value);
          });
      SharedFlinkCluster.assertNativeMemoryReleased(evidence.toString());
      System.gc();
      System.runFinalization();
    }
  }

  public static final class Fingerprint {
    public final long count;
    public final List<String> sums;
    public final Map<Long, Long> processingTimeCounts;

    Fingerprint(long count, List<String> sums, Map<Long, Long> processingTimeCounts) {
      this.count = count;
      this.sums = sums;
      this.processingTimeCounts = processingTimeCounts;
    }

    long count() {
      return count;
    }

    @Override
    public boolean equals(Object value) {
      if (!(value instanceof Fingerprint)) return false;
      Fingerprint other = (Fingerprint) value;
      return count == other.count
          && sums.equals(other.sums)
          && processingTimeCounts.equals(other.processingTimeCounts);
    }

    @Override
    public int hashCode() {
      return Objects.hash(count, sums, processingTimeCounts);
    }

    @Override
    public String toString() {
      return count + ":" + sums + ":" + processingTimeCounts;
    }
  }

  private static Fingerprint fingerprint(String ddl, String query) throws Exception {
    var sql = TableEnvironment.create(EnvironmentSettings.inBatchMode());
    sql.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    sql.getConfig().getConfiguration().setString("parallelism.default", "4");
    sql.executeSql(ddl);
    long count = 0;
    long[] sums = new long[4];
    var digest = MessageDigest.getInstance("SHA-256");
    var proctime = new TreeMap<Long, Long>();
    try (var rows = sql.executeSql("SELECT * FROM sink").collect()) {
      while (rows.hasNext()) {
        var row = rows.next();
        count++;
        if (query.equals("q12")) {
          proctime.merge(
              ((Number) row.getField(0)).longValue(),
              ((Number) row.getField(1)).longValue(),
              Long::sum);
        } else {
          var payload = new ArrayList<String>();
          for (int field = 0; field < row.getArity(); field++) {
            Object value = row.getField(field);
            payload.add(value == null ? null : value.toString());
          }
          var hash = ByteBuffer.wrap(digest.digest(JSON.writeValueAsBytes(payload)));
          for (int index = 0; index < 4; index++) sums[index] += hash.getLong();
        }
      }
    }
    for (var entry : proctime.entrySet()) {
      long bidder = entry.getKey();
      long expected = EVENTS / 50 + (EVENTS % 50 > bidder ? 1 : 0);
      assertTrue(
          bidder >= 4 && bidder < 50 && entry.getValue() >= 0 && entry.getValue() <= expected,
          "processing-time output exceeds corpus: " + entry);
    }
    return new Fingerprint(
        count,
        Arrays.stream(sums)
            .mapToObj(value -> Long.toUnsignedString(value, 16))
            .collect(ListCollectors.toList()),
        proctime);
  }

  private static List<Map<String, Object>> footers(Path table) throws Exception {
    var result = new ArrayList<Map<String, Object>>();
    try (var paths = Files.walk(table)) {
      for (Path path :
          paths
              .filter(p -> p.toString().endsWith(".parquet"))
              .sorted()
              .collect(ListCollectors.toList())) {
        try (var reader =
            ParquetUtil.getParquetReader(
                LocalFileIO.create(),
                new org.apache.paimon.fs.Path(path.toUri()),
                Files.size(path),
                new Options())) {
          var metadata = reader.getFooter();
          var row = new LinkedHashMap<String, Object>();
          row.put("path", table.relativize(path).toString());
          row.put("createdBy", metadata.getFileMetaData().getCreatedBy());
          row.put("bytes", Files.size(path));
          row.put(
              "physicalRows",
              metadata.getBlocks().stream().mapToLong(block -> block.getRowCount()).sum());
          row.put(
              "codecs",
              metadata.getBlocks().stream()
                  .flatMap(block -> block.getColumns().stream())
                  .map(column -> column.getCodec().toString())
                  .distinct()
                  .sorted()
                  .collect(ListCollectors.toList()));
          result.add(row);
        }
      }
    }
    return result;
  }

  private void append(Object row) throws Exception {
    Files.writeString(
        artifacts.resolve("observations.jsonl"),
        JSON.writeValueAsString(row) + "\n",
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND);
  }

  private static void write(Path path, Object value) throws Exception {
    Files.writeString(path, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(value) + "\n");
  }
}

package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeAllocator;

/** Release connector comparison: the same Arrow input, host control plane and final drain. */
@EnabledIfEnvironmentVariable(named = "SF_ICEBERG_DYNAMIC_BENCHMARK", matches = "true")
class IcebergDynamicBenchmark {
  private static final AtomicLong SENT = new AtomicLong();
  private static final int BATCH = 8192;
  private static final long ROWS =
      Long.parseLong(System.getenv().getOrDefault("SF_ROWS", "2000000"));
  private static final int FORMAT_VERSION =
      Integer.parseInt(System.getenv().getOrDefault("SF_ICEBERG_DYNAMIC_FORMAT_VERSION", "2"));

  @Test
  void compare() throws Exception {
    assertEquals("true", System.getenv("SF_ICEBERG_RELEASE_CONFIRMED"));
    assertTrue(FORMAT_VERSION == 2 || FORMAT_VERSION == 3);
    Path artifacts =
        Path.of(
                System.getenv()
                    .getOrDefault("SF_ICEBERG_ARTIFACTS", "target/iceberg-dynamic-benchmark"))
            .toAbsolutePath();
    Files.createDirectories(artifacts);
    for (boolean keyed : new boolean[] {false, true}) {
      int warmups = Integer.parseInt(System.getenv().getOrDefault("SF_WARMUP", "1"));
      int runs = Integer.parseInt(System.getenv().getOrDefault("SF_RUNS", "3"));
      for (int trial = -warmups; trial < runs; trial++) {
        for (boolean nativeMode : trial % 2 == 0 ? List.of(false, true) : List.of(true, false)) {
          run(artifacts, keyed, trial, nativeMode);
        }
      }
    }
  }

  private void run(Path artifacts, boolean keyed, int trial, boolean nativeMode) throws Exception {
    String token =
        (keyed ? "keyed" : "append")
            + "-"
            + (nativeMode ? "native" : "stock")
            + "-"
            + trial
            + "-"
            + UUID.randomUUID();
    Path directory = artifacts.resolve(token);
    String warehouse = directory.resolve("warehouse").toUri().toString();
    var hadoop = new org.apache.hadoop.conf.Configuration();
    var schema =
        new Schema(
            List.of(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "payload", Types.StringType.get())),
            keyed ? Set.of(1) : Set.of());
    var spec = PartitionSpec.builderFor(schema).bucket("id", 4).build();
    var options =
        Map.of(
            "target-file-size-bytes",
            "536870912",
            "dynamic-sink.cache-max-size",
            "1000",
            "dynamic-sink.immediate-table-update",
            "true");
    try (var catalog = new HadoopCatalog(hadoop, warehouse)) {
      catalog.createNamespace(Namespace.of("bench"));
      for (String name : new String[] {"a", "b"})
        catalog.createTable(
            TableIdentifier.of("bench", name),
            schema,
            spec,
            Map.of(
                "format-version",
                Integer.toString(FORMAT_VERSION),
                "write.parquet.compression-codec",
                "zstd",
                "write.parquet.compression-level",
                "3"));
      var config = Configuration.fromMap(Map.of("restart-strategy.type", "none"));
      var env = StreamExecutionEnvironment.getExecutionEnvironment(config);
      env.setParallelism(4);
      env.setMaxParallelism(128);
      env.enableCheckpointing(1000);
      SENT.set(0);
      var input =
          env.fromSequence(0, 100000000)
              .setParallelism(1)
              .flatMap(new Payloads(keyed))
              .returns(ArrowBatchTypeInformation.INSTANCE)
              .setParallelism(1);
      var loader = CatalogLoader.hadoop("benchmark", hadoop, Map.of("warehouse", warehouse));
      NativeIcebergDynamicSink.Router router =
          row ->
              new NativeIcebergDynamicSink.Destination(
                  row.getInt(0) % 2 == 0 ? "bench.a" : "bench.b",
                  "main",
                  spec,
                  row.getInt(0) % 2 == 0 ? null : DistributionMode.HASH,
                  4,
                  keyed,
                  keyed ? Set.of("id") : Set.of());
      var resources = new tech.streamfusion.IcebergBenchmarkResources();
      long started = System.nanoTime();
      if (nativeMode)
        NativeIcebergDynamicSink.append(input, schema, router, loader, config, options, "dynamic");
      else
        DynamicIcebergSink.forInput(input)
            .generator(
                (batch, out) -> {
                  try (var root = batch.root()) {
                    var reader =
                        ArrowConversion.createArrowReader(root, FlinkSchemaUtil.convert(schema));
                    for (int i = 0; i < root.getRowCount(); i++) {
                      var row = reader.read(i);
                      var target = router.route(row);
                      var record =
                          new DynamicRecord(
                              TableIdentifier.parse(target.table()),
                              target.branch(),
                              schema,
                              row,
                              spec,
                              target.distribution(),
                              target.writeParallelism());
                      record.setUpsertMode(keyed);
                      record.setEqualityFields(target.equalityFields());
                      out.collect(record);
                    }
                  }
                })
            .catalogLoader(loader)
            .flinkConf(config)
            .setAll(options)
            .uidPrefix("dynamic")
            .append();
      var graph = env.getStreamGraph();
      Files.createDirectories(directory);
      Files.writeString(directory.resolve("graph.json"), graph.getStreamingPlanAsJSON());
      var job = env.executeAsync(graph);
      boolean finished = false;
      try {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(10);
        while ((SENT.get() < ROWS || !running(job)) && System.nanoTime() < deadline) {
          resources.sample();
          if (job.getJobStatus().get().isGloballyTerminalState()) job.getJobExecutionResult().get();
          Thread.sleep(10);
        }
        assertTrue(SENT.get() >= ROWS && running(job));
        job.triggerSavepoint(
                directory.resolve("streaming-checkpoint").toUri().toString(),
                SavepointFormatType.CANONICAL)
            .get(120, TimeUnit.SECONDS);
        job.stopWithSavepoint(
                true,
                directory.resolve("savepoint").toUri().toString(),
                SavepointFormatType.CANONICAL)
            .get(120, TimeUnit.SECONDS);
        job.getJobExecutionResult().get(120, TimeUnit.SECONDS);
        finished = true;
      } finally {
        if (!finished) {
          job.cancel().get();
          job.getJobExecutionResult().handle((value, failure) -> null).get();
        }
      }
      double seconds = (System.nanoTime() - started) / 1e9;
      var resourceObservation = resources.finish();
      Set<Integer> seen = new HashSet<>();
      Set<String> dataFiles = new HashSet<>();
      Set<String> deleteFiles = new HashSet<>();
      Set<String> puffinFiles = new HashSet<>();
      for (String name : new String[] {"a", "b"}) {
        var table = catalog.loadTable(TableIdentifier.of("bench", name));
        try (var tasks = table.newScan().planFiles()) {
          for (var task : tasks) {
            dataFiles.add(task.file().location());
            for (var delete : task.deletes()) {
              deleteFiles.add(delete.location());
              if (delete.format() == org.apache.iceberg.FileFormat.PUFFIN) {
                assertNotNull(delete.referencedDataFile());
                assertTrue(delete.contentOffset() >= 0);
                assertTrue(delete.contentSizeInBytes() > 0);
                puffinFiles.add(delete.location());
              }
            }
          }
        }
        try (var rows = IcebergGenerics.read(table).build()) {
          for (var row : rows) {
            int id = (Integer) row.getField("id");
            assertTrue(seen.add(id));
            assertEquals(name.equals("a"), id % 2 == 0);
            assertEquals(id % 7 == 0 ? null : "payload-é-" + id, row.getField("payload"));
          }
        }
        if (nativeMode)
          NativeIcebergSqlTest.assertNativeDataFiles(table, "dynamic release benchmark");
      }
      assertEquals(keyed ? Math.min(ROWS, 100000) : ROWS, seen.size());
      if (keyed && FORMAT_VERSION == 3 && ROWS > 100000)
        assertFalse(puffinFiles.isEmpty(), "v3 benchmark must exercise Java deletion vectors");
      assertEquals("", NativeIceberg.liveNativeHandles());
      assertEquals(0L, NativeAllocator.SHARED.getAllocatedMemory(), token);
      assertEquals(0L, tech.streamfusion.operator.TaskOffHeapMemory.reservedBytes(), token);
      String observation =
          new com.fasterxml.jackson.databind.ObjectMapper()
              .writeValueAsString(
                  Map.of(
                      "mode",
                      keyed ? "keyed" : "forward-hash",
                      "trial",
                      trial,
                      "native",
                      nativeMode,
                      "rows",
                      ROWS,
                      "seconds",
                      seconds,
                      "committedRows",
                      seen.size(),
                      "resources",
                      resourceObservation,
                      "directory",
                      directory.toString(),
                      "formatVersion",
                      FORMAT_VERSION,
                      "files",
                      Map.of(
                          "data",
                          dataFiles.size(),
                          "deletes",
                          deleteFiles.size(),
                          "puffin",
                          puffinFiles.size())));
      Files.writeString(
          artifacts.resolve("dynamic-observations.jsonl"),
          observation + "\n",
          java.nio.file.StandardOpenOption.CREATE,
          java.nio.file.StandardOpenOption.APPEND);
      System.out.println("DYNAMIC_BENCHMARK " + observation);
    }
  }

  private static boolean running(org.apache.flink.core.execution.JobClient job) throws Exception {
    var field =
        org.apache.flink.runtime.minicluster.MiniClusterJobClient.class.getDeclaredField(
            "miniCluster");
    field.setAccessible(true);
    var cluster = (org.apache.flink.runtime.minicluster.MiniCluster) field.get(job);
    for (var vertex : cluster.getExecutionGraph(job.getJobID()).get().getAllExecutionVertices())
      if (vertex.getExecutionState() != org.apache.flink.runtime.execution.ExecutionState.RUNNING)
        return false;
    return true;
  }

  private static final class Payloads extends RichFlatMapFunction<Long, ArrowBatch> {
    private final boolean keyed;

    Payloads(boolean keyed) {
      this.keyed = keyed;
    }

    @Override
    public void flatMap(Long batch, Collector<ArrowBatch> out) throws Exception {
      long start = batch * BATCH;
      if (start >= ROWS) {
        Thread.sleep(5);
        return;
      }
      int count = (int) Math.min(BATCH, ROWS - start);
      var ids = new IntVector("id", NativeAllocator.SHARED);
      var payloads = new VarCharVector("payload", NativeAllocator.SHARED);
      var root = VectorSchemaRoot.of(ids, payloads);
      root.allocateNew();
      for (int i = 0; i < count; i++) {
        int id = Math.toIntExact(keyed ? (start + i) % 100000 : start + i);
        ids.setSafe(i, id);
        if (id % 7 != 0)
          payloads.setSafe(
              i, ("payload-é-" + id).getBytes(java.nio.charset.StandardCharsets.UTF_8));
      }
      root.setRowCount(count);
      out.collect(new ArrowBatch(root));
      SENT.addAndGet(count);
    }
  }
}

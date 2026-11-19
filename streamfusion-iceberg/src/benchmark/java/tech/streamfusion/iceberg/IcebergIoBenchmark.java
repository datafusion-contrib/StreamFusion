package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.source.reader.ReaderFunction;
import org.apache.iceberg.flink.source.reader.RowDataReaderFunction;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** Fast file-task boundary comparison; no cluster, query state, discovery, shuffle or commits. */
public final class IcebergIoBenchmark implements AutoCloseable {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(17, "id", Types.LongType.get()),
          Types.NestedField.optional(23, "category", Types.IntegerType.get()),
          Types.NestedField.optional(38, "price", Types.DoubleType.get()),
          Types.NestedField.optional(41, "payload", Types.StringType.get()),
          Types.NestedField.optional(52, "at", Types.TimestampType.withoutZone()),
          Types.NestedField.optional(67, "flag", Types.BooleanType.get()));
  private static final RowType TYPE = FlinkSchemaUtil.convert(SCHEMA);
  private static final com.fasterxml.jackson.databind.ObjectMapper JSON =
      new com.fasterxml.jackson.databind.ObjectMapper();
  private static final RowData.FieldGetter[] GETTERS =
      java.util.stream.IntStream.range(0, TYPE.getFieldCount())
          .mapToObj(field -> RowData.createFieldGetter(TYPE.getTypeAt(field), field))
          .toArray(RowData.FieldGetter[]::new);
  private static final Map<String, String> SETTINGS =
      Map.of(
          TableProperties.FORMAT_VERSION, "2",
          TableProperties.PARQUET_COMPRESSION, "zstd",
          TableProperties.PARQUET_COMPRESSION_LEVEL, "3",
          TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "134217728",
          TableProperties.PARQUET_PAGE_SIZE_BYTES, "1048576",
          TableProperties.PARQUET_DICT_SIZE_BYTES, "2097152");

  private final int rows = setting("ROWS", 262144);
  private final int batchRows = setting("BATCH_ROWS", 8192);
  private final int width = setting("WIDTH", 48);
  private final int partitions = setting("PARTITIONS", 4);
  private final int stripes = setting("STRIPES", 4);
  private final int warmups = setting("WARMUP", 2);
  private final int runs = setting("RUNS", 5);
  private final long targetBytes = Long.parseLong(env("TARGET_BYTES", "536870912"));
  private final Path directory = Path.of(env("DIRECTORY", "target/iceberg-io/run"));
  private final Path fixtures = Path.of(env("FIXTURES", "target/iceberg-io/fixtures"));
  private final List<RowData> records = new ArrayList<>();
  private final List<VectorSchemaRoot> batches = new ArrayList<>();
  private final HadoopTables tables = new HadoopTables(new org.apache.hadoop.conf.Configuration());
  private final PartitionSpec spec =
      partitions == 1
          ? PartitionSpec.unpartitioned()
          : PartitionSpec.builderFor(SCHEMA).bucket("id", partitions).build();
  private Table input;
  private long nativeTasks;
  private long fallbackTasks;
  private List<FileScanTask> tasks;

  @Test
  @EnabledIfEnvironmentVariable(named = "SF_ICEBERG_IO_PREPARE", matches = "true")
  void prepareClasspath() throws Exception {
    Files.writeString(
        Path.of(env("CLASSPATH", "target/iceberg-io/classpath.txt")),
        System.getProperty("surefire.test.class.path"));
  }

  public static void main(String[] args) throws Exception {
    try (var benchmark = new IcebergIoBenchmark()) {
      benchmark.run();
    }
  }

  private void run() throws Exception {
    if (rows < 1
        || batchRows < 1
        || stripes < 1
        || width < 0
        || partitions < 1
        || warmups < 1
        || runs < 2)
      throw new IllegalArgumentException(
          "Positive dimensions, at least one warmup and two repetitions required");
    Files.createDirectories(directory);
    TaskOffHeapMemory.initialize(
        Configuration.fromMap(Map.of("taskmanager.memory.task.off-heap.size", "1gb")));
    prepareRecords();
    var cases = List.of(env("CASES", "read,write").split(","));
    for (String name : cases) {
      if (!List.of("read", "write", "route", "dynamic-route", "dynamic-key").contains(name))
        throw new IllegalArgumentException("Unknown case: " + name);
    }
    if (cases.contains("read")) prepareInput();
    Table output =
        cases.contains("write")
            ? tables.create(SCHEMA, spec, SETTINGS, directory.resolve("output").toString())
            : null;
    var method =
        new java.util.LinkedHashMap<String, Object>(
            Map.of(
                "kind",
                "method",
                "rows",
                rows,
                "batchRows",
                batchRows,
                "width",
                width,
                "partitions",
                partitions,
                "stripes",
                stripes,
                "warmups",
                warmups,
                "runs",
                runs,
                "targetBytes",
                targetBytes,
                "settings",
                SETTINGS));
    method.put("cases", cases);
    method.put("stockFetchRows", batchRows);
    method.put(
        "profileEvent",
        System.getenv("SF_ICEBERG_IO_PROFILER") == null ? "none" : env("PROFILE", "itimer"));
    method.put("inputFiles", tasks == null ? 0 : tasks.size());
    emit(method);
    for (String name : cases) {
      // Full stock-decoded payload checks are outside the timed loop on both implementations.
      if (name.equals("read")) {
        assertEquals(rows, read(false, true));
        assertEquals(rows, read(true, true));
      }
      for (int trial = -warmups; trial < runs; trial++) {
        for (boolean nativeMode : trial % 2 == 0 ? List.of(false, true) : List.of(true, false)) {
          long nanos;
          if (name.equals("read")) {
            profile("start", name, nativeMode, trial);
            long start = System.nanoTime();
            long count = read(nativeMode, false);
            nanos = System.nanoTime() - start;
            profile("stop", name, nativeMode, trial);
            assertEquals(rows, count);
            if (nativeMode) {
              assertEquals(tasks.size(), nativeTasks);
              assertEquals(0, fallbackTasks);
            }
          } else if (name.equals("route")
              || name.equals("dynamic-route")
              || name.equals("dynamic-key")) {
            profile("start", name, nativeMode, trial);
            long start = System.nanoTime();
            int[] actual =
                name.equals("dynamic-route")
                    ? dynamicRoute(nativeMode)
                    : route(nativeMode, name.equals("dynamic-key"));
            nanos = System.nanoTime() - start;
            profile("stop", name, nativeMode, trial);
            assertArrayEquals(
                name.equals("dynamic-route")
                    ? dynamicRoute(false)
                    : route(false, name.equals("dynamic-key")),
                actual);
          } else {
            // Retaining fixture views models owned input batches, outside the file-write timer.
            var owned = new ArrayList<ArrowBatch>();
            if (nativeMode) for (var root : batches) owned.add(new ArrowBatch(retain(root)));
            var table = output;
            WriteResult result;
            profile("start", name, nativeMode, trial);
            long start = System.nanoTime();
            if (nativeMode) {
              try (var writer = nativeWriter(table)) {
                for (var batch : owned) writer.write(batch);
                result = writer.complete();
              }
            } else {
              try (var writer = stockWriter(table)) {
                for (var record : records) writer.write(record);
                result = writer.complete();
              }
            }
            nanos = System.nanoTime() - start;
            profile("stop", name, nativeMode, trial);
            validate(result, table, nativeMode);
            for (var file : result.dataFiles()) table.io().deleteFile(file.location());
          }
          assertEquals("", NativeIceberg.liveNativeHandles());
          assertEquals(
              NativeAllocator.SHARED.getAllocatedMemory(), TaskOffHeapMemory.reservedBytes());
          emit(
              Map.of(
                  "kind",
                  "measurement",
                  "operation",
                  name,
                  "native",
                  nativeMode,
                  "trial",
                  trial,
                  "warmup",
                  trial < 0,
                  "seconds",
                  nanos / 1e9,
                  "rows",
                  rows,
                  "arrowBytes",
                  NativeAllocator.SHARED.getAllocatedMemory(),
                  "nativeTasks",
                  name.equals("read") ? nativeTasks : 0,
                  "fallbackTasks",
                  name.equals("read") ? fallbackTasks : 0));
        }
      }
    }
  }

  private void prepareRecords() {
    String padding = "x".repeat(width);
    for (int i = 0; i < rows; i++) {
      records.add(
          GenericRowData.of(
              (long) i,
              i % 13 == 0 ? null : i % 31,
              i % 17 == 0 ? null : i * .25,
              i % 7 == 0 ? null : StringData.fromString("é-" + i % 4096 + padding),
              TimestampData.fromEpochMillis(i - 1234567L),
              i % 11 == 0 ? null : i % 2 == 0));
    }
    for (int start = 0; start < rows; start += batchRows) {
      var root =
          VectorSchemaRoot.create(ArrowConversion.toArrowSchema(TYPE), NativeAllocator.SHARED);
      var writer = ArrowConversion.createRowDataArrowWriter(root, TYPE);
      for (int i = start; i < Math.min(rows, start + batchRows); i++) writer.write(records.get(i));
      writer.finish();
      batches.add(root);
    }
  }

  private void prepareInput() throws Exception {
    String definition =
        SchemaParser.toJson(SCHEMA) + JSON.writeValueAsString(new java.util.TreeMap<>(SETTINGS));
    String fingerprint =
        java.util.HexFormat.of()
            .formatHex(
                java.security.MessageDigest.getInstance("SHA-256")
                    .digest(definition.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
            .substring(0, 16);
    String key = "v1-" + fingerprint + "-" + rows + "-" + width + "-" + partitions + "-" + stripes;
    Path path = fixtures.resolve(key);
    Files.createDirectories(fixtures);
    if (tables.exists(path.toString())) input = tables.load(path.toString());
    else {
      input = tables.create(SCHEMA, spec, SETTINGS, path.toString());
      var append = input.newAppend();
      for (int stripe = 0; stripe < stripes; stripe++) {
        try (var writer = stockWriter(input)) {
          for (int i = (int) ((long) rows * stripe / stripes);
              i < (long) rows * (stripe + 1) / stripes;
              i++) writer.write(records.get(i));
          for (var file : writer.complete().dataFiles()) append.appendFile(file);
        }
      }
      append.commit();
    }
    tasks = new ArrayList<>();
    try (var planned = input.newScan().planFiles()) {
      for (var task : planned) tasks.add(task);
    }
    assertEquals(rows, tasks.stream().mapToLong(t -> t.file().recordCount()).sum());
  }

  private long read(boolean nativeMode, boolean check) throws Exception {
    var configuration = new Configuration();
    configuration.set(
        org.apache.iceberg.flink.FlinkConfigOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT,
        batchRows);
    ReaderFunction<?> function =
        nativeMode
            ? new IcebergArrowReaderFunction(
                input.schema(),
                input.schema().columns().stream().map(Types.NestedField::fieldId).toList(),
                input.io(),
                batchRows)
            : new RowDataReaderFunction(
                configuration,
                input.schema(),
                input.schema(),
                null,
                true,
                input.io(),
                input.encryption(),
                List.of());
    var nativeCounter = new org.apache.flink.metrics.SimpleCounter();
    var fallbackCounter = new org.apache.flink.metrics.SimpleCounter();
    if (function instanceof IcebergArrowReaderFunction arrow) {
      arrow.registerMetrics(
          new org.apache.flink.metrics.groups.UnregisteredMetricsGroup() {
            @Override
            public org.apache.flink.metrics.Counter counter(String name) {
              return switch (name) {
                case "nativeParquetTasks" -> nativeCounter;
                case "stockFallbackTasks" -> fallbackCounter;
                default -> super.counter(name);
              };
            }
          });
    }
    long count = 0;
    var seen = check ? new BitSet(rows) : null;
    for (var task : tasks) {
      var split = IcebergSourceSplit.fromCombinedScanTask(new BaseCombinedScanTask(List.of(task)));
      try (var iterator = function.apply(split)) {
        while (iterator.hasNext()) {
          var fetched = iterator.next();
          try {
            while (fetched.nextSplit() != null) {
              org.apache.iceberg.flink.source.reader.RecordAndPosition<?> positioned;
              while ((positioned = fetched.nextRecordFromSplit()) != null) {
                if (nativeMode) {
                  try (var root = ((ArrowBatch) positioned.record()).root()) {
                    count += root.getRowCount();
                    if (check) {
                      var reader = ArrowConversion.createArrowReader(root, TYPE);
                      for (int row = 0; row < root.getRowCount(); row++)
                        check(reader.read(row), seen);
                    }
                  }
                } else {
                  count++;
                  if (check) check((RowData) positioned.record(), seen);
                }
              }
            }
          } finally {
            fetched.recycle();
          }
        }
      }
    }
    if (check) assertEquals(rows, seen.cardinality());
    if (function instanceof IcebergArrowReaderFunction arrow) arrow.cancelIo();
    nativeTasks = nativeCounter.getCount();
    fallbackTasks = fallbackCounter.getCount();
    return count;
  }

  @SuppressWarnings("unchecked")
  private int[] route(boolean nativeMode, boolean dynamic) throws Exception {
    var stock = new org.apache.iceberg.flink.sink.PartitionKeySelector(spec, SCHEMA, TYPE);
    org.apache.flink.api.java.functions.KeySelector<RowData, Integer> groups =
        row ->
            org.apache.flink.runtime.state.KeyGroupRangeAssignment.assignToKeyGroup(
                stock.getKey(row), 128);
    if (dynamic) {
      groups = dynamicKeySelector();
      if (nativeMode) groups = IcebergDynamicRouting.memoize(groups);
    } else if (nativeMode) {
      try {
        var method =
            IcebergHashRouter.class.getDeclaredMethod(
                "grouping", org.apache.flink.api.java.functions.KeySelector.class, int.class);
        method.setAccessible(true);
        groups =
            (org.apache.flink.api.java.functions.KeySelector<RowData, Integer>)
                method.invoke(null, stock, 128);
      } catch (NoSuchMethodException previousConnector) {
        // The same benchmark can compare the previous connector's uncached routing.
      }
    }
    int[] destinations = new int[rows];
    int position = 0;
    if (nativeMode) {
      for (var batch : batches) {
        var reader = ArrowConversion.createArrowReader(batch, TYPE);
        for (int row = 0; row < batch.getRowCount(); row++)
          destinations[position++] = groups.getKey(reader.read(row));
      }
    } else for (var record : records) destinations[position++] = groups.getKey(record);
    return destinations;
  }

  @SuppressWarnings("unchecked")
  private org.apache.flink.api.java.functions.KeySelector<RowData, Integer> dynamicKeySelector()
      throws Exception {
    Class<?> type = Class.forName("org.apache.iceberg.flink.sink.dynamic.HashKeyGenerator");
    var constructor = type.getDeclaredConstructor(int.class, int.class);
    constructor.setAccessible(true);
    var method =
        type.getDeclaredMethod(
            "getKeySelector",
            String.class,
            Schema.class,
            PartitionSpec.class,
            org.apache.iceberg.DistributionMode.class,
            java.util.Set.class,
            int.class);
    method.setAccessible(true);
    return (org.apache.flink.api.java.functions.KeySelector<RowData, Integer>)
        method.invoke(
            constructor.newInstance(1, 128),
            "bench.t",
            SCHEMA,
            spec,
            org.apache.iceberg.DistributionMode.HASH,
            java.util.Set.of(),
            4);
  }

  private RowDataTaskWriterFactory factory(Table table) {
    var factory =
        new RowDataTaskWriterFactory(
            table, TYPE, targetBytes, FileFormat.PARQUET, SETTINGS, List.of(), false);
    factory.initialize(0, 0);
    return factory;
  }

  private TaskWriter<RowData> stockWriter(Table table) {
    return factory(table).create();
  }

  @SuppressWarnings("unchecked")
  private TaskWriter<ArrowBatch> nativeWriter(Table table) throws Exception {
    var factory = factory(table);
    var context = new ArrowWriteContext();
    var decorator =
        NativeIcebergSink.dataWriterDecorator(
            SchemaParser.toJson(table.schema()),
            IcebergParquetSettings.translate(SETTINGS),
            MetricsConfig.forTable(table));
    var field = IcebergSinkAdapter.field(RowDataTaskWriterFactory.class, "fileWriterFactory");
    field.set(
        factory, decorator.decorate((FileWriterFactory<RowData>) field.get(factory), context));
    // Run the production batch writer; only operator metric setup needs a live Flink task.
    var adapter = new IcebergTaskWriterFactory(factory, TYPE, decorator, null);
    IcebergSinkAdapter.field(IcebergTaskWriterFactory.class, "context").set(adapter, context);
    return adapter.create();
  }

  private void check(RowData row, BitSet seen) {
    int id = Math.toIntExact(row.getLong(0));
    if (id < 0 || id >= rows || seen.get(id)) fail("Invalid or duplicate ID: " + id);
    seen.set(id);
    RowData expected = records.get(id);
    for (int field = 1; field < TYPE.getFieldCount(); field++) {
      var getter = GETTERS[field];
      Object expectedValue = getter.getFieldOrNull(expected);
      Object actualValue = getter.getFieldOrNull(row);
      if (!java.util.Objects.equals(expectedValue, actualValue))
        fail(
            "id="
                + id
                + " field="
                + field
                + " expected="
                + expectedValue
                + " actual="
                + actualValue);
    }
  }

  private void validate(WriteResult result, Table table, boolean nativeMode) throws Exception {
    assertEquals(0, result.deleteFiles().length);
    var seen = new BitSet(rows);
    for (var file : result.dataFiles()) {
      try (var footer =
          org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader.open(
              org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                  new org.apache.hadoop.fs.Path(file.location()),
                  new org.apache.hadoop.conf.Configuration()))) {
        assertEquals(
            nativeMode, footer.getFooter().getFileMetaData().getCreatedBy().contains("parquet-rs"));
      }
      try (var input =
          org.apache.iceberg.parquet.Parquet.read(table.io().newInputFile(file.location()))
              .project(table.schema())
              .createReaderFunc(
                  schema ->
                      org.apache.iceberg.flink.data.FlinkParquetReaders.buildReader(
                          table.schema(), schema))
              .<RowData>build()) {
        for (var row : input) check(row, seen);
      }
    }
    assertEquals(rows, seen.cardinality());
  }

  private static VectorSchemaRoot retain(VectorSchemaRoot root) {
    var fields = new ArrayList<FieldVector>();
    for (var vector : root.getFieldVectors()) {
      var transfer = vector.getTransferPair(vector.getAllocator());
      transfer.splitAndTransfer(0, root.getRowCount());
      fields.add((FieldVector) transfer.getTo());
    }
    return new VectorSchemaRoot(root.getSchema(), fields, root.getRowCount());
  }

  private int[] dynamicRoute(boolean nativeMode) throws Exception {
    NativeIcebergDynamicSink.Router router =
        row -> {
          long id = row.getLong(0);
          return new NativeIcebergDynamicSink.Destination(
              "bench.target" + id % partitions,
              id % 2 == 0 ? "main" : "audit",
              spec,
              org.apache.iceberg.DistributionMode.HASH,
              4,
              false,
              java.util.Set.of());
        };
    org.apache.iceberg.flink.sink.dynamic.DynamicRecordGenerator<RowData> generator =
        (row, out) -> {
          var kind = row.getRowKind();
          var destination = router.route(row);
          if (row.getRowKind() != kind)
            throw new IllegalArgumentException("Dynamic router changed the borrowed row kind");
          var record =
              new org.apache.iceberg.flink.sink.dynamic.DynamicRecord(
                  org.apache.iceberg.catalog.TableIdentifier.parse(destination.table()),
                  destination.branch(),
                  SCHEMA,
                  row,
                  destination.spec(),
                  destination.distribution(),
                  destination.writeParallelism());
          record.setUpsertMode(destination.upsert());
          record.setEqualityFields(destination.equalityFields());
          out.collect(record);
        };
    if (nativeMode) {
      try {
        var factory =
            NativeIcebergDynamicSink.class.getDeclaredMethod(
                "generator", Schema.class, NativeIcebergDynamicSink.Router.class, boolean.class);
        factory.setAccessible(true);
        generator =
            (org.apache.iceberg.flink.sink.dynamic.DynamicRecordGenerator<RowData>)
                factory.invoke(null, SCHEMA, router, true);
      } catch (NoSuchMethodException previousImplementation) {
        // Saved pre-optimization connector classes use the original uncached generator above.
      }
    }
    int[] result = new int[rows];
    int[] position = {0};
    var collector =
        new org.apache.flink.util.Collector<org.apache.iceberg.flink.sink.dynamic.DynamicRecord>() {
          public void collect(org.apache.iceberg.flink.sink.dynamic.DynamicRecord record) {
            result[position[0]++] =
                31 * record.tableIdentifier().hashCode() + record.branch().hashCode();
          }

          public void close() {}
        };
    if (nativeMode) {
      for (var root : batches) {
        var reader = ArrowConversion.createArrowReader(root, TYPE);
        for (int row = 0; row < root.getRowCount(); row++)
          generator.generate(reader.read(row), collector);
      }
    } else for (var row : records) generator.generate(row, collector);
    assertEquals(rows, position[0]);
    return result;
  }

  private void profile(String action, String operation, boolean nativeMode, int trial)
      throws Exception {
    String tool = System.getenv("SF_ICEBERG_IO_PROFILER");
    if (tool == null) return;
    String event = env("PROFILE", "itimer");
    var command = new ArrayList<>(List.of(tool, action));
    if (action.equals("start")) command.addAll(List.of("-e", event, "-t", "--cstack", "dwarf"));
    else
      command.addAll(
          List.of(
              "-o",
              "collapsed",
              "-f",
              directory
                  .resolve(operation + "-" + nativeMode + "-" + trial + "-" + event + ".collapsed")
                  .toString()));
    command.add(Long.toString(ProcessHandle.current().pid()));
    var process = new ProcessBuilder(command).inheritIO().start();
    assertTrue(process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS));
    assertEquals(0, process.exitValue());
  }

  private void emit(Map<String, ?> data) throws Exception {
    String json = JSON.writeValueAsString(data);
    Files.writeString(
        directory.resolve("observations.jsonl"),
        json + "\n",
        java.nio.file.StandardOpenOption.CREATE,
        java.nio.file.StandardOpenOption.APPEND);
    System.out.println("ICEBERG_IO " + json);
  }

  private static String env(String key, String fallback) {
    return System.getenv().getOrDefault("SF_ICEBERG_IO_" + key, fallback);
  }

  private static int setting(String key, int fallback) {
    return Integer.parseInt(env(key, Integer.toString(fallback)));
  }

  @Override
  public void close() {
    for (var batch : batches) batch.close();
    assertEquals("", NativeIceberg.liveNativeHandles());
    assertEquals(0, NativeAllocator.SHARED.getAllocatedMemory());
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
  }
}

package tech.streamfusion.delta;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.management.ThreadMXBean;
import io.delta.flink.sink.DeltaSinkConf;
import io.delta.flink.sink.DeltaWriterResult;
import io.delta.kernel.Scan;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.arrow.memory.AllocationListener;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.operator.NativeAllocator;

/** Production write-only buffering boundary; conversion, commit and stock validation are untimed. */
@EnabledIfEnvironmentVariable(named = "SF_DELTA_BUFFER_BENCHMARK", matches = "true")
class DeltaBufferingBenchmark {
  private static final int BATCH_ROWS = 1024;
  private static final int UPSERT_KEYS = 16;
  private static final RowType TYPE = RowType.of(
      new LogicalType[] {new BigIntType(false), new IntType(),
          new VarCharType(VarCharType.MAX_LENGTH)}, new String[] {"id", "v", "payload"});
  private static final StructType SCHEMA = new StructType(List.of(
      new StructField("id", LongType.LONG, false),
      new StructField("v", IntegerType.INTEGER, true),
      new StructField("payload", StringType.STRING, true)));
  private static final SinkWriter.Context CONTEXT = new SinkWriter.Context() {
    @Override public long currentWatermark() { return Long.MIN_VALUE; }
    @Override public Long timestamp() { return null; }
  };

  private static final class Allocations implements AllocationListener {
    long requests;
    @Override public void onAllocation(long size) { requests++; }
  }

  @Test
  void heldCheckpointAppendAndRepeatedUpserts() throws Exception {
    var memory = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    assertTrue(memory.isThreadAllocatedMemorySupported(), "requires Java thread allocation accounting");
    memory.setThreadAllocatedMemoryEnabled(true);
    int repetitions = Integer.parseInt(System.getenv().getOrDefault("SF_DELTA_BUFFER_REPEATS", "3"));
    assertTrue(repetitions > 0);
    for (String shape : System.getenv().getOrDefault("SF_DELTA_BUFFER_ROWS", "4096,32768").split(",")) {
      int rows = Integer.parseInt(shape);
      assertTrue(rows >= UPSERT_KEYS && rows % UPSERT_KEYS == 0);
      for (int width : new int[] {16, 256}) {
        for (boolean upsert : new boolean[] {false, true}) {
          for (int iteration = -1; iteration < repetitions; iteration++) {
            run(memory, rows, width, upsert, iteration);
          }
        }
      }
    }
  }

  private static void run(ThreadMXBean memory, int rows, int width, boolean upsert, int iteration)
      throws Exception {
    Path directory = Files.createTempDirectory("delta-buffer-benchmark");
    var table = new NativeDeltaHadoopTable(directory.toUri(), Map.of(), SCHEMA, List.of());
    table.open();
    var conf = new DeltaSinkConf(TYPE, upsert
        ? Map.of("write.mode", "upsert", "primary_key", "0") : Map.of());
    var writer = new NativeDeltaSinkWriter("buffer-benchmark", 0, 0, table, conf);
    Allocations allocations = new Allocations();
    List<ArrowKernelRows> input = new ArrayList<>();
    String payload = "x".repeat(width);
    try (var allocator = NativeAllocator.SHARED.newChildAllocator(
        "delta-buffer-input", allocations, 0, Long.MAX_VALUE)) {
      try {
        for (int base = 0; base < rows; base += BATCH_ROWS) {
          List<RowData> batch = new ArrayList<>();
          for (int row = base; row < Math.min(rows, base + BATCH_ROWS); row++) {
            var value = GenericRowData.of(upsert ? (long) (row % UPSERT_KEYS) : (long) row,
                row, StringData.fromString(payload));
            value.setRowKind(upsert && row >= UPSERT_KEYS ? RowKind.UPDATE_AFTER : RowKind.INSERT);
            batch.add(value);
          }
          input.add(new ArrowKernelRows(
              RowDataArrowConverter.write(batch, TYPE, allocator, upsert), TYPE, SCHEMA));
        }
        long inputBytes = allocator.getAllocatedMemory();
        long inputRequests = allocations.requests;
        long thread = Thread.currentThread().getId();
        long heapBefore = memory.getThreadAllocatedBytes(thread);
        long requestsBefore = allocations.requests;
        long started = System.nanoTime();
        for (int batch = 0; batch < input.size(); batch++) {
          ArrowKernelRows next = input.set(batch, null);
          writer.write(next, CONTEXT);
        }
        long nanos = System.nanoTime() - started;
        long heapBytes = memory.getThreadAllocatedBytes(thread) - heapBefore;
        long writeRequests = allocations.requests - requestsBefore;
        long retainedBytes = allocator.getAllocatedMemory();
        if (iteration >= 0) {
          System.out.printf(java.util.Locale.ROOT,
              "DELTA_BUFFER rows=%d batch_rows=%d payload_bytes=%d mode=%s iteration=%d "
                  + "write_ns=%d rows_per_second=%.0f heap_allocated_bytes=%d "
                  + "input_arrow_bytes=%d retained_arrow_bytes=%d "
                  + "input_arrow_allocation_requests=%d write_arrow_allocation_requests=%d%n",
              rows, BATCH_ROWS, width, upsert ? "upsert" : "append", iteration, nanos,
              rows * 1e9 / nanos, heapBytes, inputBytes, retainedBytes, inputRequests, writeRequests);
        }
        List<Row> actions = new ArrayList<>();
        for (DeltaWriterResult result : writer.prepareCommit()) actions.addAll(result.getDeltaActions());
        table.commit(CloseableIterable.inMemoryIterable(Utils.toCloseableIterator(actions.iterator())),
            "buffer-benchmark", 1, Map.of());
        validate(directory, rows, payload, upsert);
      } finally {
        input.stream().filter(java.util.Objects::nonNull).forEach(ArrowKernelRows::close);
        writer.close();
      }
      assertEquals(0, allocator.getAllocatedMemory(), "writer must release all input Arrow buffers");
    } finally {
      DeltaTestCleanup.deleteDirectory(directory);
    }
  }

  private static void validate(Path directory, int rows, String payload, boolean upsert)
      throws Exception {
    var engine = DefaultEngine.create(new org.apache.hadoop.conf.Configuration());
    var snapshot = TableManager.loadSnapshot(directory.toString()).build(engine);
    Scan scan = snapshot.getScanBuilder().withReadSchema(SCHEMA).build();
    Row state = scan.getScanState(engine);
    var physical = ScanStateRow.getPhysicalDataReadSchema(state);
    Map<Long, Integer> actual = new HashMap<>();
    try (CloseableIterator<FilteredColumnarBatch> files = scan.getScanFiles(engine)) {
      while (files.hasNext()) {
        try (var fileRows = files.next().getRows()) {
          while (fileRows.hasNext()) {
            Row file = fileRows.next();
            try (var data = Scan.transformPhysicalData(engine, state, file,
                engine.getParquetHandler().readParquetFiles(
                    singletonCloseableIterator(InternalScanFileUtils.getAddFileStatus(file)),
                    physical, Optional.empty()).map(result -> result.getData()))) {
              while (data.hasNext()) {
                try (var logical = data.next().getRows()) {
                  while (logical.hasNext()) {
                    Row row = logical.next();
                    assertEquals(null, actual.put(row.getLong(0), row.getInt(1)), "duplicate key");
                    assertEquals(payload, row.getString(2));
                  }
                }
              }
            }
          }
        }
      }
    }
    assertEquals(upsert ? UPSERT_KEYS : rows, actual.size());
    for (int key = 0; key < actual.size(); key++) {
      assertEquals(Integer.valueOf(upsert ? rows - UPSERT_KEYS + key : key), actual.get((long) key));
    }
  }
}

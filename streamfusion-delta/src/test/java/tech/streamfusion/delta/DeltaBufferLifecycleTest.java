package tech.streamfusion.delta;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static org.junit.jupiter.api.Assertions.*;

import io.delta.flink.sink.DeltaSinkConf;
import io.delta.kernel.Scan;
import io.delta.kernel.TableManager;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.arrow.memory.BufferAllocator;
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
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

class DeltaBufferLifecycleTest {
  private static final RowType TYPE = RowType.of(new LogicalType[] {
      new BigIntType(false), new IntType(), new VarCharType(VarCharType.MAX_LENGTH)},
      new String[] {"id", "v", "pt"});
  private static final StructType SCHEMA = new StructType(List.of(
      new StructField("id", LongType.LONG, false), new StructField("v", IntegerType.INTEGER, true),
      new StructField("pt", StringType.STRING, true)));

  @Test
  void completeReplacementReleasesArrowAndRetiredMetadataBeforeCheckpoint() throws Exception {
    try (var fixture = new Fixture()) {
      BufferAllocator previous = null;
      for (int update = 0; update < 100; update++) {
        var next = fixture.write(row(RowKind.INSERT, 1, update, "a"),
            row(RowKind.INSERT, 2, update + 1, "a"));
        if (previous != null) assertEquals(0, previous.getAllocatedMemory());
        assertTrue(next.getAllocatedMemory() > 0);
        assertEquals(1, selections(fixture.writer), "retired batches must not retain metadata");
        previous = next;
      }
      fixture.verify(Map.of(1L, "99|a", 2L, "100|a"));
    }
  }

  @Test
  void partialBatchSurvivesUntilItsLastLiveKeyIsDeleted() throws Exception {
    try (var fixture = new Fixture()) {
      var first = fixture.write(row(RowKind.INSERT, 1, 10, "a"),
          row(RowKind.INSERT, 2, 20, "a"), row(RowKind.INSERT, 3, 30, "a"));
      fixture.write(row(RowKind.UPDATE_AFTER, 1, 11, "a"));
      assertTrue(first.getAllocatedMemory() > 0);
      var deleted = fixture.write(row(RowKind.DELETE, 2, 20, "a"),
          row(RowKind.DELETE, 3, 30, "a"));
      assertEquals(0, first.getAllocatedMemory());
      assertEquals(0, deleted.getAllocatedMemory());
      assertEquals(1, selections(fixture.writer));
      fixture.verify(Map.of(1L, "11|a"));
    }
  }

  @Test
  void primitiveSelectionsGrowAndRetireAcrossBitsetWords() throws Exception {
    try (var fixture = new Fixture()) {
      RowData[] original = new RowData[130];
      RowData[] updates = new RowData[129];
      Map<Long, String> expected = new HashMap<>();
      for (int index = 0; index < original.length; index++) {
        original[index] = row(RowKind.INSERT, index, 0, "a");
        if (index < updates.length) {
          updates[index] = row(RowKind.UPDATE_AFTER, index, 1, "a");
          expected.put((long) index, "1|a");
        }
      }
      var first = fixture.write(original);
      fixture.write(updates);
      assertTrue(first.getAllocatedMemory() > 0);
      fixture.write(row(RowKind.DELETE, 129, 0, "a"));
      assertEquals(0, first.getAllocatedMemory());
      assertEquals(1, selections(fixture.writer));
      fixture.verify(expected);
    }
  }

  @Test
  void deleteAndReinsertWithinOneBatchRetainsTheNewSelection() throws Exception {
    try (var fixture = new Fixture()) {
      var input = fixture.write(row(RowKind.INSERT, 1, 10, "a"),
          row(RowKind.DELETE, 1, 10, "a"), row(RowKind.INSERT, 1, 11, "a"));
      assertTrue(input.getAllocatedMemory() > 0);
      assertEquals(1, selections(fixture.writer));
      fixture.verify(Map.of(1L, "11|a"));
    }
  }

  @Test
  void partitionMovesRetireEmptyPartitionsAndCloseWithoutCommitReleasesEverything() throws Exception {
    try (var fixture = new Fixture()) {
      BufferAllocator previous = fixture.write(row(RowKind.INSERT, 1, 0, "p0"));
      for (int partition = 1; partition < 32; partition++) {
        var next = fixture.write(row(RowKind.UPDATE_AFTER, 1, partition, "p" + partition));
        assertEquals(0, previous.getAllocatedMemory());
        assertEquals(1, partitions(fixture.writer).size());
        previous = next;
      }
      fixture.verify(Map.of(1L, "31|p31"));
    }
    try (var fixture = new Fixture()) {
      fixture.write(row(RowKind.INSERT, 1, 10, "a"));
      fixture.close();
    }
  }

  private static GenericRowData row(RowKind kind, long id, int value, String partition) {
    var row = GenericRowData.of(id, value, StringData.fromString(partition));
    row.setRowKind(kind);
    return row;
  }

  private static Map<?, ?> partitions(NativeDeltaSinkWriter writer) throws Exception {
    var field = NativeDeltaSinkWriter.class.getDeclaredField("partitions");
    field.setAccessible(true);
    return (Map<?, ?>) field.get(writer);
  }

  private static int selections(NativeDeltaSinkWriter writer) throws Exception {
    int count = 0;
    for (Object partition : partitions(writer).values()) {
      var field = partition.getClass().getDeclaredField("selections");
      field.setAccessible(true);
      count += ((Collection<?>) field.get(partition)).size();
    }
    return count;
  }

  private static final class Fixture implements AutoCloseable {
    final Path directory = Files.createTempDirectory("delta-buffer-lifetime");
    final NativeDeltaHadoopTable table;
    final NativeDeltaSinkWriter writer;
    final List<BufferAllocator> allocators = new ArrayList<>();
    boolean closed;

    Fixture() throws Exception {
      table = new NativeDeltaHadoopTable(directory.toUri(), Map.of(), SCHEMA, List.of("pt"));
      table.open();
      writer = new NativeDeltaSinkWriter("lifetime", 0, 0, table,
          new DeltaSinkConf(TYPE, Map.of("write.mode", "upsert", "primary_key", "0")));
    }

    BufferAllocator write(RowData... rows) {
      var allocator = NativeAllocator.SHARED.newChildAllocator("batch-" + allocators.size(), 0, Long.MAX_VALUE);
      allocators.add(allocator);
      var batch = new ArrowKernelRows(RowDataArrowConverter.write(List.of(rows), TYPE, allocator, true),
          TYPE, SCHEMA);
      writer.write(batch, new SinkWriter.Context() {
        @Override public long currentWatermark() { return Long.MIN_VALUE; }
        @Override public Long timestamp() { return null; }
      });
      return allocator;
    }

    void verify(Map<Long, String> expected) throws Exception {
      List<Row> actions = new ArrayList<>();
      for (var result : writer.prepareCommit()) actions.addAll(result.getDeltaActions());
      table.commit(CloseableIterable.inMemoryIterable(Utils.toCloseableIterator(actions.iterator())),
          "lifetime", 1, Map.of());
      for (var allocator : allocators) assertEquals(0, allocator.getAllocatedMemory());
      var engine = DefaultEngine.create(new org.apache.hadoop.conf.Configuration());
      var snapshot = TableManager.loadSnapshot(directory.toString()).build(engine);
      Scan scan = snapshot.getScanBuilder().withReadSchema(SCHEMA).build();
      Row state = scan.getScanState(engine);
      Map<Long, String> actual = new HashMap<>();
      try (var files = scan.getScanFiles(engine)) {
        while (files.hasNext()) {
          try (var fileRows = files.next().getRows()) {
            while (fileRows.hasNext()) {
              Row file = fileRows.next();
              try (var data = Scan.transformPhysicalData(engine, state, file,
                  engine.getParquetHandler().readParquetFiles(
                      singletonCloseableIterator(InternalScanFileUtils.getAddFileStatus(file)),
                      ScanStateRow.getPhysicalDataReadSchema(state), Optional.empty()).map(r -> r.getData()))) {
                while (data.hasNext()) {
                  try (var rows = data.next().getRows()) {
                    while (rows.hasNext()) {
                      Row row = rows.next();
                      assertNull(actual.put(row.getLong(0), row.getInt(1) + "|" + row.getString(2)));
                    }
                  }
                }
              }
            }
          }
        }
      }
      assertEquals(expected, actual);
    }

    @Override public void close() throws Exception {
      if (closed) return;
      closed = true;
      writer.close();
      for (var allocator : allocators) {
        assertEquals(0, allocator.getAllocatedMemory());
        allocator.close();
      }
      DeltaTestCleanup.deleteDirectory(directory);
    }
  }
}

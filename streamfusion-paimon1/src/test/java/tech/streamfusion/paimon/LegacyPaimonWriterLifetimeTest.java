package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.sink.GlobalFullCompactionSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteState;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.compat.ListCollectors;
import tech.streamfusion.operator.RowDataArrowConverter;

class LegacyPaimonWriterLifetimeTest {
  @TempDir java.nio.file.Path directory;
  private static final RowType TYPE = RowType.of(new IntType(), new BigIntType(), new IntType());

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void delayedDataCommitPreservesCompactionAndUnrelatedIdleCleanup(boolean nativeWriter)
      throws Exception {
    var path = new Path(directory.toUri());
    var io = LocalFileIO.create();
    new SchemaManager(io, path)
        .createTable(
            new Schema(
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT().notNull()),
                        DataTypes.FIELD(1, "v", DataTypes.BIGINT()),
                        DataTypes.FIELD(2, "pt", DataTypes.INT().notNull()))
                    .getFields(),
                List.of("pt"),
                List.of("id", "pt"),
                Map.of(
                    "bucket",
                    "1",
                    "changelog-producer",
                    "full-compaction",
                    "full-compaction.delta-commits",
                    "3",
                    "num-sorted-run.compaction-trigger",
                    "100",
                    "num-levels",
                    "4"),
                ""));
    var table = FileStoreTableFactory.create(io, path);
    try (var disk = new IOManagerAsync();
        var allocator = new RootAllocator();
        var router = table.newStreamWriteBuilder().newWrite();
        var commit = table.newStreamWriteBuilder().withCommitUser("lifetime").newCommit()) {
      var delegate =
          new GlobalFullCompactionSinkWrite(
              table,
              "lifetime",
              new MemoryState(),
              disk,
              false,
              false,
              3,
              true,
              new HeapMemorySegmentPool(
                  table.coreOptions().writeBufferSize(), table.coreOptions().pageSize()),
              null);
      StoreSinkWrite write = nativeWriter ? new NativeKeyValueSinkWrite(table, delegate) : delegate;
      try {
        feed(write, router, allocator, 1, 10, 0);
        feed(write, router, allocator, 2, 20, 1);
        commit.commit(
            1,
            write.prepareCommit(true, 1).stream()
                .map(c -> (org.apache.paimon.table.sink.CommitMessage) c.wrappedCommittable())
                .collect(ListCollectors.toList()));
        commit.commit(
            2,
            write.prepareCommit(false, 2).stream()
                .map(c -> (org.apache.paimon.table.sink.CommitMessage) c.wrappedCommittable())
                .collect(ListCollectors.toList()));
        feed(write, router, allocator, 1, 100, 0);
        var pending =
            write.prepareCommit(false, 4).stream()
                .map(c -> (org.apache.paimon.table.sink.CommitMessage) c.wrappedCommittable())
                .collect(ListCollectors.toList());
        assertEquals(
            1, delegate.getWrite().checkpoint().size(), "only the modified bucket remains open");
        var compacted =
            write.prepareCommit(false, 6).stream()
                .map(c -> (org.apache.paimon.table.sink.CommitMessage) c.wrappedCommittable())
                .collect(ListCollectors.toList());
        commit.commit(4, pending);
        commit.commit(6, compacted);
        var builder = table.newReadBuilder();
        var scan = builder.newStreamScan();
        scan.restore(1L);
        var changes = new ArrayList<String>();
        while (scan.checkpoint() <= table.snapshotManager().latestSnapshotId()) {
          try (var reader = builder.newRead().createReader(scan.plan())) {
            reader.forEachRemaining(
                row ->
                    changes.add(
                        row.getRowKind().shortString()
                            + "|"
                            + row.getInt(0)
                            + "|"
                            + row.getLong(1)
                            + "|"
                            + row.getInt(2)));
          }
        }
        assertEquals(4, changes.size(), changes.toString());
        assertTrue(changes.contains("-U|1|10|0"), changes.toString());
        assertTrue(changes.contains("+U|1|100|0"), changes.toString());
      } finally {
        write.close();
      }
    }
  }

  private static void feed(
      StoreSinkWrite write,
      StreamTableWrite router,
      RootAllocator allocator,
      int id,
      long value,
      int partition)
      throws Exception {
    var row = GenericRow.of(id, value, partition);
    if (write instanceof NativeKeyValueSinkWrite) {
      NativeKeyValueSinkWrite nativeWrite = ((NativeKeyValueSinkWrite) write);

      nativeWrite.writeBundle(
          router.getPartition(row).copy(),
          router.getBucket(row),
          RowDataArrowConverter.write(
              List.of(GenericRowData.of(id, value, partition)), TYPE, allocator, false));
    } else {
      write.write(row);
    }
  }

  private static final class MemoryState implements StoreSinkWriteState {
    private final Map<String, List<StateValue>> values = new java.util.HashMap<>();

    @Override
    public StateValueFilter stateValueFilter() {
      return (name, partition, bucket) -> true;
    }

    @Override
    public List<StateValue> get(String table, String key) {
      return values.get(table + "/" + key);
    }

    @Override
    public void put(String table, String key, List<StateValue> state) {
      values.put(table + "/" + key, state);
    }

    @Override
    public void snapshotState() {}
  }
}

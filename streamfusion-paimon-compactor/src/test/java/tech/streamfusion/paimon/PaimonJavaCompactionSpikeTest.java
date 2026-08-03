package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.Native;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.state.StateTableCompactor;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.junit.jupiter.api.Test;

/**
 * The decisive experiment for "Java Paimon owns state-table compaction": the native store
 * (paimon-rust) writes and commits a state table across several checkpoints, with the module's
 * compactor folding each barrier's level-0 run away between the data commit and a re-pinning
 * second checkpoint — the production barrier protocol, mandatory now that state tables always
 * carry deletion vectors (raw reads skip level 0, so an unmaintained run is invisible to both
 * implementations). Java then reads the rows back through the vector-masked files, runs a full
 * compaction (pick + sequence-preserving rewrite + commit), and the native store restores from
 * the Java-compacted snapshot and keeps operating. Every arrow of the cross-implementation
 * compatibility diagram is exercised: Rust manifests/data read by Java, Java manifests/data read
 * by Rust. Runs on parquet state files — the format both implementations share today (Java's
 * vortex format is unreleased, targeted at Paimon 2.0).
 */
class PaimonJavaCompactionSpikeTest {

  private static final RowType INPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"k", "v"});
  private static final RowType OUTPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType()},
          new String[] {"key0", "result0"});

  @Test
  void rustWritesJavaCompactsRustRestores() throws Exception {
    String tableDir = Files.createTempDirectory("spike-table").toString();

    // --- Rust writes: SUM(v) GROUP BY k over four checkpoints. Rounds 1..3 run the production
    // barrier protocol — data commit, the module's minimal compaction round (up-level the run,
    // maintain the vectors), and a second checkpoint re-pinning the maintenance snapshot;
    // without it the next round's probe would miss the level-0 rows entirely. Round 4 commits
    // its data and stops, leaving one uncompacted level-0 run for Java's full compaction to
    // fold.
    long handle = createAggregator(tableDir, new String[0], new String[0]);
    long snapshotId = -1;
    try (BufferAllocator allocator = new RootAllocator();
        StateTableCompactor.Session session =
            new JavaPaimonStateCompactor().open(tableDir)) {
      for (int round = 1; round <= 4; round++) {
        update(allocator, handle, insertBatch(allocator, round));
        String[] manifest = Native.checkpointPaimonGroupAggregator(handle);
        if (round < 4) {
          session.compact(round);
          manifest = Native.checkpointPaimonGroupAggregator(handle);
        }
        snapshotId = Long.parseLong(manifest[0]);
      }
    } finally {
      Native.closePaimonGroupAggregator(handle);
    }
    assertTrue(snapshotId > 0);

    // --- Java reads the Rust-written table through the deletion vectors: round 4's uncompacted
    // level-0 run is invisible (the contract the barrier protocol exists for — Java agrees with
    // the native store), so the sums show exactly rounds 1..3: 10k*(1+2+3) = 60k.
    FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), new Path(tableDir));
    Map<Long, List<Long>> rows = readState(table);
    assertEquals(3, rows.size());
    for (long key = 1; key <= 3; key++) {
      assertEquals(3, rows.get(key).get(0), "records for key " + key);
      assertEquals(60 * key, rows.get(key).get(1), "sum for key " + key);
    }
    Set<Integer> buckets = new HashSet<>();
    for (Split split : table.newReadBuilder().newScan().plan().splits()) {
      buckets.add(((DataSplit) split).bucket());
    }

    // --- Java compacts with its own machinery (pick, sequence-preserving rewrite, commit),
    // folding the level-0 run into the vector-masked view.
    StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser("java-spike");
    try (IOManager ioManager =
            IOManager.create(Files.createTempDirectory("spike-lookup").toString());
        StreamTableWrite write = writeBuilder.newWrite();
        StreamTableCommit commit = writeBuilder.newCommit()) {
      // The lookup rewriter of a deletion-vector table spills its key-position files through an
      // IOManager, exactly as the module's own writer does.
      write.withIOManager(ioManager);
      for (int bucket : buckets) {
        write.compact(BinaryRow.EMPTY_ROW, bucket, true);
      }
      List<CommitMessage> messages = write.prepareCommit(true, 1);
      commit.commit(1, messages);
    }
    FileStoreTable reopened =
        FileStoreTableFactory.create(LocalFileIO.create(), new Path(tableDir));
    int filesAfter = 0;
    for (Split split : reopened.newReadBuilder().newScan().plan().splits()) {
      filesAfter += ((DataSplit) split).dataFiles().size();
    }
    assertEquals(buckets.size(), filesAfter, "full compaction leaves one file per bucket");
    // Round 4's rows surface once compacted: 10k*(1+2+3+4) = 100k over 4 records per key.
    Map<Long, List<Long>> compacted = readState(reopened);
    for (long key = 1; key <= 3; key++) {
      assertEquals(4, compacted.get(key).get(0), "records for key " + key);
      assertEquals(100 * key, compacted.get(key).get(1), "sum for key " + key);
    }
    long javaSnapshot = reopened.snapshotManager().latestSnapshotId();
    assertTrue(javaSnapshot > snapshotId);

    // --- Rust restores from the Java-compacted snapshot and keeps operating: an update to each
    // key must emit -U with the accumulated sum (proving the probe read Java's commit) and +U on
    // top of it.
    String restoredDir = Files.createTempDirectory("spike-restored").toString();
    long restored =
        createAggregator(restoredDir, new String[] {tableDir}, new String[] {Long.toString(javaSnapshot)});
    try (BufferAllocator allocator = new RootAllocator()) {
      List<List<Object>> out =
          update(allocator, restored, insertBatch(allocator, 100));
      List<List<Object>> expected = new ArrayList<>();
      for (long key = 1; key <= 3; key++) {
        expected.add(List.of(RowKind.UPDATE_BEFORE, key, 100 * key));
        expected.add(List.of(RowKind.UPDATE_AFTER, key, 100 * key + 10 * key * 100));
      }
      assertEquals(expected, out);
    } finally {
      Native.closePaimonGroupAggregator(restored);
    }
  }

  private static long createAggregator(String tableDir, String[] sources, String[] snapshots) {
    return Native.createPaimonGroupAggregator(
        new int[] {0}, // SUM
        new int[] {0}, // BIGINT
        new int[] {1},
        new int[] {0},
        new int[] {-1}, // key timestamp precisions
        new int[] {-1}, // filter columns
        new int[] {-1}, // count columns
        new int[] {-1}, // distinct view columns
        -1,
        true,
        false,
        0L, // state TTL off
        0L, // restore clock (unused with TTL off)
        -1L,
        tableDir,
        128,
        1,
        "parquet",
        "uncompressed",
        sources,
        snapshots,
        0,
        127,
        true);
  }

  /** Rows (k, 10k * round) for k in 1..3, so sums stay distinguishable per round. */
  private static VectorSchemaRoot insertBatch(BufferAllocator allocator, int round) {
    List<RowData> rows = new ArrayList<>();
    for (long key = 1; key <= 3; key++) {
      rows.add(GenericRowData.of(key, 10 * key * round));
    }
    return RowDataArrowConverter.write(rows, INPUT, allocator);
  }

  private static List<List<Object>> update(
      BufferAllocator allocator, long handle, VectorSchemaRoot in) {
    try (CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
        ArrowArray inArray = ArrowArray.allocateNew(allocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, in, dictionaries, inArray, inSchema);
      Native.updatePaimonGroupAggregator(
          handle,
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          0, // wall clock, unused with TTL off
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      List<List<Object>> rows = new ArrayList<>();
      try (VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries)) {
        for (RowData row : RowDataArrowConverter.read(out, OUTPUT)) {
          rows.add(List.of(row.getRowKind(), row.getLong(0), row.getLong(1)));
        }
      } finally {
        in.close();
      }
      return rows;
    }
  }

  /** key -> [records, sum], decoding the Flink BinaryRow key bytes Java sees as VARBINARY. */
  private static Map<Long, List<Long>> readState(FileStoreTable table) throws Exception {
    ReadBuilder readBuilder = table.newReadBuilder();
    List<Split> splits = readBuilder.newScan().plan().splits();
    Map<Long, List<Long>> rows = new HashMap<>();
    try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(splits)) {
      reader.forEachRemaining(
          row -> {
            byte[] keyBytes = row.getBinary(1);
            BinaryRowData key = new BinaryRowData(1);
            key.pointTo(MemorySegmentFactory.wrap(keyBytes), 0, keyBytes.length);
            long records = row.getLong(2);
            long sum = row.isNullAt(3) ? Long.MIN_VALUE : row.getLong(3);
            rows.put(key.getLong(0), List.of(records, sum));
          });
    }
    return rows;
  }
}

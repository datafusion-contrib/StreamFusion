package io.github.jordepic.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.Native;
import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
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
 * (paimon-rust) writes and commits a state table across several checkpoints; stock, released Java
 * Paimon (1.4.2) opens the same directory, reads the rows back, runs its own full compaction
 * (pick + sequence-preserving rewrite + commit); and the native store then restores from the
 * Java-compacted snapshot and keeps operating. Every arrow of the cross-implementation
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

    // --- Rust writes: SUM(v) GROUP BY k over four checkpoints -> four level-0 runs.
    long handle = createAggregator(tableDir, new String[0], new long[0]);
    long snapshotId = -1;
    try (BufferAllocator allocator = new RootAllocator()) {
      for (int round = 1; round <= 4; round++) {
        // Every round touches keys 1..3, so each bucket accumulates one run per round.
        update(allocator, handle, insertBatch(allocator, round));
        String[] manifest =
            Native.checkpointPaimonGroupAggregator(
                handle, Files.createTempDirectory("spike-cp" + round).toString());
        snapshotId = Long.parseLong(manifest[0]);
      }
    } finally {
      Native.closePaimonGroupAggregator(handle);
    }
    assertTrue(snapshotId > 0);

    // --- Java reads the Rust-written table: sums must be 4 rounds of accumulation.
    FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), new Path(tableDir));
    Map<Long, List<Long>> rows = readState(table);
    assertEquals(3, rows.size());
    // Each round r contributed 10k*r, so after rounds 1..4 the sum is 10k*(1+2+3+4) = 100k.
    for (long key = 1; key <= 3; key++) {
      assertEquals(4, rows.get(key).get(0), "records for key " + key);
      assertEquals(100 * key, rows.get(key).get(1), "sum for key " + key);
    }
    Set<Integer> buckets = new HashSet<>();
    int filesBefore = 0;
    for (Split split : table.newReadBuilder().newScan().plan().splits()) {
      DataSplit dataSplit = (DataSplit) split;
      buckets.add(dataSplit.bucket());
      filesBefore += dataSplit.dataFiles().size();
    }
    assertTrue(filesBefore > buckets.size(), "several runs per bucket before compaction");

    // --- Java compacts with its own machinery (pick, sequence-preserving rewrite, commit).
    StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser("java-spike");
    try (StreamTableWrite write = writeBuilder.newWrite();
        StreamTableCommit commit = writeBuilder.newCommit()) {
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
    assertEquals(rows, readState(reopened), "compaction must not change the state's content");
    long javaSnapshot = reopened.snapshotManager().latestSnapshotId();
    assertTrue(javaSnapshot > snapshotId);

    // --- Rust restores from the Java-compacted snapshot and keeps operating: an update to each
    // key must emit -U with the accumulated sum (proving the probe read Java's commit) and +U on
    // top of it.
    String restoredDir = Files.createTempDirectory("spike-restored").toString();
    long restored =
        createAggregator(restoredDir, new String[] {tableDir}, new long[] {javaSnapshot});
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

  private static long createAggregator(String tableDir, String[] sources, long[] snapshots) {
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
        -1L,
        tableDir,
        128,
        "parquet",
        "uncompressed",
        sources,
        snapshots,
        0,
        127);
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

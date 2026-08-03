package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.state.StateTableCompactor;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;

/**
 * Physical TTL cleanup — the RocksDB-compaction-filter analog: a session opened with the
 * strategy's record-level retention options makes every compaction rewrite drop rows whose
 * {@code ts} is past the retention. The read path already treats such rows as expired, so these
 * tests assert the space-reclaim side: expired rows physically vanish, and — the correctness
 * boundary — a logically live row is NEVER dropped, however Paimon truncates the clock and the
 * epoch-millis column to whole seconds.
 */
class JavaPaimonStateCompactorTtlTest {

  private static final long TTL_MILLIS = 60 * 60 * 1000L;

  /** What {@code PaimonSnapshotStrategy.recordLevelExpireOptions(TTL_MILLIS)} hands a session
   * (pinned there by its own unit test): the padded {@code ceil(ttl/1000) + 1} seconds. */
  private static final Map<String, String> TTL_OPTIONS =
      Map.of("record-level.expire-time", "3601s", "record-level.time-field", "ts");

  private static final byte LIVE = 1;
  private static final byte NEAR_EXPIRY = 2;
  private static final byte EXPIRED = 3;
  private static final byte NULL_TS = 4;

  /** Paimon's record-level expire, observed end to end on a compaction rewrite: the BIGINT ts is
   * auto-detected as epoch millis (values >= 10^12), an expired row is physically dropped, a
   * NULL ts is kept forever, and a row short of logical expiry survives the truncation pad. */
  @Test
  void compactionRewriteDropsOnlyRowsPastLogicalExpiry() throws Exception {
    String dir = createStateShapedTable(false);
    long now = System.currentTimeMillis();
    write(
        dir,
        1,
        row(LIVE, 10, now - TTL_MILLIS + 60_000),
        // Just short of logical expiry: the padded expire seconds must keep it through the
        // whole-second truncation of both the clock and the ts column (the margin is 5s rather
        // than 1ms only so a slow filesystem between this write and the compaction's own clock
        // read cannot flake the test; the exact-boundary arithmetic is pinned by the strategy's
        // recordLevelExpireOptions unit test).
        row(NEAR_EXPIRY, 20, now - TTL_MILLIS + 5_000),
        row(EXPIRED, 30, now - TTL_MILLIS - 60_000),
        rowWithNullTs(NULL_TS, 40));

    // A zero cadence escalates the first shaping round to a full compaction: the one rewrite
    // this vector-less test table is guaranteed to see (without deletion vectors, barrier
    // rounds are trigger-gated).
    try (StateTableCompactor.Session session =
        new JavaPaimonStateCompactor(0).open(dir, TTL_OPTIONS)) {
      session.shape(100);
    }

    Map<Byte, Long[]> rows = readState(dir);
    assertEquals(Set.of(LIVE, NEAR_EXPIRY, NULL_TS), rows.keySet(), "only the expired row drops");
    assertEquals(10L, rows.get(LIVE)[0]);
    assertEquals(20L, rows.get(NEAR_EXPIRY)[0]);
    assertEquals(40L, rows.get(NULL_TS)[0]);
    assertNull(rows.get(NULL_TS)[1], "a NULL ts row is kept forever, untouched");
  }

  /** On a deletion-vector table the barrier's minimal round already rewrites every level-0 run,
   * so retention rides along at every checkpoint: the up-level itself drops the expired row. */
  @Test
  void barrierRoundAppliesRetentionOnDeletionVectorTables() throws Exception {
    String dir = createStateShapedTable(true);
    long now = System.currentTimeMillis();
    write(
        dir,
        1,
        row(LIVE, 10, now - TTL_MILLIS + 60_000),
        row(EXPIRED, 30, now - TTL_MILLIS - 60_000),
        rowWithNullTs(NULL_TS, 40));

    try (StateTableCompactor.Session session =
        new JavaPaimonStateCompactor().open(dir, TTL_OPTIONS)) {
      session.compact(100);
    }

    assertEquals(0, levelZeroFiles(dir), "the barrier round must have up-leveled the run");
    assertEquals(Set.of(LIVE, NULL_TS), readState(dir).keySet());
  }

  /**
   * The periodic full round (RocksDB's periodicCompactionSeconds analog): cold files are never
   * picked by the barrier's minimal round or the ordinary shaping triggers (pinned here on a
   * vector-less table, whose barrier rounds are all trigger-gated), so their expired rows
   * linger until the wall-clock cadence escalates a shaping round to a full compaction.
   */
  @Test
  void periodicFullCompactionReclaimsColdFiles() throws Exception {
    String dir = createStateShapedTable(false);
    long now = System.currentTimeMillis();
    write(
        dir,
        1,
        row(LIVE, 10, now - TTL_MILLIS + 60_000),
        row(EXPIRED, 30, now - TTL_MILLIS - 60_000));

    // With retention but the default (long) cadence, neither a barrier round nor an ordinary
    // shaping round touches the single settled run — expired rows survive untouched.
    try (StateTableCompactor.Session session =
        new JavaPaimonStateCompactor().open(dir, TTL_OPTIONS)) {
      session.compact(101);
      session.shape(102);
    }
    assertEquals(Set.of(LIVE, EXPIRED), readState(dir).keySet());

    // A lowered cadence escalates the first shaping round to a full compaction: reclaimed
    // without any normal compaction trigger firing.
    try (StateTableCompactor.Session session =
        new JavaPaimonStateCompactor(0).open(dir, TTL_OPTIONS)) {
      session.shape(103);
    }
    assertEquals(Set.of(LIVE), readState(dir).keySet());

    // The full pick is stats-guided: with nothing left past the retention, the next periodic
    // round rewrites (and commits) nothing.
    long settled = latestSnapshotId(dir);
    try (StateTableCompactor.Session session =
        new JavaPaimonStateCompactor(0).open(dir, TTL_OPTIONS)) {
      session.shape(104);
    }
    assertEquals(settled, latestSnapshotId(dir), "an all-live table is not rewritten");
    assertEquals(Set.of(LIVE), readState(dir).keySet());
  }

  /** A session without retention options must behave exactly as before: no rewrite ever drops a
   * row, however stale its ts. */
  @Test
  void sessionWithoutOptionsNeverDropsRows() throws Exception {
    String dir = createStateShapedTable(false);
    long now = System.currentTimeMillis();
    write(dir, 1, row(EXPIRED, 30, now - TTL_MILLIS - 60_000), rowWithNullTs(NULL_TS, 40));

    try (StateTableCompactor.Session session = new JavaPaimonStateCompactor(0).open(dir)) {
      session.compact(100);
      session.shape(101);
    }

    assertEquals(Set.of(EXPIRED, NULL_TS), readState(dir).keySet());
  }

  /** The native state-table shape: pk (kg INT, k BYTES), a value column, the trailing nullable
   * BIGINT ts column the TTL'd stores append (epoch millis). */
  private static String createStateShapedTable(boolean deletionVectors) throws Exception {
    String dir = Files.createTempDirectory("ttl-state-table").toString();
    Schema.Builder schema =
        Schema.newBuilder()
            .column("kg", DataTypes.INT().notNull())
            .column("k", DataTypes.BYTES().notNull())
            .column("v", DataTypes.BIGINT())
            .column("ts", DataTypes.BIGINT())
            .primaryKey("kg", "k")
            .option("bucket", "1");
    if (deletionVectors) {
      schema.option("deletion-vectors.enabled", "true");
    }
    new SchemaManager(LocalFileIO.create(), new Path(dir)).createTable(schema.build());
    return dir;
  }

  private static GenericRow row(byte key, long value, long tsMillis) {
    return GenericRow.of(0, new byte[] {key}, value, tsMillis);
  }

  private static GenericRow rowWithNullTs(byte key, long value) {
    return GenericRow.of(0, new byte[] {key}, value, null);
  }

  private static void write(String dir, long commitId, GenericRow... rows) throws Exception {
    FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), new Path(dir));
    StreamWriteBuilder builder = table.newStreamWriteBuilder().withCommitUser("ttl-test-writer");
    try (IOManager ioManager =
            IOManager.create(Files.createTempDirectory("ttl-test-lookup").toString());
        StreamTableWrite write = builder.newWrite();
        StreamTableCommit commit = builder.newCommit()) {
      write.withIOManager(ioManager);
      for (GenericRow row : rows) {
        write.write(row);
      }
      commit.commit(commitId, write.prepareCommit(false, commitId));
    }
  }

  /** key byte -> [v, ts (nullable)] from a fresh scan, so only physically present rows count. */
  private static Map<Byte, Long[]> readState(String dir) throws Exception {
    FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), new Path(dir));
    ReadBuilder readBuilder = table.newReadBuilder();
    Map<Byte, Long[]> rows = new HashMap<>();
    try (RecordReader<InternalRow> reader =
        readBuilder.newRead().createReader(readBuilder.newScan().plan().splits())) {
      reader.forEachRemaining(
          row ->
              rows.put(
                  row.getBinary(1)[0],
                  new Long[] {row.getLong(2), row.isNullAt(3) ? null : row.getLong(3)}));
    }
    return rows;
  }

  private static int levelZeroFiles(String dir) throws Exception {
    FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), new Path(dir));
    int levelZero = 0;
    for (Split split : table.newReadBuilder().newScan().plan().splits()) {
      for (DataFileMeta file : ((DataSplit) split).dataFiles()) {
        if (file.level() == 0) {
          levelZero++;
        }
      }
    }
    return levelZero;
  }

  private static long latestSnapshotId(String dir) throws Exception {
    Long latest =
        FileStoreTableFactory.create(LocalFileIO.create(), new Path(dir))
            .snapshotManager()
            .latestSnapshotId();
    assertTrue(latest != null && latest > 0);
    return latest;
  }
}

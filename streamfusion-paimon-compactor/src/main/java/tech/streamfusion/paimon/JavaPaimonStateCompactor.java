package tech.streamfusion.paimon;

import tech.streamfusion.state.StateTableCompactor;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import org.apache.paimon.CoreOptions;
import java.util.List;
import java.util.Comparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.Snapshot;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.SnapshotManager;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Table maintenance by stock Java Paimon. A session holds the table and one long-lived minimal
 * writer across barriers — the dedicated-compaction-job pattern ({@code StoreCompactOperator}):
 * after each barrier's native data commit, the session reads only that snapshot's delta
 * manifest and folds the new level-0 files into the held writer via {@code notifyNewFiles},
 * then asks compaction to up-level them. The full manifest chain is scanned once at session
 * open, not per barrier — on churn-heavy state the per-barrier rebuild was the dominant
 * maintenance cost, dwarfing the compaction itself. Shaping rounds use a throwaway writer under
 * the table's own triggers and invalidate the held view when they commit (a long-lived writer
 * must be the table's only compactor to keep its level view truthful). Sequence numbers are
 * preserved by Paimon's rewriter and deletions drop exactly per its own rules.
 */
public class JavaPaimonStateCompactor implements StateTableCompactor {

  private static final String COMMIT_USER = "streamfusion-compactor";
  private static final String SHAPE_COMMIT_USER = "streamfusion-shaper";

  /**
   * How often a shaping round escalates to a full compaction when the session carries retention
   * options — the analog of RocksDB's periodicCompactionSeconds: a file the universal triggers
   * never pick again would otherwise keep its expired rows forever. Paimon's full pick is
   * stats-guided (a max-level run only rewrites the files whose min timestamp is past the
   * retention), so on a steady table the periodic round is usually a no-op scan. One hour is
   * deliberately conservative; expired rows are already invisible to reads, this only bounds how
   * long their bytes linger.
   */
  private static final long DEFAULT_FULL_COMPACTION_INTERVAL_MILLIS = 60 * 60 * 1000L;

  private final long fullCompactionIntervalMillis;

  public JavaPaimonStateCompactor() {
    this(DEFAULT_FULL_COMPACTION_INTERVAL_MILLIS);
  }

  JavaPaimonStateCompactor(long fullCompactionIntervalMillis) {
    this.fullCompactionIntervalMillis = fullCompactionIntervalMillis;
  }

  @Override
  public boolean available() {
    try {
      Class.forName("org.apache.paimon.table.FileStoreTableFactory");
      return true;
    } catch (ClassNotFoundException | NoClassDefFoundError e) {
      return false;
    }
  }

  @Override
  public boolean supports(String fileFormat) {
    // The deployed Paimon must have a reader/writer for the state files (vortex arrives with
    // Paimon 2.0; parquet is always in the bundle).
    try {
      org.apache.paimon.factories.FormatFactoryUtil.discoverFactory(
          JavaPaimonStateCompactor.class.getClassLoader(), fileFormat.toLowerCase());
      return true;
    } catch (RuntimeException e) {
      return false;
    }
  }

  /**
   * Probes the deployed Paimon's slice comparator with the state tables' exact key shape
   * (INT key group, VARBINARY key): releases without the binary-field fix (apache/paimon#8873)
   * throw ClassCastException the first time lookup compaction seeks a lookup file, so a broken
   * deployment fails closed at backend creation rather than at the first post-restore barrier.
   */
  @Override
  public boolean supportsDeletionVectors() {
    try {
      RowCompactedSerializer serializer =
          new RowCompactedSerializer(RowType.of(DataTypes.INT(), DataTypes.BYTES()));
      Comparator<MemorySlice> comparator = serializer.createSliceComparator();
      MemorySlice small =
          MemorySlice.wrap(serializer.serializeToBytes(GenericRow.of(1, new byte[] {1})));
      MemorySlice large =
          MemorySlice.wrap(serializer.serializeToBytes(GenericRow.of(1, new byte[] {2})));
      return comparator.compare(small, large) < 0 && comparator.compare(large, small) > 0;
    } catch (RuntimeException | Error probeFailure) {
      return false;
    }
  }

  @Override
  public Session open(String tableDirectory) {
    return open(tableDirectory, Map.of());
  }

  @Override
  public Session open(String tableDirectory, Map<String, String> dynamicOptions) {
    return new PaimonSession(tableDirectory, dynamicOptions, fullCompactionIntervalMillis);
  }

  private static final class PaimonSession implements Session {

    private final String tableDirectory;
    /** Applied to every writer the session creates (see {@link StateTableCompactor#open(String,
     * Map)}); with the retention options present, every compaction rewrite drops rows past it. */
    private final Map<String, String> dynamicOptions;
    private final long fullCompactionIntervalMillis;
    /** Wall-clock instant the next shaping round escalates to a full compaction; never with an
     * option-less session (nothing expires, so cold files need no periodic rewrite). */
    private long fullCompactionDeadline;

    private FileStoreTable table;
    private StreamTableWrite write;
    private StreamTableCommit commit;
    private IOManager ioManager;
    /** Newest snapshot whose files the held writer's level view includes. */
    private long lastSeenSnapshot;
    private int buckets;

    private PaimonSession(
        String tableDirectory,
        Map<String, String> dynamicOptions,
        long fullCompactionIntervalMillis) {
      this.tableDirectory = tableDirectory;
      this.dynamicOptions = dynamicOptions;
      this.fullCompactionIntervalMillis = fullCompactionIntervalMillis;
      this.fullCompactionDeadline =
          dynamicOptions.isEmpty()
              ? Long.MAX_VALUE
              : System.currentTimeMillis() + fullCompactionIntervalMillis;
    }

    @Override
    public void compact(long round) throws Exception {
      if (write == null && !openWriter()) {
        return; // nothing committed yet; retry next round
      }
      syncForeignCommits();
      for (int bucket = 0; bucket < buckets; bucket++) {
        write.compact(BinaryRow.EMPTY_ROW, bucket, false);
      }
      List<CommitMessage> messages = write.prepareCommit(true, round);
      // Nothing picked -> no snapshot; an empty maintenance commit every barrier would bloat
      // snapshot history for no work.
      boolean empty =
          messages.stream().allMatch(message -> ((CommitMessageImpl) message).isEmpty());
      if (!empty) {
        commit.commit(round, messages);
        // The writer applied its own compaction result internally; fold the commit into the
        // watermark so the delta walk never revisits it.
        Long latest = table.snapshotManager().latestSnapshotId();
        if (latest != null) {
          lastSeenSnapshot = latest;
        }
      }
    }

    /**
     * Opens the held table and writer, pinned to the barrier's minimal strategy: with the
     * universal triggers unreachable, ForceUpLevel0Compaction falls through to exactly the
     * correctness-critical rewrite — up-level the barrier's level-0 runs, marking overwritten
     * rows in higher levels through the lookup index instead of merging them. num-levels must
     * be pinned to the table's real value first: its default is derived from the run trigger,
     * and deriving it from MAX_VALUE would ask Levels for two billion runs. The writer's bucket
     * views restore lazily from the manifests on first touch — the only full-chain scan the
     * session ever pays.
     */
    private boolean openWriter() throws Exception {
      FileStoreTable fresh =
          FileStoreTableFactory.create(LocalFileIO.create(), new Path(tableDirectory));
      Long latest = fresh.snapshotManager().latestSnapshotId();
      if (latest == null) {
        return false;
      }
      int bucketCount = fresh.coreOptions().bucket();
      if (bucketCount <= 0) {
        return false;
      }
      Map<String, String> options = new HashMap<>(dynamicOptions);
      options.put(CoreOptions.NUM_LEVELS.key(), String.valueOf(fresh.coreOptions().numLevels()));
      options.put(
          CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER.key(),
          String.valueOf(Integer.MAX_VALUE));
      options.put(
          CoreOptions.COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT.key(),
          String.valueOf(Integer.MAX_VALUE));
      table = fresh.copy(options);
      buckets = bucketCount;
      StreamWriteBuilder builder = table.newStreamWriteBuilder().withCommitUser(COMMIT_USER);
      // Lookup compaction (the deletion-vector rewriter) spills its key-position lookup files
      // through an IOManager; give it scratch space under the JVM temp dir.
      ioManager =
          IOManager.create(Files.createTempDirectory("streamfusion-compactor-lookup").toString());
      write = builder.newWrite();
      write.withIOManager(ioManager);
      commit = builder.newCommit();
      lastSeenSnapshot = latest;
      return true;
    }

    /**
     * Folds commits the held writer did not make — the native store's barrier data commits —
     * into its level view by reading just each snapshot's delta manifest, the
     * dedicated-compaction-job pattern. Our own minimal commits are skipped (the writer applied
     * those results internally); shaping commits never reach this walk because a shaping round
     * that commits invalidates the whole session. Foreign commits are pure appends (the native
     * store expresses deletion as rows, never as manifest deletes), so ADD entries are the
     * complete delta. notifyNewFiles itself drops files a bucket's lazy restore already saw.
     */
    private void syncForeignCommits() throws Exception {
      SnapshotManager snapshots = table.snapshotManager();
      Long latest = snapshots.latestSnapshotId();
      if (latest == null || latest <= lastSeenSnapshot) {
        return;
      }
      for (long id = lastSeenSnapshot + 1; id <= latest; id++) {
        Snapshot snapshot = snapshots.snapshot(id);
        if (COMMIT_USER.equals(snapshot.commitUser())) {
          continue;
        }
        Map<Integer, List<DataFileMeta>> byBucket = new HashMap<>();
        Iterator<ManifestEntry> entries =
            table.store()
                .newScan()
                .withSnapshot(snapshot)
                .withKind(ScanMode.DELTA)
                .dropStats()
                .readFileIterator();
        while (entries.hasNext()) {
          ManifestEntry entry = entries.next();
          if (entry.kind() == FileKind.ADD) {
            byBucket.computeIfAbsent(entry.bucket(), b -> new ArrayList<>()).add(entry.file());
          }
        }
        for (Map.Entry<Integer, List<DataFileMeta>> bucket : byBucket.entrySet()) {
          // notifyNewFiles lives on the implementation, not the StreamTableWrite interface —
          // the same cast Paimon's own StoreCompactOperator relies on through StoreSinkWrite.
          ((TableWriteImpl<?>) write)
              .notifyNewFiles(id, BinaryRow.EMPTY_ROW, bucket.getKey(), bucket.getValue());
        }
      }
      lastSeenSnapshot = latest;
    }

    @Override
    public void shape(long round) throws Exception {
      // Ordinary universal picks under the table's own triggers, through a throwaway writer —
      // a distinct commit user keeps the identifier sequence independent of the barrier rounds
      // (Paimon dedupes per user). A commit rewrites files the held writer believes live, so it
      // invalidates the session; the next barrier's open pays one restore scan, amortized over
      // the many barriers between trigger-gated shaping commits.
      FileStoreTable fresh =
          FileStoreTableFactory.create(LocalFileIO.create(), new Path(tableDirectory));
      if (fresh.snapshotManager().latestSnapshotId() == null) {
        return;
      }
      int bucketCount = fresh.coreOptions().bucket();
      if (bucketCount <= 0) {
        return;
      }
      if (!dynamicOptions.isEmpty()) {
        fresh = fresh.copy(dynamicOptions);
      }
      // The periodic full round (RocksDB's periodicCompactionSeconds analog) reclaims expired
      // rows from files the universal triggers never pick again; the deadline advances whether
      // or not the pick found anything, so an all-live table is not re-scanned every round.
      boolean full = System.currentTimeMillis() >= fullCompactionDeadline;
      if (full) {
        fullCompactionDeadline = System.currentTimeMillis() + fullCompactionIntervalMillis;
      }
      StreamWriteBuilder builder = fresh.newStreamWriteBuilder().withCommitUser(SHAPE_COMMIT_USER);
      try (IOManager shapeIo =
              IOManager.create(
                  Files.createTempDirectory("streamfusion-compactor-lookup").toString());
          StreamTableWrite shapeWrite = builder.newWrite();
          StreamTableCommit shapeCommit = builder.newCommit()) {
        shapeWrite.withIOManager(shapeIo);
        for (int bucket = 0; bucket < bucketCount; bucket++) {
          shapeWrite.compact(BinaryRow.EMPTY_ROW, bucket, full);
        }
        List<CommitMessage> messages = shapeWrite.prepareCommit(true, round);
        boolean empty =
            messages.stream().allMatch(message -> ((CommitMessageImpl) message).isEmpty());
        if (!empty) {
          shapeCommit.commit(round, messages);
          close();
        }
      }
    }

    @Override
    public void close() {
      try {
        if (write != null) {
          write.close();
        }
        if (commit != null) {
          commit.close();
        }
        if (ioManager != null) {
          ioManager.close();
        }
      } catch (Exception e) {
        throw new RuntimeException("closing paimon maintenance session", e);
      } finally {
        write = null;
        commit = null;
        ioManager = null;
        table = null;
      }
    }
  }
}

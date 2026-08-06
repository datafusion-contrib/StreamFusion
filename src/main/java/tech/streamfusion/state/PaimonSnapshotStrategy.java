package tech.streamfusion.state;

import java.io.DataInputStream;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.SnapshotResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Incremental snapshots of a native operator's local Paimon table, mirroring the RocksDB
 * incremental strategy's shared-state contract: a Paimon data file is immutable and uniquely
 * named, so a file already uploaded by a completed checkpoint is referenced with a placeholder
 * handle instead of re-uploaded, and the {@code SharedStateRegistry} resolves ownership on the
 * checkpoint coordinator. Snapshot/manifest/schema documents travel as private state each
 * checkpoint (they are small and pin the exact snapshot); the checkpoint's metadata document
 * carries the Paimon snapshot id for restore.
 *
 * <p>The synchronous phase runs the native barrier commit and receives a hard-linked file listing;
 * the asynchronous phase only moves bytes. Bookkeeping follows {@code
 * RocksIncrementalSnapshotStrategy}: only checkpoints confirmed complete are a reuse base, a file
 * re-uploaded by a later checkpoint drops out of the base (notification-delay race), and
 * complete/abort notifications prune the map.
 *
 * <p>The manifest's first entry is an OPAQUE snapshot token, defined and consumed only by the
 * native store (a single-table store uses its Paimon snapshot id; a multi-table operator can pack
 * several). This layer stores it in the meta document and hands it back on restore; the one thing
 * it interprets is emptiness — an empty token means no state was ever committed.
 */
final class PaimonSnapshotStrategy
    implements SnapshotStrategy<KeyedStateHandle, PaimonSnapshotStrategy.PaimonSnapshotResources> {

  private static final Logger LOG = LoggerFactory.getLogger(PaimonSnapshotStrategy.class);

  /** Version tag of the checkpoint metadata document; v2 replaced the snapshot-id long with an
   * opaque token string (v1 documents read back as the id's decimal string). */
  private static final int META_VERSION = 2;

  private static final int COPY_BUFFER_BYTES = 64 * 1024;

  private final UUID backendUID;
  private final KeyGroupRange keyGroupRange;
  private final File checkpointLinkRoot;
  private final File tableDirectory;
  /** Maintains the state tables synchronously at each barrier, between the data commit and the
   * manifest capture; never null — the backend fails closed at creation without a compactor. */
  private final StateTableCompactor compactor;

  /** Kicked after each barrier's minimal round. */
  private final PaimonTableShaping shaping;

  /** One compactor per table at a time (see {@link PaimonTableShaping}). */
  private final Object compactionMutex = new Object();

  /** Long-lived maintenance sessions per table directory, guarded by the mutex: a session holds
   * the table and writer across barriers and folds the native store's commits in incrementally,
   * so the full manifest chain is scanned once per session, not once per barrier. */
  private final Map<String, StateTableCompactor.Session> sessions = new HashMap<>();

  /** Compaction commit identifier: monotonic across restarts (millis-seeded — Paimon dedupes
   * re-committed identifiers per commit user, so barrier checkpoint ids, which restart small,
   * cannot be reused for the compactor's own commits). */
  private long compactRound = System.currentTimeMillis();

  private PaimonNativeState nativeState;

  /** The operator's idle-state retention millis (0 = off), set at native-state registration —
   * before any maintenance session opens — so every session carries the retention as dynamic
   * record-level-expire options. */
  private long stateTtlMillis;

  /** Shared files uploaded per checkpoint; a reuse base once the checkpoint completes. */
  private final SortedMap<Long, Collection<HandleAndLocalPath>> uploadedFiles = new TreeMap<>();

  private long lastCompletedCheckpointId = -1;

  /** The in-flight snapshot's options and stream factory, stashed by the backend just before the
   * sync phase (task thread, so strictly ordered): the sync phase decides file reuse — which
   * files become placeholders and which the async phase uploads — so it can hard-link exactly the
   * files the upload will read; the async phase consumes that one decision, so the two can never
   * drift. */
  private CheckpointOptions currentOptions;

  private CheckpointStreamFactory currentStreamFactory;

  PaimonSnapshotStrategy(
      UUID backendUID,
      KeyGroupRange keyGroupRange,
      File checkpointLinkRoot,
      File tableDirectory,
      StateTableCompactor compactor) {
    this.backendUID = backendUID;
    this.keyGroupRange = keyGroupRange;
    this.checkpointLinkRoot = checkpointLinkRoot;
    this.tableDirectory = tableDirectory;
    this.compactor = compactor;
    this.shaping = new PaimonTableShaping(this::shapeTables);
  }

  void close() {
    shaping.close();
    synchronized (compactionMutex) {
      for (StateTableCompactor.Session session : sessions.values()) {
        session.close();
      }
      sessions.clear();
    }
  }

  void registerNativeState(PaimonNativeState nativeState, long stateTtlMillis) {
    this.nativeState = nativeState;
    this.stateTtlMillis = stateTtlMillis;
  }

  boolean hasNativeState() {
    return nativeState != null;
  }

  /**
   * Commits the native write buffer to the local table without creating Flink checkpoint state.
   * Maintenance makes the new run readable through the deletion-vector raw-scan path; a second
   * native call re-pins that maintained snapshot. A later barrier uploads these immutable files.
   */
  void flushForMemoryPressure() throws Exception {
    String[] manifest = nativeState.checkpoint();
    if (!manifest[0].isEmpty()) {
      compactTables();
      nativeState.checkpoint();
      shaping.kick();
    }
  }

  /** Seeds the reuse base from a restored checkpoint (single-handle, claim-style restore). */
  void seedRestored(long checkpointId, List<HandleAndLocalPath> sharedState) {
    synchronized (uploadedFiles) {
      uploadedFiles.put(checkpointId, new ArrayList<>(sharedState));
      lastCompletedCheckpointId = checkpointId;
    }
  }

  void notifyCheckpointComplete(long completedCheckpointId) {
    synchronized (uploadedFiles) {
      // Ignore ids we never uploaded for (savepoints), or the reuse base degrades to full.
      if (completedCheckpointId > lastCompletedCheckpointId
          && uploadedFiles.containsKey(completedCheckpointId)) {
        uploadedFiles.keySet().removeIf(id -> id < completedCheckpointId);
        lastCompletedCheckpointId = completedCheckpointId;
      }
    }
  }

  void notifyCheckpointAborted(long abortedCheckpointId) {
    synchronized (uploadedFiles) {
      uploadedFiles.keySet().remove(abortedCheckpointId);
    }
  }

  /** Stashes the snapshot's options; called by the backend just before running the strategy. */
  void beforeSnapshot(CheckpointOptions options, CheckpointStreamFactory streamFactory) {
    this.currentOptions = options;
    this.currentStreamFactory = streamFactory;
  }

  @Override
  public PaimonSnapshotResources syncPrepareResources(long checkpointId) throws Exception {
    long profileStart = System.nanoTime();
    long profileCompactNs = 0;
    File linkDir = new File(checkpointLinkRoot, "chk-" + checkpointId);
    String[] manifest = nativeState.checkpoint();
    if (!manifest[0].isEmpty()) {
      // Maintenance runs synchronously between the data commit and the manifest capture —
      // Paimon's own lookup-wait model. The barrier's sorted runs are compacted away (with
      // deletion vectors maintained) before any file is listed, so the checkpoint carries no
      // level-0 files and the next interval's reads take the raw path. The second native
      // checkpoint call commits nothing (the write buffer already drained); it re-pins the
      // maintenance snapshot, lists its files, and lets local GC drop the superseded runs. A
      // maintenance failure must fail the snapshot: on a deletion-vector table, reads over an
      // uncompacted run would bypass the vectors and resurrect masked rows.
      long compactStart = System.nanoTime();
      compactTables();
      profileCompactNs = System.nanoTime() - compactStart;
      manifest = nativeState.checkpoint();
      // The discretionary merges (run counts, space amplification) happen off-thread; deletion
      // vectors keep reads correct however far shaping lags.
      shaping.kick();
    }
    if (System.getenv("SF_STATE_PROFILE") != null) {
      System.err.printf(
          "SFPROF barrier chk=%d sync_ms=%d compact_ms=%d%n",
          checkpointId,
          (System.nanoTime() - profileStart) / 1_000_000,
          profileCompactNs / 1_000_000);
    }
    String snapshotToken = manifest[0];
    List<String> dataFiles = new ArrayList<>();
    List<String> metaFiles = new ArrayList<>();
    for (int i = 1; i < manifest.length; i++) {
      String entry = manifest[i];
      if (entry.startsWith("d:")) {
        dataFiles.add(entry.substring(2));
      } else if (entry.startsWith("m:")) {
        metaFiles.add(entry.substring(2));
      } else {
        throw new IOException("unrecognized checkpoint manifest entry: " + entry);
      }
    }
    Map<String, StreamStateHandle> confirmedBase;
    synchronized (uploadedFiles) {
      confirmedBase = confirmedBase(uploadedFiles, lastCompletedCheckpointId);
    }
    // Decide reuse now and hard-link exactly the files the async upload will read, so they
    // survive the compaction and GC of later barriers while the upload runs. Only
    // FORWARD_BACKWARD sharing may reuse the confirmed base as placeholders (never read again —
    // linking them re-linked the whole table each barrier); every other mode (savepoints,
    // NO_SHARING, rescale-bound FORWARD) uploads everything. Meta documents re-upload every
    // checkpoint regardless.
    boolean mayReuse =
        currentOptions != null
            && currentOptions.getCheckpointType().getSharingFilesStrategy()
                == SnapshotType.SharingFilesStrategy.FORWARD_BACKWARD;
    Map<String, StreamStateHandle> reusable = new HashMap<>();
    if (!snapshotToken.isEmpty()) {
      for (String rel : dataFiles) {
        StreamStateHandle confirmed = mayReuse ? confirmedBase.get(rel) : null;
        if (confirmed != null
            && currentStreamFactory != null
            && currentStreamFactory.couldReuseStateHandle(confirmed)) {
          reusable.put(rel, confirmed);
          continue;
        }
        link(rel, linkDir);
      }
      for (String rel : metaFiles) {
        link(rel, linkDir);
      }
    }
    return new PaimonSnapshotResources(snapshotToken, dataFiles, metaFiles, linkDir, reusable);
  }

  private void compactTables() throws Exception {
    synchronized (compactionMutex) {
      for (File table : discoverTables(tableDirectory)) {
        session(table).compact(++compactRound);
      }
    }
  }

  /** One shaping round over every table; called by the shaping thread. */
  private void shapeTables(long round) throws Exception {
    synchronized (compactionMutex) {
      for (File table : discoverTables(tableDirectory)) {
        session(table).shape(round);
      }
    }
  }

  private StateTableCompactor.Session session(File table) throws Exception {
    String dir = table.getAbsolutePath();
    StateTableCompactor.Session session = sessions.get(dir);
    if (session == null) {
      session = compactor.open(dir, recordLevelExpireOptions(stateTtlMillis));
      sessions.put(dir, session);
    }
    return session;
  }

  /**
   * The dynamic Paimon options letting a maintenance session physically drop rows past the
   * operator's retention during its compaction rewrites — the analog of RocksDB's compaction
   * filter. The read path already enforces expiry logically off the trailing {@code ts}
   * epoch-millis column, so this only reclaims space for rows never read again; correctness
   * therefore demands physical drops happen strictly AFTER logical expiry ({@code now >= ts +
   * ttl}), never before.
   *
   * <p>Paimon's {@code RecordLevelExpire} truncates everything to whole seconds: it compares
   * {@code currentTimeMillis()/1000} against {@code ts/1000 + expireSec}, keeping a row iff
   * {@code nowSec <= tsSec + expireSec}. Both floor divisions lose up to ~1s each, so the expiry
   * seconds are padded — {@code ceil(ttl/1000) + 1} — to guarantee a logically live row can never
   * be dropped, at the cost of holding a dead row for up to ~2 extra seconds. Do NOT "simplify"
   * the ceil or the +1 away: an unpadded floor could physically drop a row the read path still
   * serves. The same pad absorbs any sub-second skew between the task's stamping clock and the
   * compactor's clock (they share a JVM).
   */
  static Map<String, String> recordLevelExpireOptions(long stateTtlMillis) {
    if (stateTtlMillis <= 0) {
      return Collections.emptyMap();
    }
    long paddedExpireSeconds = (stateTtlMillis + 999) / 1000 + 1;
    Map<String, String> options = new HashMap<>();
    options.put("record-level.expire-time", paddedExpireSeconds + "s");
    options.put("record-level.time-field", "ts");
    return options;
  }

  /**
   * Restore-time maintenance, called once after the operator's native state opened: a rescale
   * restore rewrites rows at level 0, which deletion-vector reads skip, so the tables must be
   * compacted — and the native store re-pinned onto the maintenance snapshot — before the first
   * record is processed. An adoption restore has no level-0 files, so this is a cheap no-op scan.
   */
  void maintainAfterRestore() throws Exception {
    compactTables();
    nativeState.checkpoint();
  }

  private void link(String rel, File linkDir) throws IOException {
    Path to = new File(linkDir, rel).toPath();
    Files.createDirectories(to.getParent());
    Files.createLink(to, new File(tableDirectory, rel).toPath());
  }

  /**
   * The Paimon tables under an operator's state directory. The native side owns the layout — a
   * single table rooted at the directory itself, or one table per immediate child for a
   * multi-state operator (the join's two sides) — and the presence of a {@code schema/} dir is
   * the ground truth, so the compactor plugin interface stays a single-table contract.
   */
  static List<File> discoverTables(File tableDirectory) {
    if (new File(tableDirectory, "schema").isDirectory()) {
      return Collections.singletonList(tableDirectory);
    }
    File[] children = tableDirectory.listFiles(File::isDirectory);
    if (children == null) {
      return Collections.emptyList();
    }
    List<File> tables = new ArrayList<>();
    Arrays.sort(children);
    for (File child : children) {
      if (new File(child, "schema").isDirectory()) {
        tables.add(child);
      }
    }
    return tables;
  }

  /**
   * The last completed checkpoint's shared files, minus any file a later (unconfirmed) checkpoint
   * re-uploaded — that re-upload means the JM may already have discarded the confirmed copy when
   * it subsumes (the notification-delay race the RocksDB strategy documents).
   */
  private static Map<String, StreamStateHandle> confirmedBase(
      SortedMap<Long, Collection<HandleAndLocalPath>> uploaded, long lastCompleted) {
    Collection<HandleAndLocalPath> confirmed = uploaded.get(lastCompleted);
    if (confirmed == null) {
      return Collections.emptyMap();
    }
    Map<String, StreamStateHandle> base = new HashMap<>();
    for (HandleAndLocalPath handle : confirmed) {
      base.put(handle.getLocalPath(), handle.getHandle());
    }
    for (Map.Entry<Long, Collection<HandleAndLocalPath>> later :
        uploaded.tailMap(lastCompleted + 1).entrySet()) {
      for (HandleAndLocalPath handle : later.getValue()) {
        if (!(handle.getHandle() instanceof PlaceholderStreamStateHandle)) {
          base.remove(handle.getLocalPath());
        }
      }
    }
    return base;
  }

  @Override
  public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(
      PaimonSnapshotResources resources,
      long checkpointId,
      long timestamp,
      CheckpointStreamFactory streamFactory,
      CheckpointOptions checkpointOptions) {

    if (resources.snapshotToken.isEmpty()) {
      return registry -> SnapshotResult.empty();
    }

    final SnapshotType.SharingFilesStrategy sharing =
        checkpointOptions.getCheckpointType().getSharingFilesStrategy();
    final CheckpointedStateScope scope =
        sharing == SnapshotType.SharingFilesStrategy.NO_SHARING
            ? CheckpointedStateScope.EXCLUSIVE
            : CheckpointedStateScope.SHARED;
    // The sync phase already decided reuse per file (and linked everything else); consuming that
    // one decision here keeps the linked set and the upload set identical by construction.
    final Map<String, StreamStateHandle> reuseBase = resources.reusable;

    return snapshotCloseableRegistry -> {
      List<HandleAndLocalPath> sharedState = new ArrayList<>();
      List<HandleAndLocalPath> privateState = new ArrayList<>();
      List<StreamStateHandle> reused = new ArrayList<>();
      List<StreamStateHandle> uploadedNow = new ArrayList<>();
      boolean completed = false;
      try {
        long checkpointedSize = 0;
        for (String relPath : resources.dataFiles) {
          StreamStateHandle confirmed = reuseBase.get(relPath);
          if (confirmed != null) {
            StreamStateHandle placeholder =
                new PlaceholderStreamStateHandle(
                    confirmed.getStreamStateHandleID(), confirmed.getStateSize(), false);
            sharedState.add(HandleAndLocalPath.of(placeholder, relPath));
            reused.add(confirmed);
          } else {
            StreamStateHandle uploaded =
                uploadFile(resources.linkDir, relPath, streamFactory, scope, snapshotCloseableRegistry);
            uploadedNow.add(uploaded);
            sharedState.add(HandleAndLocalPath.of(uploaded, relPath));
            checkpointedSize += uploaded.getStateSize();
          }
        }
        for (String relPath : resources.metaFiles) {
          StreamStateHandle uploaded =
              uploadFile(resources.linkDir, relPath, streamFactory, scope, snapshotCloseableRegistry);
          uploadedNow.add(uploaded);
          privateState.add(HandleAndLocalPath.of(uploaded, relPath));
          checkpointedSize += uploaded.getStateSize();
        }
        StreamStateHandle metaHandle =
            writeMetaDocument(resources.snapshotToken, streamFactory, snapshotCloseableRegistry);
        uploadedNow.add(metaHandle);
        checkpointedSize += metaHandle.getStateSize();

        if (sharing != SnapshotType.SharingFilesStrategy.NO_SHARING) {
          synchronized (uploadedFiles) {
            uploadedFiles.put(checkpointId, Collections.unmodifiableList(sharedState));
          }
        }
        IncrementalRemoteKeyedStateHandle handle =
            new IncrementalRemoteKeyedStateHandle(
                backendUID,
                keyGroupRange,
                checkpointId,
                sharedState,
                privateState,
                metaHandle,
                checkpointedSize);
        completed = true;
        return SnapshotResult.of(handle);
      } finally {
        if (completed) {
          streamFactory.reusePreviousStateHandle(reused);
        } else {
          for (StreamStateHandle handle : uploadedNow) {
            try {
              handle.discardState();
            } catch (Exception cleanupFailure) {
              // Best effort: the checkpoint is failing anyway.
            }
          }
        }
      }
    };
  }

  private static StreamStateHandle uploadFile(
      File linkDir,
      String relPath,
      CheckpointStreamFactory streamFactory,
      CheckpointedStateScope scope,
      CloseableRegistry closeableRegistry)
      throws IOException {
    File source = new File(linkDir, relPath);
    CheckpointStateOutputStream out = streamFactory.createCheckpointStateOutputStream(scope);
    closeableRegistry.registerCloseable(out);
    try (InputStream in = Files.newInputStream(source.toPath())) {
      byte[] buffer = new byte[COPY_BUFFER_BYTES];
      int read;
      while ((read = in.read(buffer)) >= 0) {
        ((OutputStream) out).write(buffer, 0, read);
      }
      StreamStateHandle handle = out.closeAndGetHandle();
      closeableRegistry.unregisterCloseable(out);
      return handle == null
          ? new org.apache.flink.runtime.state.memory.ByteStreamStateHandle(
              UUID.randomUUID().toString(), new byte[0])
          : handle;
    } catch (IOException e) {
      if (closeableRegistry.unregisterCloseable(out)) {
        out.close();
      }
      throw e;
    }
  }

  private static StreamStateHandle writeMetaDocument(
      String snapshotToken,
      CheckpointStreamFactory streamFactory,
      CloseableRegistry closeableRegistry)
      throws IOException {
    CheckpointStateOutputStream out =
        streamFactory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE);
    closeableRegistry.registerCloseable(out);
    try {
      DataOutputStream data = new DataOutputStream(out);
      data.writeInt(META_VERSION);
      data.writeUTF(snapshotToken);
      data.flush();
      StreamStateHandle handle = out.closeAndGetHandle();
      closeableRegistry.unregisterCloseable(out);
      return handle;
    } catch (IOException e) {
      if (closeableRegistry.unregisterCloseable(out)) {
        out.close();
      }
      throw e;
    }
  }

  /** Reads the snapshot token back out of a checkpoint's metadata document. */
  static String readMetaDocument(StreamStateHandle metaHandle) throws IOException {
    try (InputStream in = metaHandle.openInputStream()) {
      DataInputStream data = new DataInputStream(in);
      int version = data.readInt();
      if (version == 1) {
        // v1 carried the single-table Paimon snapshot id as a long; its token form is the
        // decimal string, so pre-token checkpoints stay restorable.
        return Long.toString(data.readLong());
      }
      if (version != META_VERSION) {
        throw new IOException("unknown paimon state metadata version " + version);
      }
      return data.readUTF();
    }
  }

  static final class PaimonSnapshotResources implements SnapshotResources {
    final String snapshotToken;
    final List<String> dataFiles;
    final List<String> metaFiles;
    final File linkDir;
    /** rel path -> confirmed handle for files the async phase re-references as placeholders. */
    final Map<String, StreamStateHandle> reusable;

    PaimonSnapshotResources(
        String snapshotToken,
        List<String> dataFiles,
        List<String> metaFiles,
        File linkDir,
        Map<String, StreamStateHandle> reusable) {
      this.snapshotToken = snapshotToken;
      this.dataFiles = dataFiles;
      this.metaFiles = metaFiles;
      this.linkDir = linkDir;
      this.reusable = reusable;
    }

    @Override
    public void release() {
      // The uploads are done (or cancelled); the hard links have served their purpose.
      try {
        FileUtils.deleteDirectory(linkDir);
      } catch (IOException e) {
        // Leak a link dir rather than fail the checkpoint path; the task dir is cleaned on exit.
      }
    }
  }
}

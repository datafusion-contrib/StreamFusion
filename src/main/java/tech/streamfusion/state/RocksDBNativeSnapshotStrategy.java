package tech.streamfusion.state;

import java.io.DataInputStream;
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
 * Incremental snapshots of a native operator's local RocksDB table, mirroring the RocksDB
 * incremental strategy's shared-state contract: a RocksDB data file is immutable and uniquely
 * named, so a file already uploaded by a completed checkpoint is referenced with a placeholder
 * handle instead of re-uploaded, and the {@code SharedStateRegistry} resolves ownership on the
 * checkpoint coordinator. Snapshot/manifest/schema documents travel as private state each
 * checkpoint (they are small and pin the exact snapshot); the checkpoint's metadata document
 * carries the RocksDB snapshot id for restore.
 *
 * <p>The synchronous phase runs the native barrier commit and has RocksDB create its native
 * hard-linked checkpoint directory; the asynchronous phase only moves bytes. Bookkeeping follows {@code
 * RocksIncrementalSnapshotStrategy}: only checkpoints confirmed complete are a reuse base, a file
 * re-uploaded by a later checkpoint drops out of the base (notification-delay race), and
 * complete/abort notifications prune the map.
 *
 * <p>The manifest's first entry is an OPAQUE snapshot token, defined and consumed only by the
 * native store (a single-table store uses its RocksDB snapshot id; a multi-table operator can pack
 * several). This layer stores it in the meta document and hands it back on restore; the one thing
 * it interprets is emptiness — an empty token means no state was ever committed.
 */
final class RocksDBNativeSnapshotStrategy
    implements SnapshotStrategy<KeyedStateHandle, RocksDBNativeSnapshotStrategy.RocksDBSnapshotResources> {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBNativeSnapshotStrategy.class);

  /** A backend discriminator followed by the metadata format version. */
  private static final int META_MAGIC = 0x5346524b; // SFRK
  private static final int META_VERSION = 1;

  private static final int COPY_BUFFER_BYTES = 64 * 1024;

  private final UUID backendUID;
  private final KeyGroupRange keyGroupRange;
  private final File checkpointLinkRoot;
  private final boolean incrementalCheckpoints;
  private RocksDBNativeState nativeState;

  /** Shared files uploaded per checkpoint; a reuse base once the checkpoint completes. */
  private final SortedMap<Long, Collection<HandleAndLocalPath>> uploadedFiles = new TreeMap<>();

  private long lastCompletedCheckpointId = -1;

  /** The in-flight snapshot's options and stream factory, stashed by the backend just before the
   * sync phase (task thread, so strictly ordered): the sync phase decides file reuse — which
   * files become placeholders and which the async phase uploads — from the already-pinned native
   * checkpoint directory. */
  private CheckpointOptions currentOptions;

  private CheckpointStreamFactory currentStreamFactory;

  RocksDBNativeSnapshotStrategy(
      UUID backendUID,
      KeyGroupRange keyGroupRange,
      File checkpointLinkRoot,
      boolean incrementalCheckpoints) {
    this.backendUID = backendUID;
    this.keyGroupRange = keyGroupRange;
    this.checkpointLinkRoot = checkpointLinkRoot;
    this.incrementalCheckpoints = incrementalCheckpoints;
  }

  void close() {}

  void registerNativeState(RocksDBNativeState nativeState, long stateTtlMillis) {
    this.nativeState = nativeState;
  }

  boolean hasNativeState() {
    return nativeState != null;
  }

  RocksDBNativeState nativeState() {
    if (nativeState == null) {
      throw new IllegalStateException("no native RocksDB state is registered");
    }
    return nativeState;
  }

  /**
   * Commits the native write buffer to the local table without creating Flink checkpoint state.
   * A later barrier uses RocksDB's native checkpoint API to pin and upload the immutable files.
   */
  void flushForMemoryPressure() throws Exception {
    nativeState.checkpoint("");
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
  public RocksDBSnapshotResources syncPrepareResources(long checkpointId) throws Exception {
    long profileStart = System.nanoTime();
    File linkDir = new File(checkpointLinkRoot, "chk-" + checkpointId);
    if (linkDir.exists()) {
      FileUtils.deleteDirectory(linkDir);
    }
    String[] manifest = nativeState.checkpoint(linkDir.getAbsolutePath());
    if (System.getenv("SF_STATE_PROFILE") != null) {
      System.err.printf(
          "SFPROF rocksdb barrier chk=%d sync_ms=%d%n",
          checkpointId,
          (System.nanoTime() - profileStart) / 1_000_000);
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
    // Decide reuse against the already-pinned native checkpoint directory. Only FORWARD_BACKWARD
    // sharing may reuse the confirmed base as placeholders; every other mode (savepoints,
    // NO_SHARING, rescale-bound FORWARD) uploads everything. Meta documents re-upload every
    // checkpoint regardless.
    boolean mayReuse =
        incrementalCheckpoints
            && currentOptions != null
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
      }
    }
    return new RocksDBSnapshotResources(snapshotToken, dataFiles, metaFiles, linkDir, reusable);
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
      RocksDBSnapshotResources resources,
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
      data.writeInt(META_MAGIC);
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
      int magic = data.readInt();
      if (magic != META_MAGIC) {
        throw new IOException("not StreamFusion RocksDB state metadata");
      }
      int version = data.readInt();
      if (version != META_VERSION) {
        throw new IOException("unknown StreamFusion RocksDB state metadata version " + version);
      }
      return data.readUTF();
    }
  }

  static boolean isNativeMeta(StreamStateHandle metaHandle) {
    try {
      readMetaDocument(metaHandle);
      return true;
    } catch (IOException ignored) {
      return false;
    }
  }

  static final class RocksDBSnapshotResources implements SnapshotResources {
    final String snapshotToken;
    final List<String> dataFiles;
    final List<String> metaFiles;
    final File linkDir;
    /** rel path -> confirmed handle for files the async phase re-references as placeholders. */
    final Map<String, StreamStateHandle> reusable;

    RocksDBSnapshotResources(
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

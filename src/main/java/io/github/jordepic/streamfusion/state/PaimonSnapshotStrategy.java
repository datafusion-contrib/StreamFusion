package io.github.jordepic.streamfusion.state;

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

import javax.annotation.Nullable;

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
 */
final class PaimonSnapshotStrategy
    implements SnapshotStrategy<KeyedStateHandle, PaimonSnapshotStrategy.PaimonSnapshotResources> {

  private static final Logger LOG = LoggerFactory.getLogger(PaimonSnapshotStrategy.class);

  /** Version tag of the checkpoint metadata document. */
  private static final int META_VERSION = 1;

  private static final int COPY_BUFFER_BYTES = 64 * 1024;

  private final UUID backendUID;
  private final KeyGroupRange keyGroupRange;
  private final File checkpointLinkRoot;
  private final File tableDirectory;
  @Nullable private final StateTableCompactor compactor;

  private PaimonNativeState nativeState;

  /** Shared files uploaded per checkpoint; a reuse base once the checkpoint completes. */
  private final SortedMap<Long, Collection<HandleAndLocalPath>> uploadedFiles = new TreeMap<>();

  private long lastCompletedCheckpointId = -1;

  PaimonSnapshotStrategy(
      UUID backendUID,
      KeyGroupRange keyGroupRange,
      File checkpointLinkRoot,
      File tableDirectory,
      @Nullable StateTableCompactor compactor) {
    this.backendUID = backendUID;
    this.keyGroupRange = keyGroupRange;
    this.checkpointLinkRoot = checkpointLinkRoot;
    this.tableDirectory = tableDirectory;
    this.compactor = compactor;
  }

  void registerNativeState(PaimonNativeState nativeState) {
    this.nativeState = nativeState;
  }

  boolean hasNativeState() {
    return nativeState != null;
  }

  boolean hasExternalCompactor() {
    return compactor != null;
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

  @Override
  public PaimonSnapshotResources syncPrepareResources(long checkpointId) throws Exception {
    if (compactor != null) {
      // Maintenance commits its own snapshot directly beneath the checkpoint's data commit. A
      // failure loses maintenance, not the checkpoint.
      try {
        compactor.compact(tableDirectory.getAbsolutePath(), checkpointId);
      } catch (Exception e) {
        LOG.warn("state-table compaction failed; continuing the checkpoint without maintenance", e);
      }
    }
    File linkDir = new File(checkpointLinkRoot, "chk-" + checkpointId);
    String[] manifest = nativeState.checkpoint(linkDir.getAbsolutePath());
    long snapshotId = Long.parseLong(manifest[0]);
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
    return new PaimonSnapshotResources(snapshotId, dataFiles, metaFiles, linkDir, confirmedBase);
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

    if (resources.snapshotId < 0) {
      return registry -> SnapshotResult.empty();
    }

    final SnapshotType.SharingFilesStrategy sharing =
        checkpointOptions.getCheckpointType().getSharingFilesStrategy();
    final CheckpointedStateScope scope =
        sharing == SnapshotType.SharingFilesStrategy.NO_SHARING
            ? CheckpointedStateScope.EXCLUSIVE
            : CheckpointedStateScope.SHARED;
    final Map<String, StreamStateHandle> reuseBase =
        sharing == CheckpointType.SharingFilesStrategy.FORWARD_BACKWARD
            ? resources.confirmedBase
            : Collections.emptyMap();

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
          if (confirmed != null && streamFactory.couldReuseStateHandle(confirmed)) {
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
            writeMetaDocument(resources.snapshotId, streamFactory, snapshotCloseableRegistry);
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
      long paimonSnapshotId,
      CheckpointStreamFactory streamFactory,
      CloseableRegistry closeableRegistry)
      throws IOException {
    CheckpointStateOutputStream out =
        streamFactory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE);
    closeableRegistry.registerCloseable(out);
    try {
      DataOutputStream data = new DataOutputStream(out);
      data.writeInt(META_VERSION);
      data.writeLong(paimonSnapshotId);
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

  /** Reads the Paimon snapshot id back out of a checkpoint's metadata document. */
  static long readMetaDocument(StreamStateHandle metaHandle) throws IOException {
    try (InputStream in = metaHandle.openInputStream()) {
      java.io.DataInputStream data = new java.io.DataInputStream(in);
      int version = data.readInt();
      if (version != META_VERSION) {
        throw new IOException("unknown paimon state metadata version " + version);
      }
      return data.readLong();
    }
  }

  static final class PaimonSnapshotResources implements SnapshotResources {
    final long snapshotId;
    final List<String> dataFiles;
    final List<String> metaFiles;
    final File linkDir;
    final Map<String, StreamStateHandle> confirmedBase;

    PaimonSnapshotResources(
        long snapshotId,
        List<String> dataFiles,
        List<String> metaFiles,
        File linkDir,
        Map<String, StreamStateHandle> confirmedBase) {
      this.snapshotId = snapshotId;
      this.dataFiles = dataFiles;
      this.metaFiles = metaFiles;
      this.linkDir = linkDir;
      this.confirmedBase = confirmedBase;
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

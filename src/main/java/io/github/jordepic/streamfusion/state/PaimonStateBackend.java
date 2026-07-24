package io.github.jordepic.streamfusion.state;

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * A state backend for jobs with native StreamFusion operators: JVM-side state (fallback operators,
 * timers) lives in the wrapped hashmap backend exactly as it would without us, while native
 * operators keep their state in local Paimon tables and checkpoint them incrementally — a Paimon
 * snapshot is a manifest-pinned set of immutable, uniquely named files, so each checkpoint uploads
 * only new files and references the rest, under the same {@code SharedStateRegistry} contract the
 * RocksDB backend uses.
 *
 * <p>Selected with {@code state.backend.type:
 * io.github.jordepic.streamfusion.state.PaimonStateBackendFactory}.
 */
public class PaimonStateBackend implements StateBackend {

  private static final long serialVersionUID = 1L;

  private final HashMapStateBackend delegate = new HashMapStateBackend();

  @Override
  public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
      KeyedStateBackendParameters<K> parameters) throws Exception {
    List<IncrementalRemoteKeyedStateHandle> paimonHandles = new ArrayList<>();
    List<KeyedStateHandle> delegateHandles = new ArrayList<>();
    for (KeyedStateHandle handle : parameters.getStateHandles()) {
      if (handle instanceof IncrementalRemoteKeyedStateHandle) {
        paimonHandles.add((IncrementalRemoteKeyedStateHandle) handle);
      } else {
        delegateHandles.add(handle);
      }
    }

    CheckpointableKeyedStateBackend<K> inner =
        delegate.createKeyedStateBackend(
            new KeyedStateBackendParametersImpl<>(parameters).setStateHandles(delegateHandles));

    File workingDirectory = workingDirectory(parameters);
    List<PaimonRestoredSource> sources = new ArrayList<>();
    for (int i = 0; i < paimonHandles.size(); i++) {
      sources.add(materialize(paimonHandles.get(i), new File(workingDirectory, "restore-" + i)));
    }

    // A single-handle restore keeps the restored backend identity and reuse base (the files are
    // already on the checkpoint storage); a rescale builds a new table, so its first checkpoint
    // uploads fresh files under a fresh identity — the same policy as the RocksDB backend.
    UUID backendUID =
        paimonHandles.size() == 1
            ? paimonHandles.get(0).getBackendIdentifier()
            : UUID.randomUUID();
    PaimonSnapshotStrategy strategy =
        new PaimonSnapshotStrategy(
            backendUID, parameters.getKeyGroupRange(), new File(workingDirectory, "checkpoints"));
    if (paimonHandles.size() == 1) {
      IncrementalRemoteKeyedStateHandle restored = paimonHandles.get(0);
      strategy.seedRestored(restored.getCheckpointId(), restored.getSharedState());
    }
    return new PaimonKeyedStateBackend<>(inner, strategy, workingDirectory, sources);
  }

  private static File workingDirectory(KeyedStateBackendParameters<?> parameters) {
    String operator = parameters.getOperatorIdentifier().replaceAll("[^A-Za-z0-9_-]", "_");
    int subtask = parameters.getEnv().getTaskInfo().getIndexOfThisSubtask();
    int attempt = parameters.getEnv().getTaskInfo().getAttemptNumber();
    File root = parameters.getEnv().getTaskManagerInfo().getTmpWorkingDirectory();
    return new File(
        root,
        "paimon-state/"
            + parameters.getJobID()
            + "/"
            + operator
            + "_"
            + subtask
            + "_"
            + attempt);
  }

  /** Downloads one restored checkpoint's files to local disk at their checkpoint-local paths. */
  private static PaimonRestoredSource materialize(
      IncrementalRemoteKeyedStateHandle handle, File directory) throws IOException {
    long paimonSnapshotId = PaimonSnapshotStrategy.readMetaDocument(handle.getMetaDataStateHandle());
    List<HandleAndLocalPath> files = new ArrayList<>(handle.getSharedState());
    files.addAll(handle.getPrivateState());
    for (HandleAndLocalPath file : files) {
      File target = new File(directory, file.getLocalPath());
      Files.createDirectories(target.getParentFile().toPath());
      StreamStateHandle source = file.getHandle();
      try (InputStream in = source.openInputStream();
          OutputStream out = Files.newOutputStream(target.toPath())) {
        byte[] buffer = new byte[64 * 1024];
        int read;
        while ((read = in.read(buffer)) >= 0) {
          out.write(buffer, 0, read);
        }
      }
    }
    return new PaimonRestoredSource(directory.getAbsolutePath(), paimonSnapshotId);
  }

  @Override
  public OperatorStateBackend createOperatorStateBackend(OperatorStateBackendParameters parameters)
      throws Exception {
    return delegate.createOperatorStateBackend(parameters);
  }

  @Override
  public boolean supportsNoClaimRestoreMode() {
    // NO_CLAIM makes the first checkpoint's sharing strategy FORWARD; the snapshot strategy then
    // uploads every file instead of referencing the restored ones.
    return true;
  }

  @Override
  public boolean supportsSavepointFormat(SavepointFormatType formatType) {
    // Native operators cannot express their state in the canonical unified format.
    return formatType == SavepointFormatType.NATIVE;
  }
}

package tech.streamfusion.state;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.state.rocksdb.RocksDBOptions;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Flink state backend that gives native StreamFusion operators a Rust-owned RocksDB instance and
 * delegates ordinary JVM keyed state, timers, and operator state to Flink's RocksDB backend.
 */
public final class RocksDBNativeStateBackend implements StateBackend {

  private static final long serialVersionUID = 1L;

  private final EmbeddedRocksDBStateBackend delegate;
  private final FlinkRocksDBOptions nativeOptions;
  private final boolean incrementalCheckpoints;
  private final String localDirectories;

  RocksDBNativeStateBackend(ReadableConfig config, ClassLoader classLoader) {
    this.delegate = new EmbeddedRocksDBStateBackend().configure(config, classLoader);
    this.nativeOptions = FlinkRocksDBOptions.from(config);
    this.incrementalCheckpoints = config.get(CheckpointingOptions.INCREMENTAL_CHECKPOINTS);
    this.localDirectories = config.get(RocksDBOptions.LOCAL_DIRECTORIES);
  }

  @Override
  public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
      KeyedStateBackendParameters<K> parameters) throws Exception {
    List<IncrementalRemoteKeyedStateHandle> nativeHandles = new ArrayList<>();
    List<KeyedStateHandle> delegateHandles = new ArrayList<>();
    for (KeyedStateHandle handle : parameters.getStateHandles()) {
      if (handle instanceof IncrementalRemoteKeyedStateHandle
          && RocksDBNativeSnapshotStrategy.isNativeMeta(
              ((IncrementalRemoteKeyedStateHandle) handle).getMetaDataStateHandle())) {
        nativeHandles.add((IncrementalRemoteKeyedStateHandle) handle);
      } else {
        delegateHandles.add(handle);
      }
    }

    CheckpointableKeyedStateBackend<K> inner =
        delegate.createKeyedStateBackend(
            new KeyedStateBackendParametersImpl<>(parameters).setStateHandles(delegateHandles));
    File workingDirectory = workingDirectory(parameters);
    List<RocksDBRestoredSource> sources = new ArrayList<>();
    for (int i = 0; i < nativeHandles.size(); i++) {
      sources.add(materialize(nativeHandles.get(i), new File(workingDirectory, "restore-" + i)));
    }

    boolean alignedRestore =
        nativeHandles.size() == 1
            && nativeHandles.get(0).getKeyGroupRange().equals(parameters.getKeyGroupRange());
    UUID backendUID =
        alignedRestore ? nativeHandles.get(0).getBackendIdentifier() : UUID.randomUUID();
    RocksDBNativeSnapshotStrategy strategy =
        new RocksDBNativeSnapshotStrategy(
            backendUID,
            parameters.getKeyGroupRange(),
            new File(workingDirectory, "checkpoints"),
            new File(workingDirectory, "db"),
            incrementalCheckpoints);
    if (alignedRestore && incrementalCheckpoints) {
      IncrementalRemoteKeyedStateHandle restored = nativeHandles.get(0);
      strategy.seedRestored(restored.getCheckpointId(), restored.getSharedState());
    }
    return new RocksDBNativeKeyedStateBackend<>(
        inner, strategy, workingDirectory, sources, nativeOptions.json());
  }

  private File workingDirectory(KeyedStateBackendParameters<?> parameters) {
    String operator = parameters.getOperatorIdentifier().replaceAll("[^A-Za-z0-9_-]", "_");
    int subtask = parameters.getEnv().getTaskInfo().getIndexOfThisSubtask();
    int attempt = parameters.getEnv().getTaskInfo().getAttemptNumber();
    File base = parameters.getEnv().getTaskManagerInfo().getTmpWorkingDirectory();
    if (localDirectories != null && !localDirectories.isBlank()) {
      String[] configured = localDirectories.split(",|" + java.util.regex.Pattern.quote(File.pathSeparator));
      String selected = configured[Math.floorMod(subtask, configured.length)].trim();
      if (!selected.isEmpty()) {
        base = new File(selected);
      }
    }
    return new File(
        base,
        "streamfusion-rocksdb/"
            + parameters.getJobID()
            + "/"
            + operator
            + "_"
            + subtask
            + "_"
            + attempt);
  }

  private static RocksDBRestoredSource materialize(
      IncrementalRemoteKeyedStateHandle handle, File directory) throws IOException {
    String token =
        RocksDBNativeSnapshotStrategy.readMetaDocument(handle.getMetaDataStateHandle());
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
    return new RocksDBRestoredSource(
        directory.getAbsolutePath(),
        token,
        handle.getKeyGroupRange().getStartKeyGroup(),
        handle.getKeyGroupRange().getEndKeyGroup());
  }

  @Override
  public OperatorStateBackend createOperatorStateBackend(OperatorStateBackendParameters parameters)
      throws Exception {
    return delegate.createOperatorStateBackend(parameters);
  }

  @Override
  public boolean useManagedMemory() {
    return delegate.useManagedMemory();
  }

  @Override
  public boolean supportsNoClaimRestoreMode() {
    return true;
  }

  @Override
  public boolean supportsSavepointFormat(SavepointFormatType formatType) {
    return formatType == SavepointFormatType.NATIVE;
  }
}

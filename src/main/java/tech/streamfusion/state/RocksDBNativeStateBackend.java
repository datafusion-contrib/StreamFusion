package tech.streamfusion.state;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.SharedResources;
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
  private final boolean useManagedMemory;
  private final long fixedPerSlotBytes;
  private final double writeBufferRatio;

  RocksDBNativeStateBackend(ReadableConfig config, ClassLoader classLoader) {
    this.delegate = new EmbeddedRocksDBStateBackend().configure(config, classLoader);
    this.nativeOptions = FlinkRocksDBOptions.from(config);
    this.incrementalCheckpoints = config.get(CheckpointingOptions.INCREMENTAL_CHECKPOINTS);
    this.localDirectories = config.get(RocksDBOptions.LOCAL_DIRECTORIES);
    this.useManagedMemory = config.get(RocksDBOptions.USE_MANAGED_MEMORY);
    this.fixedPerSlotBytes =
        config.getOptional(RocksDBOptions.FIX_PER_SLOT_MEMORY_SIZE)
            .map(MemorySize::getBytes)
            .orElse(0L);
    this.writeBufferRatio = config.get(RocksDBOptions.WRITE_BUFFER_RATIO);
  }

  /**
   * Leases the slot's native RocksDB memory pool, resolved with Flink's own precedence:
   * fixed-per-slot wins, else the slot's managed-memory share (the default), else fixed-per-TM at
   * TM scope; nothing configured (or a zero budget) leaves stores on their per-instance options.
   * The C++ pool lives in StreamFusion's RocksDB library, so it is leased under its own resource
   * id alongside the delegate backend's pool rather than shared with it.
   */
  private OpaqueMemoryResource<NativeRocksSharedResources> leaseSharedResources(
      KeyedStateBackendParameters<?> parameters) throws Exception {
    MemoryManager memoryManager = parameters.getEnv().getMemoryManager();
    double ratio = writeBufferRatio;
    if (fixedPerSlotBytes > 0) {
      return memoryManager.getExternalSharedMemoryResource(
          "streamfusion-rocksdb-slot-memory",
          size -> new NativeRocksSharedResources(size, ratio),
          fixedPerSlotBytes);
    }
    if (useManagedMemory && parameters.getManagedMemoryFraction() > 0) {
      long budget = memoryManager.computeMemorySize(parameters.getManagedMemoryFraction());
      if (budget > 0) {
        return memoryManager.getExternalSharedMemoryResource(
            "streamfusion-rocksdb-slot-memory",
            size -> new NativeRocksSharedResources(size, ratio),
            budget);
      }
    }
    long fixedPerTm =
        parameters
            .getEnv()
            .getTaskManagerInfo()
            .getConfiguration()
            .getOptional(RocksDBOptions.FIX_PER_TM_MEMORY_SIZE)
            .map(MemorySize::getBytes)
            .orElse(0L);
    if (fixedPerTm > 0) {
      SharedResources sharedResources = parameters.getEnv().getSharedResources();
      Object leaseHolder = new Object();
      SharedResources.ResourceAndSize<NativeRocksSharedResources> resource =
          sharedResources.getOrAllocateSharedResource(
              "streamfusion-rocksdb-tm-memory",
              leaseHolder,
              size -> new NativeRocksSharedResources(size, ratio),
              fixedPerTm);
      return new OpaqueMemoryResource<>(
          resource.resourceHandle(),
          resource.size(),
          () ->
              sharedResources.release(
                  "streamfusion-rocksdb-tm-memory", leaseHolder, unused -> {}));
    }
    return null;
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

    KeyedStateBackendParametersImpl<K> delegateParameters =
        new KeyedStateBackendParametersImpl<>(parameters).setStateHandles(delegateHandles);
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
            incrementalCheckpoints);
    if (alignedRestore && incrementalCheckpoints) {
      IncrementalRemoteKeyedStateHandle restored = nativeHandles.get(0);
      strategy.seedRestored(restored.getCheckpointId(), restored.getSharedState());
    }
    RocksDBNativeKeyedStateBackend<K> backend =
        new RocksDBNativeKeyedStateBackend<>(
            () -> delegate.createKeyedStateBackend(delegateParameters),
            parameters.getKeyGroupRange(),
            parameters.getKeySerializer(),
            parameters.getNumberOfKeyGroups(),
            strategy,
            workingDirectory,
            sources,
            nativeOptions.json(),
            leaseSharedResources(parameters));
    if (!delegateHandles.isEmpty()) {
      try {
        backend.materializeDelegate();
      } catch (Exception failure) {
        backend.dispose();
        throw failure;
      }
    }
    return backend;
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
    return formatType == SavepointFormatType.NATIVE
        || formatType == SavepointFormatType.CANONICAL;
  }
}

package tech.streamfusion.state;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.memory.SharedResources;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StreamStateHandle;
import tech.streamfusion.compat.FlinkStateBackendCompat;
import tech.streamfusion.compat.KeyedBackendContext;
import tech.streamfusion.compat.RocksDBOptionsCompat;

/**
 * Flink state backend that gives native StreamFusion operators a Rust-owned RocksDB instance and
 * delegates ordinary JVM keyed state, timers, and operator state to Flink's RocksDB backend.
 */
public final class RocksDBNativeStateBackend extends FlinkStateBackendCompat {

  private static final long serialVersionUID = 1L;

  private final FlinkRocksDBOptions nativeOptions;
  private final boolean incrementalCheckpoints;
  private final String localDirectories;
  private final boolean useManagedMemory;
  private final long fixedPerSlotBytes;
  private final double writeBufferRatio;

  RocksDBNativeStateBackend(ReadableConfig config, ClassLoader classLoader) {
    this(config, configuredDelegate(config, classLoader));
  }

  /** Retains the user's configured host backend when the planner selects native RocksDB. */
  public RocksDBNativeStateBackend(ReadableConfig config, StateBackend delegate) {
    super(delegate);
    this.nativeOptions = FlinkRocksDBOptions.from(config);
    this.incrementalCheckpoints = config.get(CheckpointingOptions.INCREMENTAL_CHECKPOINTS);
    this.localDirectories = config.get(RocksDBOptionsCompat.LOCAL_DIRECTORIES);
    this.useManagedMemory = config.get(RocksDBOptionsCompat.USE_MANAGED_MEMORY);
    this.fixedPerSlotBytes =
        config
            .getOptional(RocksDBOptionsCompat.FIX_PER_SLOT_MEMORY_SIZE)
            .map(MemorySize::getBytes)
            .orElse(0L);
    this.writeBufferRatio = config.get(RocksDBOptionsCompat.WRITE_BUFFER_RATIO);
  }

  /**
   * Leases the slot's native RocksDB memory pool, resolved with Flink's own precedence:
   * fixed-per-slot wins, else the slot's managed-memory share (the default), else fixed-per-TM at
   * TM scope; nothing configured (or a zero budget) leaves stores on their per-instance options.
   * The C++ pool lives in StreamFusion's RocksDB library, so it is leased under its own resource id
   * alongside the delegate backend's pool rather than shared with it.
   */
  private OpaqueMemoryResource<NativeRocksSharedResources> leaseSharedResources(
      KeyedBackendContext<?> parameters) throws Exception {
    MemoryManager memoryManager = parameters.environment().getMemoryManager();
    double ratio = writeBufferRatio;
    if (fixedPerSlotBytes > 0) {
      return memoryManager.getExternalSharedMemoryResource(
          "streamfusion-rocksdb-slot-memory",
          size -> new NativeRocksSharedResources(size, ratio),
          fixedPerSlotBytes);
    }
    if (useManagedMemory && parameters.managedMemoryFraction() > 0) {
      long budget = memoryManager.computeMemorySize(parameters.managedMemoryFraction());
      if (budget > 0) {
        return memoryManager.getExternalSharedMemoryResource(
            "streamfusion-rocksdb-slot-memory",
            size -> new NativeRocksSharedResources(size, ratio),
            budget);
      }
    }
    long fixedPerTm =
        parameters
            .environment()
            .getTaskManagerInfo()
            .getConfiguration()
            .getOptional(RocksDBOptionsCompat.FIX_PER_TM_MEMORY_SIZE)
            .map(MemorySize::getBytes)
            .orElse(0L);
    if (fixedPerTm > 0) {
      SharedResources sharedResources = parameters.environment().getSharedResources();
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
  protected <K> CheckpointableKeyedStateBackend<K> createNativeKeyedBackend(
      KeyedBackendContext<K> parameters) throws Exception {
    List<IncrementalRemoteKeyedStateHandle> nativeHandles = new ArrayList<>();
    List<KeyedStateHandle> delegateHandles = new ArrayList<>();
    for (KeyedStateHandle handle : parameters.stateHandles()) {
      if (handle instanceof IncrementalRemoteKeyedStateHandle
          && RocksDBNativeSnapshotStrategy.isNativeMeta(
              tech.streamfusion.compat.StateCompat.metaHandle(
                  (IncrementalRemoteKeyedStateHandle) handle))) {
        nativeHandles.add((IncrementalRemoteKeyedStateHandle) handle);
      } else {
        delegateHandles.add(handle);
      }
    }

    File workingDirectory = workingDirectory(parameters);
    List<RocksDBRestoredSource> sources = new ArrayList<>();
    for (int i = 0; i < nativeHandles.size(); i++) {
      sources.add(materialize(nativeHandles.get(i), new File(workingDirectory, "restore-" + i)));
    }

    boolean alignedRestore =
        nativeHandles.size() == 1
            && nativeHandles.get(0).getKeyGroupRange().equals(parameters.keyGroupRange());
    UUID backendUID =
        alignedRestore ? nativeHandles.get(0).getBackendIdentifier() : UUID.randomUUID();
    RocksDBNativeSnapshotStrategy strategy =
        new RocksDBNativeSnapshotStrategy(
            backendUID,
            parameters.keyGroupRange(),
            new File(workingDirectory, "checkpoints"),
            incrementalCheckpoints);
    if (alignedRestore && incrementalCheckpoints) {
      IncrementalRemoteKeyedStateHandle restored = nativeHandles.get(0);
      strategy.seedRestored(restored.getCheckpointId(), restored.getSharedState());
    }
    RocksDBNativeKeyedStateBackend<K> backend =
        new RocksDBNativeKeyedStateBackend<>(
            () -> parameters.delegateFactory().apply(delegateHandles),
            parameters.keyGroupRange(),
            parameters.keySerializer(),
            parameters.numberOfKeyGroups(),
            strategy,
            workingDirectory,
            sources,
            nativeOptions.json(),
            leaseSharedResources(parameters));
    if (parameters.canonicalProjectionFactory() != null) {
      backend.setCanonicalProjectionFactory(
          () -> parameters.canonicalProjectionFactory().apply(delegateHandles));
    }
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

  private File workingDirectory(KeyedBackendContext<?> parameters) {
    String operator = parameters.operatorIdentifier().replaceAll("[^A-Za-z0-9_-]", "_");
    int subtask = parameters.environment().getTaskInfo().getIndexOfThisSubtask();
    int attempt = parameters.environment().getTaskInfo().getAttemptNumber();
    File base = parameters.environment().getTaskManagerInfo().getTmpWorkingDirectory();
    if (localDirectories != null && !localDirectories.isBlank()) {
      String[] configured =
          localDirectories.split(",|" + java.util.regex.Pattern.quote(File.pathSeparator));
      String selected = configured[Math.floorMod(subtask, configured.length)].trim();
      if (!selected.isEmpty()) {
        base = new File(selected);
      }
    }
    return new File(
        base,
        "streamfusion-rocksdb/"
            + parameters.jobId()
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
        RocksDBNativeSnapshotStrategy.readMetaDocument(
            tech.streamfusion.compat.StateCompat.metaHandle(handle));
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
  public boolean supportsNoClaimRestoreMode() {
    return true;
  }

  @Override
  public boolean supportsSavepointFormat(SavepointFormatType formatType) {
    return formatType == SavepointFormatType.NATIVE
        || formatType == SavepointFormatType.CANONICAL;
  }
}

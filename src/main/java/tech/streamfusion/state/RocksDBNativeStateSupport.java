package tech.streamfusion.state;

import tech.streamfusion.Native;
import java.util.List;
import java.util.function.BooleanSupplier;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The per-operator glue between a native stateful operator and the RocksDB keyed state backend:
 * the gate deciding whether the operator's state lives in a RocksDB table this run, plus the
 * restored-source marshalling and checkpoint-hook registration every RocksDB-backed operator
 * repeats verbatim. An operator supplies only its own pieces — the native supported-probe, the
 * native create call, and the native checkpoint method — and branches its hot paths on whether
 * this resolved.
 */
public final class RocksDBNativeStateSupport {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBNativeStateSupport.class);

  private final RocksDBNativeKeyedStateBackend<?> backend;
  private final String[] sourceDirectories;
  private final String[] sourceSnapshotTokens;
  private final boolean aligned;
  private final long stateTtlMillis;

  private RocksDBNativeStateSupport(RocksDBNativeKeyedStateBackend<?> backend, long stateTtlMillis) {
    this.backend = backend;
    this.stateTtlMillis = stateTtlMillis;
    List<RocksDBRestoredSource> sources = backend.restoredSources();
    this.sourceDirectories = new String[sources.size()];
    this.sourceSnapshotTokens = new String[sources.size()];
    this.aligned =
        sources.size() == 1
            && sources.get(0).keyGroupStart() == backend.getKeyGroupRange().getStartKeyGroup()
            && sources.get(0).keyGroupEnd() == backend.getKeyGroupRange().getEndKeyGroup();
    for (int i = 0; i < sources.size(); i++) {
      sourceDirectories[i] = sources.get(i).directory();
      sourceSnapshotTokens[i] = sources.get(i).snapshotToken();
    }
  }

  /**
   * Resolves RocksDB mode for one operator, or null when its state stays on memory. The backend
   * takes over only when the job selected it, no raw keyed state arrived (a checkpoint written by
   * the memory backend restores on the memory backend — no silent migration), this build carries
   * the native store, and the operator's own state shape is persistable. A RocksDB backend that
   * loses on a later gate logs the fallback — memory state stays correct, just non-incremental.
   */
  public static RocksDBNativeStateSupport resolve(
      KeyedStateBackend<?> keyedStateBackend,
      String operatorLabel,
      boolean rawStateRestored,
      BooleanSupplier operatorSupported) {
    return resolve(keyedStateBackend, operatorLabel, rawStateRestored, operatorSupported, 0);
  }

  /** {@link #resolve} for an operator whose persistent shape carries state-TTL timestamps. */
  public static RocksDBNativeStateSupport resolve(
      KeyedStateBackend<?> keyedStateBackend,
      String operatorLabel,
      boolean rawStateRestored,
      BooleanSupplier operatorSupported,
      long stateTtlMillis) {
    if (!(keyedStateBackend instanceof RocksDBNativeKeyedStateBackend)) {
      return null;
    }
    RocksDBNativeKeyedStateBackend<?> backend = (RocksDBNativeKeyedStateBackend<?>) keyedStateBackend;
    if (!rawStateRestored && Native.rocksdbStateAvailable() && operatorSupported.getAsBoolean()) {
      return new RocksDBNativeStateSupport(backend, stateTtlMillis);
    }
    LOG.info(
        "{} falls back to memory state under the RocksDB backend "
            + "(unsupported state shape, missing native feature, or raw-state restore)",
        operatorLabel);
    return null;
  }

  public String tableDirectory() {
    return backend.tableDirectory();
  }

  public String optionsJson() {
    return backend.optionsJson();
  }

  public String[] sourceDirectories() {
    return sourceDirectories;
  }

  public String[] sourceSnapshotTokens() {
    return sourceSnapshotTokens;
  }

  /**
   * Whether the restore is a single source covering exactly this subtask's key-group range —
   * the wholesale file-adoption fast path; anything else clips by key-group range at recovery.
   */
  public boolean aligned() {
    return aligned;
  }

  /**
   * The operator's idle-state retention millis (0 = off): the store writes each row's last-write
   * timestamp behind it, and the RocksDB compaction filter may drop expired rows.
   */
  public long stateTtlMillis() {
    return stateTtlMillis;
  }

  public int keyGroupStart() {
    return backend.getKeyGroupRange().getStartKeyGroup();
  }

  public int keyGroupEnd() {
    return backend.getKeyGroupRange().getEndKeyGroup();
  }

  /** Installs the operator's checkpoint hook (see {@link RocksDBNativeState}); call once. */
  public void register(RocksDBNativeState nativeState) {
    backend.registerNativeState(nativeState, stateTtlMillis);
  }

  /** Flushes the write buffer locally; checkpoint publication remains barrier-driven. */
  public void flushForMemoryPressure() {
    try {
      backend.flushForMemoryPressure();
    } catch (Exception failure) {
      throw new IllegalStateException("RocksDB state memory-pressure flush failed", failure);
    }
  }
}

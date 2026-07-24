package io.github.jordepic.streamfusion.state;

import io.github.jordepic.streamfusion.Native;
import java.util.List;
import java.util.function.BooleanSupplier;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The per-operator glue between a native stateful operator and the Paimon keyed state backend:
 * the gate deciding whether the operator's state lives in a Paimon table this run, plus the
 * restored-source marshalling and checkpoint-hook registration every Paimon-backed operator
 * repeats verbatim. An operator supplies only its own pieces — the native supported-probe, the
 * native create call, and the native checkpoint method — and branches its hot paths on whether
 * this resolved.
 */
public final class PaimonNativeStateSupport {

  private static final Logger LOG = LoggerFactory.getLogger(PaimonNativeStateSupport.class);

  private final PaimonKeyedStateBackend<?> backend;
  private final String[] sourceDirectories;
  private final long[] sourceSnapshotIds;

  private PaimonNativeStateSupport(PaimonKeyedStateBackend<?> backend) {
    this.backend = backend;
    List<PaimonRestoredSource> sources = backend.restoredSources();
    this.sourceDirectories = new String[sources.size()];
    this.sourceSnapshotIds = new long[sources.size()];
    for (int i = 0; i < sources.size(); i++) {
      sourceDirectories[i] = sources.get(i).directory();
      sourceSnapshotIds[i] = sources.get(i).snapshotId();
    }
  }

  /**
   * Resolves Paimon mode for one operator, or null when its state stays on memory. The backend
   * takes over only when the job selected it, no raw keyed state arrived (a checkpoint written by
   * the memory backend restores on the memory backend — no silent migration), this build carries
   * the native store, and the operator's own state shape is persistable. A Paimon backend that
   * loses on a later gate logs the fallback — memory state stays correct, just non-incremental.
   */
  public static PaimonNativeStateSupport resolve(
      KeyedStateBackend<?> keyedStateBackend,
      String operatorLabel,
      boolean rawStateRestored,
      BooleanSupplier operatorSupported) {
    if (!(keyedStateBackend instanceof PaimonKeyedStateBackend)) {
      return null;
    }
    PaimonKeyedStateBackend<?> backend = (PaimonKeyedStateBackend<?>) keyedStateBackend;
    if (!rawStateRestored && Native.paimonStateAvailable() && operatorSupported.getAsBoolean()) {
      return new PaimonNativeStateSupport(backend);
    }
    LOG.info(
        "{} falls back to memory state under the Paimon backend "
            + "(unsupported state shape, missing native feature, or raw-state restore)",
        operatorLabel);
    return null;
  }

  public String tableDirectory() {
    return backend.tableDirectory();
  }

  public String[] sourceDirectories() {
    return sourceDirectories;
  }

  public long[] sourceSnapshotIds() {
    return sourceSnapshotIds;
  }

  public int keyGroupStart() {
    return backend.getKeyGroupRange().getStartKeyGroup();
  }

  public int keyGroupEnd() {
    return backend.getKeyGroupRange().getEndKeyGroup();
  }

  /** Installs the operator's checkpoint hook (see {@link PaimonNativeState}); call once. */
  public void register(PaimonNativeState nativeState) {
    backend.registerNativeState(nativeState);
  }
}

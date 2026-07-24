package io.github.jordepic.streamfusion.state;

/**
 * An external maintainer of a native operator's Paimon state table, discovered via {@link
 * java.util.ServiceLoader}. When one is present and available, it owns table maintenance — the
 * native store's fallback compaction is disabled — and runs at every checkpoint barrier on the
 * task thread, immediately before the store commits its write buffer, so the maintenance snapshot
 * always lands directly beneath the checkpoint's data snapshot.
 *
 * <p>The shipped implementation ({@code streamfusion-paimon-compactor}) delegates to stock Java
 * Paimon: its own compaction picks, its sequence-preserving rewriter, its deletion handling.
 */
public interface StateTableCompactor {

  /** Whether this compactor's dependencies are on the classpath (e.g. a Paimon bundle). */
  boolean available();

  /**
   * Whether this compactor can maintain tables of the given data file format. A compactor that
   * cannot read the format must decline, so the native fallback compaction stays on (e.g. Java
   * Paimon releases before 2.0 have no vortex format factory).
   */
  boolean supports(String fileFormat);

  /**
   * Runs one round of table maintenance against the local table directory. Implementations decide
   * themselves whether anything needs compacting; doing nothing is a normal outcome. A failure
   * fails only maintenance, never the checkpoint: the caller logs and continues.
   *
   * @param tableDirectory the state table's local directory
   * @param checkpointId the barrier's checkpoint id (a monotonic commit identifier)
   */
  void compact(String tableDirectory, long checkpointId) throws Exception;
}

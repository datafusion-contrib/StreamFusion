package io.github.jordepic.streamfusion.state;

/**
 * The maintainer of a native operator's Paimon state table, discovered via {@link
 * java.util.ServiceLoader}. The native store itself never compacts: whatever maintenance happens
 * comes from an implementation of this interface, run at every checkpoint barrier on the task
 * thread, immediately before the store commits its write buffer, so the maintenance snapshot
 * always lands directly beneath the checkpoint's data snapshot. Without one, state tables stay
 * correct but accumulate one sorted run per touched bucket per checkpoint.
 *
 * <p>The shipped implementation ({@code streamfusion-paimon-compactor}) delegates to stock Java
 * Paimon: its own compaction picks, its sequence-preserving rewriter, its deletion handling.
 */
public interface StateTableCompactor {

  /** Whether this compactor's dependencies are on the classpath (e.g. a Paimon bundle). */
  boolean available();

  /**
   * Whether this compactor can maintain tables of the given data file format. A compactor that
   * cannot read the format must decline — the tables then run unmaintained (e.g. Java Paimon
   * releases before 2.0 have no vortex format factory).
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

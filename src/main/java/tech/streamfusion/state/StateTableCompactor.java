package tech.streamfusion.state;

import java.util.Map;

/**
 * The maintainer of a native operator's Paimon state table, discovered via {@link
 * java.util.ServiceLoader}. The native store itself never compacts: all maintenance comes from
 * an implementation of this interface, run at every checkpoint barrier on the task thread,
 * immediately before the store commits its write buffer, so the maintenance snapshot always
 * lands directly beneath the checkpoint's data snapshot. State tables always carry deletion
 * vectors, which only this maintenance keeps correct, so the backend refuses to start without a
 * capable implementation (see {@code PaimonStateBackend}).
 *
 * <p>The shipped implementation ({@code streamfusion-paimon-compactor}) delegates to stock Java
 * Paimon: its own compaction picks, its sequence-preserving rewriter, its deletion handling.
 */
public interface StateTableCompactor {

  /** Whether this compactor's dependencies are on the classpath (e.g. a Paimon bundle). */
  boolean available();

  /**
   * Whether this compactor can maintain tables of the given data file format. A compactor that
   * cannot read the format must decline — the backend then fails closed at creation (e.g. Java
   * Paimon releases before 2.0 have no vortex format factory).
   */
  boolean supports(String fileFormat);

  /**
   * Whether this compactor can maintain deletion-vector tables: their lookup compaction compares
   * lookup-file keys through the deployed Paimon's slice comparator, which older releases break
   * on binary primary-key fields (ClassCastException — fixed upstream by comparing binary fields
   * like BinaryRow does, apache/paimon#8873). Deletion vectors are unconditional for state
   * tables, so a false answer fails the backend closed at creation.
   */
  default boolean supportsDeletionVectors() {
    return false;
  }

  /**
   * Opens a long-lived maintenance session on one state table. A session may hold the table and
   * a writer across rounds and fold other writers' commits in incrementally — rebuilding the
   * table view from the full manifest chain on every barrier is the dominant maintenance cost
   * on churn-heavy state. The caller serializes all calls on one session (one compactor per
   * table at a time) and closes it before the table directory is deleted.
   */
  Session open(String tableDirectory) throws Exception;

  /**
   * {@link #open(String)} with dynamic Paimon table options the session must apply to every
   * writer it creates on the table (e.g. the record-level retention that lets compaction
   * physically drop rows the read path already treats as expired). The options are per-session
   * state, never written into the table schema: a job restored with a different retention must
   * drop rows by the restored value, not a stale stamped one. The default ignores the options so
   * third-party implementations stay source-compatible; they then merely reclaim less space.
   */
  default Session open(String tableDirectory, Map<String, String> dynamicOptions)
      throws Exception {
    return open(tableDirectory);
  }

  /** One table's maintenance rounds; not thread-safe, serialized and closed by the caller. */
  interface Session extends AutoCloseable {

    /**
     * The minimal maintenance a barrier must wait for: up-level the barrier's level-0 runs
     * (with deletion vectors maintained) and nothing else. Deletion-vector reads skip level 0,
     * so this is correctness-critical and runs synchronously inside the snapshot; everything
     * discretionary — merging level-1+ runs for read and space amplification — belongs to
     * {@link #shape} off the barrier path. A failure fails the snapshot.
     *
     * @param round a monotonic commit identifier
     */
    void compact(long round) throws Exception;

    /**
     * One discretionary shaping round: ordinary compaction picks (universal triggers) bounding
     * run counts and space amplification. Runs on a background thread; deletion vectors keep
     * reads correct however far shaping lags, so a failed round is only a lost optimization.
     */
    void shape(long round) throws Exception;

    @Override
    void close();
  }
}

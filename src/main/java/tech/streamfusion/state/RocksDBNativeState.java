package tech.streamfusion.state;

/**
 * The native side of a checkpoint for an operator whose state lives in a local RocksDB table. The
 * backend calls this in the synchronous snapshot phase, on the task thread, at the barrier.
 */
@FunctionalInterface
public interface RocksDBNativeState {

  /**
   * Flushes the operator's write buffer, commits the checkpoint's RocksDB snapshot, and returns
   * the file manifest: the first entry is the opaque snapshot token (empty when no state was ever
   * committed), followed by one {@code d:<relative path>} entry per shared data file and one
   * {@code m:<relative path>} entry per private snapshot/manifest/schema document. The strategy
   * hard-links the files its upload will read (new against the last confirmed checkpoint) before
   * the sync phase returns, so uploads survive local compaction and GC without re-linking the
   * whole table each barrier.
   */
  String[] checkpoint() throws Exception;
}

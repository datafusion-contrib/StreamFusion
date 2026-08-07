package tech.streamfusion.state;

/**
 * The native side of a checkpoint for an operator whose state lives in a local RocksDB table. The
 * backend calls this in the synchronous snapshot phase, on the task thread, at the barrier.
 */
@FunctionalInterface
public interface RocksDBNativeState {

  /**
   * Flushes the operator's write buffer, commits a native RocksDB checkpoint into {@code
   * snapshotDirectory}, and returns
   * the file manifest: the first entry is the opaque snapshot token (empty when no state was ever
   * committed), followed by one {@code d:<relative path>} entry per shared data file and one
   * {@code m:<relative path>} entry per private snapshot/manifest/schema document. The strategy
   * uploads the files from that immutable directory asynchronously. An empty directory requests a
   * local memory-pressure flush without publishing checkpoint state.
   */
  String[] checkpoint(String snapshotDirectory) throws Exception;
}

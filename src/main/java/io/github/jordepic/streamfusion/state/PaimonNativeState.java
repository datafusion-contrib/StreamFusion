package io.github.jordepic.streamfusion.state;

/**
 * The native side of a checkpoint for an operator whose state lives in a local Paimon table. The
 * backend calls this in the synchronous snapshot phase, on the task thread, at the barrier.
 */
@FunctionalInterface
public interface PaimonNativeState {

  /**
   * Flushes the operator's write buffer, commits the checkpoint's Paimon snapshot, hard-links its
   * reachable files under {@code linkDirectory} (so the upload survives local compaction and GC),
   * and returns the file manifest: the first entry is the Paimon snapshot id ({@code -1} when no
   * state was ever committed), followed by one {@code d:<relative path>} entry per shared data
   * file and one {@code m:<relative path>} entry per private snapshot/manifest/schema document.
   */
  String[] checkpoint(String linkDirectory) throws Exception;
}

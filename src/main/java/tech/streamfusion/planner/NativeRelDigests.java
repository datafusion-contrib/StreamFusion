package tech.streamfusion.planner;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.sql.SqlExplainLevel;

/**
 * Makes every native rel digest-unique, so Flink's sub-plan reuse (which runs after this planner's
 * substitution stage and merges by digest) can never merge two native nodes — while the rowwise
 * plan around them merges normally.
 *
 * <p>Why native nodes must not merge: a columnar island hands each Arrow batch to exactly one
 * consumer, which closes its off-heap buffers after reading. A digest-merged native subtree would
 * fan one batch out to two consumers — the first close leaves the second reading freed memory.
 * Rowwise sub-plans have no such invariant, and sharing them is the point of reuse: a self-join or
 * multi-view query then reads and converts its source once instead of once per branch.
 *
 * <p>The barrier is a per-instance term emitted only at {@link SqlExplainLevel#DIGEST_ATTRIBUTES}
 * — the level both Calcite's and Flink's digest writers use — so digests differ per instance while
 * {@code EXPLAIN} output is unchanged.
 */
final class NativeRelDigests {

  private static final AtomicLong NEXT_ID = new AtomicLong();

  private NativeRelDigests() {}

  static long nextId() {
    return NEXT_ID.incrementAndGet();
  }

  static RelWriter withBarrier(RelWriter pw, long id) {
    return pw.itemIf("reuseBarrier", id, pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES);
  }
}

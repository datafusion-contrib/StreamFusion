package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;

/**
 * A native source rel whose output stream is fully determined by its configuration, so identical
 * instances within one query can collapse into a single shared source under a {@link
 * StreamPhysicalNativeShare}. Connector modules are optional artifacts — the share pass matches
 * this interface only, never a connector class, so a deployment without a given connector loses
 * nothing but that connector's sharing.
 */
interface ShareableScan {

  /**
   * Everything that determines the source's output stream, byte for byte: two scans of the same
   * class with equal keys read and decode identically, so the plan can keep one and fan its
   * batches out to every branch.
   */
  String sharingKey();

  /**
   * A copy digesting by the dedup group's token instead of a per-instance barrier, so Flink's
   * {@code SameRelObjectShuttle} clones of the shared subtree re-merge by digest (and match
   * nothing else).
   */
  RelNode withShareToken(long token);
}

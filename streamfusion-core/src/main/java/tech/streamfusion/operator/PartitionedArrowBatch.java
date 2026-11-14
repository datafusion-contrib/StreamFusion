package tech.streamfusion.operator;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * One Arrow batch routed to a filesystem sink bucket: the rows of a single partition (the bucket id
 * is Flink's partition path, empty for an unpartitioned table). The partition columns are still
 * present — the native encoder projects them out of the written file — so the batch carries the
 * full row schema end to end.
 *
 * <p>Ownership follows {@link ArrowBatch}: {@link #root()} hands the buffers over, and a backstop
 * frees a batch Flink dropped in flight without any consumer taking it.
 */
public final class PartitionedArrowBatch {

  private final VectorSchemaRoot root;
  private final String bucketId;
  private final AbandonedRootBackstop backstop;

  public PartitionedArrowBatch(VectorSchemaRoot root, String bucketId) {
    this.root = root;
    this.bucketId = bucketId;
    this.backstop = AbandonedRootBackstop.register(this, root);
  }

  /** Hands the batch over: the caller now owns the root and closes it once read. */
  public VectorSchemaRoot root() {
    backstop.handedOver();
    return root;
  }

  public String bucketId() {
    return bucketId;
  }
}

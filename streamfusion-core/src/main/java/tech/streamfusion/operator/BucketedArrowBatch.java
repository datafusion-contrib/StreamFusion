package tech.streamfusion.operator;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * One Arrow batch routed to a Paimon write destination: the rows of a single (partition, bucket)
 * pair, with the partition carried as the {@code BinaryRow} bytes Paimon's writer keys files by.
 * The partition and bucket-key columns stay in the batch so it carries the full row schema end to
 * end.
 *
 * <p>Ownership follows {@link ArrowBatch}: {@link #root()} hands the buffers over, and a backstop
 * frees a batch Flink dropped in flight without any consumer taking it.
 */
public final class BucketedArrowBatch {

  private final VectorSchemaRoot root;
  private final byte[] partition;
  private final int bucket;
  private final AbandonedRootBackstop backstop;

  public BucketedArrowBatch(VectorSchemaRoot root, byte[] partition, int bucket) {
    this.root = root;
    this.partition = partition;
    this.bucket = bucket;
    this.backstop = AbandonedRootBackstop.register(this, root);
  }

  /** Hands the batch over: the caller now owns the root and closes it once read. */
  public VectorSchemaRoot root() {
    backstop.handedOver();
    return root;
  }

  public byte[] partition() {
    return partition;
  }

  public int bucket() {
    return bucket;
  }

  public int rowCount() {
    return root.getRowCount();
  }
}

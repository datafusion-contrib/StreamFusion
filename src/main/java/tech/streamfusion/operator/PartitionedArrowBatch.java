package tech.streamfusion.operator;

import java.lang.ref.Cleaner;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * One Arrow batch routed to a filesystem sink bucket: the rows of a single partition (the bucket id
 * is Flink's partition path, empty for an unpartitioned table). The partition columns are still
 * present — the native encoder projects them out of the written file — so the batch carries the
 * full row schema end to end.
 *
 * <p>Ownership follows {@link ArrowBatch}: {@link #root()} hands the buffers over, and a {@link
 * Cleaner} backstop frees a batch Flink dropped in flight without any consumer taking it.
 */
public final class PartitionedArrowBatch {

  private static final Cleaner ABANDONED = Cleaner.create();

  private final VectorSchemaRoot root;
  private final String bucketId;
  private final Backstop backstop;

  public PartitionedArrowBatch(VectorSchemaRoot root, String bucketId) {
    this.root = root;
    this.bucketId = bucketId;
    this.backstop = new Backstop(root);
    ABANDONED.register(this, backstop);
  }

  /** Hands the batch over: the caller now owns the root and closes it once read. */
  public VectorSchemaRoot root() {
    backstop.handedOver = true;
    return root;
  }

  public String bucketId() {
    return bucketId;
  }

  /** Closes the root of a batch no consumer ever took; must not reference its batch. */
  private static final class Backstop implements Runnable {

    private final VectorSchemaRoot root;
    private volatile boolean handedOver;

    private Backstop(VectorSchemaRoot root) {
      this.root = root;
    }

    @Override
    public void run() {
      if (!handedOver) {
        root.close();
      }
    }
  }
}

package io.github.jordepic.streamfusion.operator;

import java.lang.ref.Cleaner;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * The columnar stream record passed between native operators: one Arrow batch. Carrying batches
 * instead of {@link org.apache.flink.table.data.RowData} lets a chain of native operators stay
 * columnar, with the row↔columnar transpose pushed to the boundary with the host engine.
 *
 * <p>A batch is produced fresh by one operator and handed to the next; calling {@link #root()}
 * transfers ownership — the caller must close the root once read (and an operator that merely
 * inspects the root before forwarding must wrap it in a fresh {@code ArrowBatch}). Within a chained
 * task this hand-off is in-memory (no serialization); only a network edge serializes it (Arrow IPC,
 * via the batch's type serializer).
 *
 * <p>Flink can drop a record in flight with no close hook — records queued between a failing task's
 * operators, or sitting in a source reader's fetch queue, are simply abandoned at teardown. For a
 * heap record that is garbage; for a batch it would leak off-heap buffers on every failover for the
 * TaskManager's lifetime. A {@link Cleaner} backstop frees the root when a batch is collected
 * without any consumer having taken it. Taking the root disarms the backstop, so it can never free
 * buffers a consumer is still reading.
 */
public final class ArrowBatch {

  private static final Cleaner ABANDONED = Cleaner.create();

  private final VectorSchemaRoot root;
  // The destination channel for a key-partitioned batch (the columnar shuffle); -1 when unrouted.
  private final int destination;
  private final Backstop backstop;

  public ArrowBatch(VectorSchemaRoot root) {
    this(root, -1);
  }

  public ArrowBatch(VectorSchemaRoot root, int destination) {
    this.root = root;
    this.destination = destination;
    this.backstop = new Backstop(root);
    ABANDONED.register(this, backstop);
  }

  /** Hands the batch over: the caller now owns the root and closes it once read. */
  public VectorSchemaRoot root() {
    backstop.handedOver = true;
    return root;
  }

  public int destination() {
    return destination;
  }

  public int rowCount() {
    return root.getRowCount();
  }

  /**
   * Closes the root of a batch no consumer ever took (a record Flink dropped in flight). Must not
   * reference its {@code ArrowBatch} — the cleaner runs it once the batch is unreachable.
   */
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

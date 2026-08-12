package tech.streamfusion.operator;

import java.lang.ref.Cleaner;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongConsumer;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.TransferPair;

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
 *
 * <p>A shared sub-plan (one native source feeding several branches of the same query) breaks the
 * one-consumer rule deliberately: the share operator declares the consumer count via
 * {@link #shareAcross}, and each {@link #root()} call but the last then hands out its own zero-copy
 * view over the same retained buffers — every consumer keeps its usual read-then-close contract,
 * and the buffers free when the last view (or the original root, handed to the final consumer)
 * closes. This is the refcounted fan-out Arroyo gets from {@code Arc<RecordBatch>} and RisingWave
 * from Arc-shared chunk columns, expressed through Arrow Java's buffer reference counts.
 */
public final class ArrowBatch {

  private static final Cleaner ABANDONED = Cleaner.create();
  static final long NO_HANDLE_OWNER = 0;

  private final VectorSchemaRoot root;
  // A Flink key group owned by this key-partitioned batch's destination; -1 when unrouted.
  private final int keyGroup;
  private final long parentEpochHigh;
  private final long parentEpochLow;
  private final long parentSequence;
  private final int[] rowOrdinals;
  private final int[] parentKeyGroups;
  // The producing split subtask whose failure cleanup owns a parked zero-copy handle.
  private final long handleOwner;
  private final Backstop backstop;
  // How many consumers have yet to take the batch; 1 for the normal single-consumer hand-off.
  private int pendingConsumers = 1;
  private final LongConsumer encodeTiming;
  private final NativeScanMetrics nativeScanMetrics;

  public ArrowBatch(VectorSchemaRoot root) {
    this(root, -1, NO_HANDLE_OWNER, null, null, 0, 0, -1, null, null);
  }

  public ArrowBatch(VectorSchemaRoot root, int keyGroup) {
    this(root, keyGroup, NO_HANDLE_OWNER, null, null, 0, 0, -1, null, null);
  }

  ArrowBatch(VectorSchemaRoot root, int keyGroup, long handleOwner) {
    this(root, keyGroup, handleOwner, null, null, 0, 0, -1, null, null);
  }

  ArrowBatch(
      VectorSchemaRoot root, int keyGroup, long handleOwner, LongConsumer encodeTiming) {
    this(root, keyGroup, handleOwner, encodeTiming, null, 0, 0, -1, null, null);
  }

  ArrowBatch(VectorSchemaRoot root, NativeScanMetrics nativeScanMetrics) {
    this(root, -1, NO_HANDLE_OWNER, null, nativeScanMetrics, 0, 0, -1, null, null);
  }

  ArrowBatch(
      VectorSchemaRoot root,
      int keyGroup,
      long handleOwner,
      LongConsumer encodeTiming,
      long parentEpochHigh,
      long parentEpochLow,
      long parentSequence,
      int[] rowOrdinals,
      int[] parentKeyGroups) {
    this(
        root,
        keyGroup,
        handleOwner,
        encodeTiming,
        null,
        parentEpochHigh,
        parentEpochLow,
        parentSequence,
        rowOrdinals,
        parentKeyGroups);
  }

  private ArrowBatch(
      VectorSchemaRoot root,
      int keyGroup,
      long handleOwner,
      LongConsumer encodeTiming,
      NativeScanMetrics nativeScanMetrics,
      long parentEpochHigh,
      long parentEpochLow,
      long parentSequence,
      int[] rowOrdinals,
      int[] parentKeyGroups) {
    this.root = root;
    this.keyGroup = keyGroup;
    this.handleOwner = handleOwner;
    this.encodeTiming = encodeTiming;
    this.nativeScanMetrics = nativeScanMetrics;
    this.parentEpochHigh = parentEpochHigh;
    this.parentEpochLow = parentEpochLow;
    this.parentSequence = parentSequence;
    this.rowOrdinals = rowOrdinals;
    this.parentKeyGroups = parentKeyGroups;
    this.backstop = new Backstop(root);
    ABANDONED.register(this, backstop);
  }

  /**
   * Declares that {@code consumers} chained readers will each take this batch once. Called by the
   * share operator before the batch fans out; must precede any {@link #root()} call.
   */
  public synchronized void shareAcross(int consumers) {
    pendingConsumers = consumers;
  }

  /**
   * Hands the batch over: the caller now owns the returned root and closes it once read. Under a
   * declared share, every take but the last receives its own zero-copy view over the same retained
   * buffers, so each consumer's close releases only its own references.
   */
  public synchronized VectorSchemaRoot root() {
    if (pendingConsumers > 1) {
      pendingConsumers--;
      return retainedView();
    }
    backstop.handedOver = true;
    return root;
  }

  /** A new root whose vectors share (and retain) this batch's buffers — Arrow's zero-copy split. */
  private VectorSchemaRoot retainedView() {
    List<FieldVector> shared = new ArrayList<>(root.getFieldVectors().size());
    for (FieldVector vector : root.getFieldVectors()) {
      TransferPair pair = vector.getTransferPair(vector.getAllocator());
      pair.splitAndTransfer(0, vector.getValueCount());
      shared.add((FieldVector) pair.getTo());
    }
    return new VectorSchemaRoot(root.getSchema(), shared, root.getRowCount());
  }

  public int keyGroup() {
    return keyGroup;
  }

  public boolean isOrderedKeyGroupFragment() {
    return parentSequence >= 0;
  }

  long parentEpochHigh() {
    return parentEpochHigh;
  }

  long parentEpochLow() {
    return parentEpochLow;
  }

  long parentSequence() {
    return parentSequence;
  }

  int[] rowOrdinals() {
    return rowOrdinals;
  }

  int[] parentKeyGroups() {
    return parentKeyGroups;
  }

  ArrowBatch retainedCopy() {
    return new ArrowBatch(
        retainedView(),
        keyGroup,
        NO_HANDLE_OWNER,
        null,
        parentEpochHigh,
        parentEpochLow,
        parentSequence,
        rowOrdinals,
        parentKeyGroups);
  }

  /** Compatibility alias for callers that treat the routing tag as an opaque integer. */
  @Deprecated
  public int destination() {
    return keyGroup;
  }

  long handleOwner() {
    return handleOwner;
  }

  void recordEncodeNanos(long nanos) {
    if (encodeTiming != null) {
      encodeTiming.accept(nanos);
    }
  }

  NativeScanMetrics nativeScanMetrics() {
    return nativeScanMetrics;
  }

  /** Per-batch measurements produced inside the native file {@code BulkFormat}. */
  static final class NativeScanMetrics {

    final long openingNanos;
    final long scanningUntilDataNanos;
    final long scanningNanos;
    final long processingNanos;

    NativeScanMetrics(
        long openingNanos,
        long scanningUntilDataNanos,
        long scanningNanos,
        long processingNanos) {
      this.openingNanos = openingNanos;
      this.scanningUntilDataNanos = scanningUntilDataNanos;
      this.scanningNanos = scanningNanos;
      this.processingNanos = processingNanos;
    }
  }

  /** Frees a batch removed from the zero-copy table before any deserializer claimed it. */
  synchronized void closeUnclaimed() {
    backstop.closeUnclaimed();
  }

  /**
   * How many rows the batch carries, without taking it.
   *
   * <p>{@link #root()} is a hand-off, not an accessor — under a share it spends one of the batch's
   * consumers and returns that consumer's own retained view. Anything that only wants to measure a
   * batch (metrics, logging) must ask here instead, or it silently takes a share a real consumer
   * was owed and leaks the view nobody closes.
   */
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

    private synchronized void closeUnclaimed() {
      if (!handedOver) {
        handedOver = true;
        root.close();
      }
    }
  }
}

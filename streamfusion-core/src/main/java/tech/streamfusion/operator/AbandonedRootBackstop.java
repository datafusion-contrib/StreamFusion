package tech.streamfusion.operator;

import java.lang.ref.Cleaner;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Frees the root of a batch-carrying record that Flink dropped in flight without any consumer
 * taking it. The record calls {@link #handedOver()} when a consumer takes ownership; a record that
 * is garbage collected before that closes its root here.
 */
final class AbandonedRootBackstop implements Runnable {

  private static final Cleaner ABANDONED = Cleaner.create();

  private final VectorSchemaRoot root;
  private volatile boolean handedOver;

  private AbandonedRootBackstop(VectorSchemaRoot root) {
    this.root = root;
  }

  static AbandonedRootBackstop register(Object record, VectorSchemaRoot root) {
    AbandonedRootBackstop backstop = new AbandonedRootBackstop(root);
    ABANDONED.register(record, backstop);
    return backstop;
  }

  void handedOver() {
    handedOver = true;
  }

  @Override
  public void run() {
    if (!handedOver) {
      root.close();
    }
  }
}

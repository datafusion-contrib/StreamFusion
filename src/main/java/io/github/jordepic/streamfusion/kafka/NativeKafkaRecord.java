package io.github.jordepic.streamfusion.kafka;

import io.github.jordepic.streamfusion.operator.ArrowBatch;

/**
 * One per-partition decoded batch as it flows from the native {@link NativeKafkaSplitReader} through
 * the source reader's queue to the emitter. It carries the split's next offset alongside the batch so
 * the emitter can advance that split's checkpoint state after collecting the batch downstream.
 */
final class NativeKafkaRecord {

  private final ArrowBatch batch;
  private final long nextOffset;

  NativeKafkaRecord(ArrowBatch batch, long nextOffset) {
    this.batch = batch;
    this.nextOffset = nextOffset;
  }

  ArrowBatch batch() {
    return batch;
  }

  /** Offset to resume this split from — the checkpoint position after this batch is emitted. */
  long nextOffset() {
    return nextOffset;
  }
}

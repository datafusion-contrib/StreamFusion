package io.github.jordepic.streamfusion.kafka;

import io.github.jordepic.streamfusion.operator.ArrowBatch;

/**
 * One per-partition decoded batch as it flows from the native {@link NativeKafkaSplitReader} through
 * the source reader's queue to the emitter. It carries the split's next offset alongside the batch so
 * the emitter can advance that split's checkpoint state after collecting the batch downstream, and the
 * batch's max rowtime so the emitter can timestamp it for per-split source watermarks.
 */
final class NativeKafkaRecord {

  private final ArrowBatch batch;
  private final long nextOffset;
  private final long maxRowtimeMillis;

  NativeKafkaRecord(ArrowBatch batch, long nextOffset, long maxRowtimeMillis) {
    this.batch = batch;
    this.nextOffset = nextOffset;
    this.maxRowtimeMillis = maxRowtimeMillis;
  }

  ArrowBatch batch() {
    return batch;
  }

  /** Offset to resume this split from — the checkpoint position after this batch is emitted. */
  long nextOffset() {
    return nextOffset;
  }

  /**
   * Max of the batch's rowtime column in epoch millis, or {@code Long.MIN_VALUE} when the table has no
   * watermark (or every rowtime in the batch is null). Emitted as the batch's record timestamp so
   * Flink's per-split watermark generator sees it.
   */
  long maxRowtimeMillis() {
    return maxRowtimeMillis;
  }
}

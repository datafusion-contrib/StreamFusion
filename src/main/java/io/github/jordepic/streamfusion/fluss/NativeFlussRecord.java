package io.github.jordepic.streamfusion.fluss;

import io.github.jordepic.streamfusion.operator.ArrowBatch;

/** One Arrow batch from a Fluss log split plus the split offset after the batch. */
final class NativeFlussRecord {

  private final ArrowBatch batch;
  private final long nextOffset;

  NativeFlussRecord(ArrowBatch batch, long nextOffset) {
    this.batch = batch;
    this.nextOffset = nextOffset;
  }

  ArrowBatch batch() {
    return batch;
  }

  long nextOffset() {
    return nextOffset;
  }
}

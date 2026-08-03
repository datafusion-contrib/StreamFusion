package tech.streamfusion.operator;

import java.util.function.LongConsumer;
import org.apache.flink.api.connector.source.SourceOutput;

/**
 * One per-split Arrow batch as it flows from a native split reader through the source reader's
 * queue to the emitter. It carries the split's next offset so the emitter can advance that split's
 * checkpoint state after collecting the batch downstream.
 */
public final class NativeSourceRecord {

  private final ArrowBatch batch;
  private final long nextOffset;
  private final long maxRowtimeMillis;

  public NativeSourceRecord(ArrowBatch batch, long nextOffset, long maxRowtimeMillis) {
    this.batch = batch;
    this.nextOffset = nextOffset;
    this.maxRowtimeMillis = maxRowtimeMillis;
  }

  public ArrowBatch batch() {
    return batch;
  }

  /** Offset to resume this split from — the checkpoint position after this batch is emitted. */
  public long nextOffset() {
    return nextOffset;
  }

  /**
   * Max of the batch's rowtime column in epoch millis, or {@code Long.MIN_VALUE} when the table has
   * no watermark (or every rowtime in the batch is null). Emitted as the batch's record timestamp so
   * the source operator's per-split watermark generator sees it.
   */
  public long maxRowtimeMillis() {
    return maxRowtimeMillis;
  }

  /**
   * Collects the batch downstream, then advances the split's checkpoint offset. A batch-less record
   * (a fused decode dropped every document) still advances the offset. A watermarked table's batch
   * is collected with its max rowtime as the record timestamp: the source operator's per-split
   * watermark generator ({@link NativeSourceWatermarks}) folds it in, which is equivalent to
   * feeding every row because the delay is constant and the generator keeps a max.
   */
  public void emit(SourceOutput<ArrowBatch> output, LongConsumer nextOffsetSetter) {
    if (batch != null) {
      if (maxRowtimeMillis == Long.MIN_VALUE) {
        output.collect(batch);
      } else {
        output.collect(batch, maxRowtimeMillis);
      }
    }
    nextOffsetSetter.accept(nextOffset);
  }
}

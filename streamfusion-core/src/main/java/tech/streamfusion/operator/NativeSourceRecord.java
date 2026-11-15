package tech.streamfusion.operator;

import java.util.function.LongConsumer;
import org.apache.arrow.vector.VectorSchemaRoot;
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

  public static NativeSourceRecord fromRoot(
      VectorSchemaRoot root,
      long nextOffset,
      int rowtimeIndex,
      WatermarkExpression.Evaluator expression) {
    try {
      NativeSourceWatermarks.Summary watermarks =
          NativeSourceWatermarks.summarize(root, rowtimeIndex, expression);
      return new NativeSourceRecord(
          new ArrowBatch(root, watermarks), nextOffset, watermarks.maxRowtimeMillis);
    } catch (RuntimeException | Error failure) {
      root.close();
      throw failure;
    }
  }

  /** Releases a failed source emission only while no downstream consumer has accepted its root. */
  public void discardUnclaimed() {
    if (batch != null) batch.closeUnclaimed();
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
   * no watermark (or every rowtime in the batch is null). Emitted as the batch's record timestamp
   * so downstream record timestamps retain event-time semantics.
   */
  public long maxRowtimeMillis() {
    return maxRowtimeMillis;
  }

  /**
   * Collects the batch downstream, then advances the split's checkpoint offset. A batch-less record
   * (a fused decode dropped every document) still advances the offset. A watermarked table's batch
   * is collected with its max rowtime as the record timestamp. The source operator's per-split
   * generator separately folds the precomputed watermark candidate carried by the batch.
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

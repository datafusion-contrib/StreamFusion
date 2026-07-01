package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;

/**
 * Columnar single-phase session-window aggregation: the same native session aggregator as {@link
 * NativeSessionWindowAggregateOperator}, but fed Arrow batches directly instead of buffered rows, and
 * emitting Arrow batches ({@code [key?, agg…, window_start, window_end]}). The whole operator is
 * Arrow → Arrow; a rowwise sink is reached through the dedicated {@code ArrowToRowDataOperator}
 * the planner inserts at the island perimeter.
 *
 * <p>Event-time sessions measure the gap in the rowtime column and close on a watermark. A
 * **proctime** session instead times every element at the operator's processing-time clock (Flink's
 * processing-time assigner uses the clock, not a row value) and closes a gap-separated session on a
 * processing-time timer: each batch registers a cleanup timer at {@code now + gap}, the earliest the
 * session could end with no further input. A later element extends the session and registers its own
 * later timer, so when a timer fires only the sessions the clock has truly passed are emitted.
 * Remaining open sessions are flushed when the (bounded) input finishes.
 */
public class NativeColumnarSessionWindowAggregateOperator extends NativeRowWindowOperatorCore
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch>, ProcessingTimeCallback {

  private final long gapMillis;
  private final boolean proctime;
  private final int timeColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] keyTypes;

  /** The latest cleanup boundary already scheduled, so each {@code now + gap} timer registers once. */
  private transient long registeredTimer;

  public NativeColumnarSessionWindowAggregateOperator(
      long gapMillis,
      int timeColumn,
      int[] valueColumns,
      int[] keyColumns,
      int[] keyTypes,
      int[] valueTypes,
      int[] aggregateKinds,
      String timeZoneId,
      RowType outputType,
      boolean proctime) {
    // Sessions have no fixed size or slide; the gap is the only window parameter, carried separately.
    super("streamfusion-session-aggregate-state", 0, 0, valueTypes, aggregateKinds, timeZoneId, outputType);
    this.gapMillis = gapMillis;
    this.timeColumn = timeColumn;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.keyTypes = keyTypes;
    this.proctime = proctime;
  }

  @Override
  public void open() throws Exception {
    super.open();
    registeredTimer = Long.MIN_VALUE;
  }

  @Override
  protected long createHandle() {
    return Native.createSessionAggregator(gapMillis, valueTypes, aggregateKinds, memoryBudgetBytes());
  }

  @Override
  protected long restoreHandle(byte[] snapshot) {
    return Native.restoreSessionAggregator(
        gapMillis, valueTypes, aggregateKinds, snapshot, memoryBudgetBytes());
  }

  @Override
  protected void updateHandle(long arrayAddress, long schemaAddress) {
    Native.updateSessionAggregator(handle, arrayAddress, schemaAddress);
  }

  @Override
  protected void flushHandle(long watermark, long arrayAddress, long schemaAddress) {
    Native.flushSessionAggregator(handle, watermark, arrayAddress, schemaAddress);
  }

  @Override
  protected byte[] snapshotHandle() {
    return Native.snapshotSessionAggregator(handle);
  }

  @Override
  protected void closeHandle() {
    Native.closeSessionAggregator(handle);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    try (VectorSchemaRoot in = element.getValue().root()) {
      if (proctime) {
        long now = getProcessingTimeService().getCurrentProcessingTime();
        updateColumnarProctime(in, now, valueColumns, keyColumns, keyTypes);
        emitClosedWindows(now); // emit any session the clock has already left behind by a full gap
        // The session can close no earlier than `now + gap`; a later element pushes it out and
        // registers a later timer. Processing time only advances, so the latest boundary scheduled
        // covers every session open as of `now`.
        long deadline = now + gapMillis;
        if (deadline > registeredTimer) {
          getProcessingTimeService().registerTimer(deadline, this);
          registeredTimer = deadline;
        }
      } else {
        updateColumnar(in, timeColumn, valueColumns, keyColumns, keyTypes);
      }
    }
  }

  @Override
  public void onProcessingTime(long time) {
    // Every open session was extended by an element that registered its own `now + gap` timer, so a
    // flush at the current clock is enough — no re-registration is needed here.
    emitClosedWindows(getProcessingTimeService().getCurrentProcessingTime());
  }

  @Override
  public void finish() throws Exception {
    if (proctime) {
      emitClosedWindows(Long.MAX_VALUE); // end of input: close every remaining open session
    }
    super.finish();
  }

  @Override
  protected void flushPending() {
    // Each batch is folded into the aggregator as it arrives; nothing is buffered here.
  }

  @Override
  protected void emitClosedWindows(long watermark) {
    emitFinal(watermark, keyTypes);
  }
}

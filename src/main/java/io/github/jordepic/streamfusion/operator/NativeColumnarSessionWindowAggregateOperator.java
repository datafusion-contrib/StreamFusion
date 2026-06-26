package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;

/**
 * Columnar single-phase session-window aggregation: the same native session aggregator as {@link
 * NativeSessionWindowAggregateOperator}, but fed Arrow batches directly instead of buffered rows, and
 * emitting Arrow batches ({@code [key?, agg…, window_start, window_end]}). The whole operator is
 * Arrow → Arrow (ticket 36); a rowwise sink is reached through the dedicated {@code ArrowToRowDataOperator}
 * the planner inserts at the island perimeter.
 */
public class NativeColumnarSessionWindowAggregateOperator extends NativeRowWindowOperatorCore
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final long gapMillis;
  private final int timeColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] keyTypes;

  public NativeColumnarSessionWindowAggregateOperator(
      long gapMillis,
      int timeColumn,
      int[] valueColumns,
      int[] keyColumns,
      int[] keyTypes,
      int[] valueTypes,
      int[] aggregateKinds,
      String timeZoneId,
      RowType outputType) {
    // Sessions have no fixed size or slide; the gap is the only window parameter, carried separately.
    super("streamfusion-session-aggregate-state", 0, 0, valueTypes, aggregateKinds, timeZoneId, outputType);
    this.gapMillis = gapMillis;
    this.timeColumn = timeColumn;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.keyTypes = keyTypes;
  }

  @Override
  protected long createHandle() {
    return Native.createSessionAggregator(gapMillis, valueTypes, aggregateKinds);
  }

  @Override
  protected long restoreHandle(byte[] snapshot) {
    return Native.restoreSessionAggregator(gapMillis, valueTypes, aggregateKinds, snapshot);
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
      updateColumnar(in, timeColumn, valueColumns, keyColumns, keyTypes);
    }
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

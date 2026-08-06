package tech.streamfusion.operator;

import tech.streamfusion.arrow.ArrowConversion;
import java.time.ZoneOffset;
import java.time.Instant;
import java.time.ZoneId;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

/**
 * The final-result layer of the window operator core: adds {@link #emitFinal}, which fetches the
 * windows a watermark has closed and emits them as an Arrow batch shaped to the operator's output row
 * type ({@code [key?, agg…, window_start, window_end]}). Window operators that produce final per-window
 * results (single-phase, global, session) extend this; the partial-emitting local operator extends the
 * output-agnostic {@link NativeWindowOperatorCore} directly. Every native operator but a source/sink is
 * Arrow → Arrow, so the final aggregates emit Arrow too; the transpose to {@code RowData}
 * for a rowwise sink is the dedicated {@code ArrowToRowDataOperator}, inserted by the planner at the
 * island perimeter.
 */
public abstract class NativeRowWindowOperatorCore extends NativeWindowOperatorCore<ArrowBatch> {

  private static final long NANOS_PER_MILLI = 1_000_000L;

  private final RowType outputType;
  private final boolean inputTimestampLtz;
  private final String sessionTimeZoneId;
  private transient ZoneId sessionZone;
  private transient FlinkWindowMetrics flinkWindowMetrics;

  protected NativeRowWindowOperatorCore(
      String stateName,
      long windowMillis,
      long slideMillis,
      int[] valueTypes,
      int[] aggregateKinds,
      String timeZoneId,
      boolean inputTimestampLtz,
      String sessionTimeZoneId,
      RowType outputType,
      int[] keyTimestampPrecisions,
      int maxParallelism) {
    super(
        stateName,
        windowMillis,
        slideMillis,
        valueTypes,
        aggregateKinds,
        timeZoneId,
        keyTimestampPrecisions,
        maxParallelism);
    this.outputType = outputType;
    this.inputTimestampLtz = inputTimestampLtz;
    this.sessionTimeZoneId = sessionTimeZoneId;
  }

  @Override
  public void open() throws Exception {
    super.open();
    sessionZone = ZoneId.of(sessionTimeZoneId);
    flinkWindowMetrics =
        new FlinkWindowMetrics(getMetricGroup(), getProcessingTimeService());
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    if (isEventTimeWindow()) {
      flinkWindowMetrics.onWatermark(mark.getTimestamp());
    }
    super.processWatermark(mark);
  }

  /** Whether watermarks, rather than processing-time timers, drive this window. */
  protected boolean isEventTimeWindow() {
    return true;
  }

  /** Samples the native late-row total into Flink's counter and meter. */
  protected final void reportLateRecords(long cumulativeLateRecords) {
    flinkWindowMetrics.reportLateRecords(cumulativeLateRecords);
  }

  /**
   * Emits the windows the watermark has closed as one Arrow batch in the output row order
   * {@code [key?, agg0..aggN-1, window_start, window_end]}. The native flush carries keys in their
   * natural type (int widened to int64, timestamp keys as int64 nanos), the aggregate results already
   * in their output Arrow type, and the two window bounds as int64 epoch millis; this reshapes them
   * into the output Arrow schema, narrowing int keys, carrying timestamp-key nanos through, and
   * rendering the window bounds as session-local timestamps (matching the host). Nothing is emitted
   * for an empty flush.
   */
  protected final void emitFinal(long watermark, int[] keyTypes) {
    int keyCount = keyTypes.length;
    int aggregates = aggregateCount();
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      flushHandle(watermark, array.memoryAddress(), schema.memoryAddress());
      try (VectorSchemaRoot flush =
          Data.importVectorSchemaRoot(allocator, array, schema, dictionaries)) {
        int n = flush.getRowCount();
        if (n == 0) {
          return;
        }
        VectorSchemaRoot out = VectorSchemaRoot.create(ArrowConversion.toArrowSchema(outputType), allocator);
        out.allocateNew();
        for (int j = 0; j < keyCount; j++) {
          copyKeyColumn(flush.getVector("key" + j), out.getVector(j), n);
        }
        for (int a = 0; a < aggregates; a++) {
          copyColumn(flush.getVector("result" + a), out.getVector(keyCount + a), n);
        }
        // Window properties follow the keys and aggregates: always window_start then window_end, and
        // (legacy group-window only) a rowtime attribute (= window_end - 1 ms, the window's last
        // instant) and a proctime attribute. Extra properties are present in the output schema but are
        // projected away by the Calc above; their exact value is immaterial, so the proctime marker is
        // filled with the window end. The TVF window aggregates carry only the two bound properties.
        int properties = outputType.getFieldCount() - keyCount - aggregates;
        int base = keyCount + aggregates;
        BigIntVector starts = (BigIntVector) flush.getVector("window_start");
        BigIntVector ends = (BigIntVector) flush.getVector("window_end");
        fillLocalTimestamps(starts, out.getVector(base), isLtz(base), n);
        fillLocalTimestamps(ends, out.getVector(base + 1), isLtz(base + 1), n);
        if (properties >= 3) {
          fillLocalTimestamps(ends, out.getVector(base + 2), isLtz(base + 2), n, -1L);
        }
        if (properties >= 4) {
          fillLocalTimestamps(ends, out.getVector(base + 3), isLtz(base + 3), n, 0L);
        }
        out.setRowCount(n);
        ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(out));
      }
    }
  }

  /** Copies a column verbatim (source and target share the Arrow type). */
  private static void copyColumn(FieldVector source, FieldVector target, int n) {
    for (int i = 0; i < n; i++) {
      target.copyFromSafe(i, i, source);
    }
  }

  /**
   * Copies a key column, undoing the native carriage: an int key widened to int64 narrows back to
   * int32, and a timestamp key carried as int64 nanos rides into a timestamp vector; every other key
   * type matches and copies verbatim.
   */
  private static void copyKeyColumn(FieldVector source, FieldVector target, int n) {
    if (target instanceof IntVector) {
      IntVector dst = (IntVector) target;
      BigIntVector src = (BigIntVector) source;
      for (int i = 0; i < n; i++) {
        if (src.isNull(i)) {
          dst.setNull(i);
        } else {
          dst.setSafe(i, (int) src.get(i));
        }
      }
    } else if (isTimestampVector(target) && source instanceof BigIntVector) {
      BigIntVector src = (BigIntVector) source;
      for (int i = 0; i < n; i++) {
        if (src.isNull(i)) {
          setTimestampNull(target, i);
        } else {
          setTimestampNanos(target, i, src.get(i));
        }
      }
    } else {
      copyColumn(source, target, n);
    }
  }

  /** Renders int64 epoch-millis window bounds as session-local timestamp nanos, as the host does. */
  private boolean isLtz(int field) {
    return outputType.getTypeAt(field).getTypeRoot()
        == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
  }

  private void fillLocalTimestamps(
      BigIntVector source, FieldVector target, boolean targetLtz, int n) {
    fillLocalTimestamps(source, target, targetLtz, n, 0L);
  }

  /** As above, offsetting the source millis first (e.g. -1 ms for a window's rowtime = end - 1). */
  private void fillLocalTimestamps(
      BigIntVector source, FieldVector target, boolean targetLtz, int n, long offsetMillis) {
    for (int i = 0; i < n; i++) {
      if (source.isNull(i)) {
        setTimestampNull(target, i);
      } else {
        long boundary = source.get(i) + offsetMillis;
        long rendered;
        if (targetLtz) {
          // A window-time property is an instant. LTZ input boundaries already are epoch millis;
          // plain TIMESTAMP boundaries are local wall-clock and must be interpreted in the session zone.
          rendered =
              inputTimestampLtz
                  ? boundary
                  : Instant.ofEpochMilli(boundary)
                      .atZone(ZoneOffset.UTC)
                      .toLocalDateTime()
                      .atZone(sessionZone)
                      .toInstant()
                      .toEpochMilli();
        } else {
          // Visible window_start/end are plain TIMESTAMP wall-clock values.
          rendered =
              inputTimestampLtz
                  ? Instant.ofEpochMilli(boundary)
                      .atZone(sessionZone)
                      .toLocalDateTime()
                      .toInstant(ZoneOffset.UTC)
                      .toEpochMilli()
                  : boundary;
        }
        setTimestampNanos(target, i, rendered * NANOS_PER_MILLI);
      }
    }
  }

  private static boolean isTimestampVector(FieldVector vector) {
    return vector instanceof TimeStampNanoVector || vector instanceof TimeStampNanoTZVector;
  }

  private static void setTimestampNanos(FieldVector target, int i, long nanos) {
    if (target instanceof TimeStampNanoVector) {
      ((TimeStampNanoVector) target).setSafe(i, nanos);
    } else {
      ((TimeStampNanoTZVector) target).setSafe(i, nanos);
    }
  }

  private static void setTimestampNull(FieldVector target, int i) {
    if (target instanceof TimeStampNanoVector) {
      ((TimeStampNanoVector) target).setNull(i);
    } else {
      ((TimeStampNanoTZVector) target).setNull(i);
    }
  }
}

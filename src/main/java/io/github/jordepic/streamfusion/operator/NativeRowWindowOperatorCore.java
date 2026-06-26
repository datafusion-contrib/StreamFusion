package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.arrow.ArrowConversion;
import java.time.ZoneOffset;
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
import org.apache.flink.table.types.logical.RowType;

/**
 * The final-result layer of the window operator core: adds {@link #emitFinal}, which fetches the
 * windows a watermark has closed and emits them as an Arrow batch shaped to the operator's output row
 * type ({@code [key?, agg…, window_start, window_end]}). Window operators that produce final per-window
 * results (single-phase, global, session) extend this; the partial-emitting local operator extends the
 * output-agnostic {@link NativeWindowOperatorCore} directly. Every native operator but a source/sink is
 * Arrow → Arrow (ticket 36), so the final aggregates emit Arrow too; the transpose to {@code RowData}
 * for a rowwise sink is the dedicated {@code ArrowToRowDataOperator}, inserted by the planner at the
 * island perimeter.
 */
public abstract class NativeRowWindowOperatorCore extends NativeWindowOperatorCore<ArrowBatch> {

  private static final long NANOS_PER_MILLI = 1_000_000L;

  private final RowType outputType;

  protected NativeRowWindowOperatorCore(
      String stateName,
      long windowMillis,
      long slideMillis,
      int[] valueTypes,
      int[] aggregateKinds,
      String timeZoneId,
      RowType outputType) {
    super(stateName, windowMillis, slideMillis, valueTypes, aggregateKinds, timeZoneId);
    this.outputType = outputType;
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
        fillLocalTimestamps(
            (BigIntVector) flush.getVector("window_start"), out.getVector(keyCount + aggregates), n);
        fillLocalTimestamps(
            (BigIntVector) flush.getVector("window_end"), out.getVector(keyCount + aggregates + 1), n);
        out.setRowCount(n);
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
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
  private void fillLocalTimestamps(BigIntVector source, FieldVector target, int n) {
    for (int i = 0; i < n; i++) {
      if (source.isNull(i)) {
        setTimestampNull(target, i);
      } else {
        long localMillis = toLocal(source.get(i)).toInstant(ZoneOffset.UTC).toEpochMilli();
        setTimestampNanos(target, i, localMillis * NANOS_PER_MILLI);
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

package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.arrow.ArrowConversion;
import io.github.jordepic.streamfusion.arrow.ArrowReader;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/**
 * Transpose leaving a columnar region: reads each {@link ArrowBatch} back into rows. Sits where a
 * native columnar operator feeds a rowwise (host) one, so the Arrow→row conversion happens once at
 * the boundary. It consumes (and closes) each batch it receives.
 *
 * <p>Rather than deep-copy every row off the buffers, it emits the reader's reusable {@code
 * ColumnarRowData} — a zero-copy view over the Arrow vectors that reads each field on demand (the
 * columnar→row model Spark/Comet use). The batch stays open for the whole emit loop, and the chaining
 * output consumes each row inline before the next {@code read} repoints the reused row (the host
 * copies it first when object reuse is off), so the view is always valid. This drops the per-row
 * {@code GenericRowData} allocation and field boxing the deep copy paid — most of the exit transpose.
 */
public class ArrowToRowDataOperator extends AbstractStreamOperator<RowData>
    implements OneInputStreamOperator<ArrowBatch, RowData> {

  private final RowType rowType;

  public ArrowToRowDataOperator(RowType rowType) {
    this.rowType = rowType;
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    try (VectorSchemaRoot root = element.getValue().root()) {
      ArrowReader reader = ArrowConversion.createArrowReader(root, rowType);
      TinyIntVector kinds = (TinyIntVector) root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
      int rowCount = root.getRowCount();
      for (int i = 0; i < rowCount; i++) {
        RowData row = reader.read(i);
        if (kinds != null) {
          row.setRowKind(RowKind.fromByteValue(kinds.get(i)));
        }
        output.collect(new StreamRecord<>(row));
      }
    }
  }
}

package tech.streamfusion.operator;

import java.util.ArrayList;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import tech.streamfusion.Native;
import tech.streamfusion.compat.FlinkStreamOperator;

/** Stateless columnar counterpart of Flink's DropUpdateBeforeFunction. */
public class NativeColumnarDropUpdateBeforeOperator extends FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ArrowBatch batch = element.getValue();
    int rows = batch.rowCount();
    ColumnarRecordMetrics.countIngested(getMetricGroup(), rows);
    TinyIntVector kinds =
        (TinyIntVector) batch.inspectRoot().getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
    int removed = 0;
    if (kinds != null) {
      for (int i = 0; i < rows; i++) {
        if (kinds.get(i) == 1) removed++;
      }
    }
    if (removed == 0) {
      ColumnarRecordMetrics.forward(output, getMetricGroup(), element, rows);
      return;
    }
    try (VectorSchemaRoot in = batch.root()) {
      if (removed == rows) return;
      var allocator = NativeAllocator.SHARED;
      var inAllocator = in.getFieldVectors().get(0).getAllocator();
      try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
          ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator);
          ArrowArray outArray = ArrowArray.allocateNew(allocator);
          ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(
            inAllocator, in, NativeAllocator.DICTIONARIES, inArray, inSchema);
        Native.dropUpdateBefore(
            inArray.memoryAddress(), inSchema.memoryAddress(),
            outArray.memoryAddress(), outSchema.memoryAddress());
        VectorSchemaRoot result =
            Data.importVectorSchemaRoot(
                allocator, outArray, outSchema, NativeAllocator.DICTIONARIES);
        if (!result.getSchema().equals(in.getSchema())) {
          // Filtering preserves fields. Arrow Java import can rename list children; keep the
          // original names but imported dictionary IDs. Only this replacement root owns vectors.
          var fields = new ArrayList<Field>();
          for (int i = 0; i < in.getSchema().getFields().size(); i++) {
            fields.add(
                preserveFieldNames(
                    in.getSchema().getFields().get(i), result.getSchema().getFields().get(i)));
          }
          result =
              new VectorSchemaRoot(
                  new Schema(fields, result.getSchema().getCustomMetadata()),
                  result.getFieldVectors(),
                  result.getRowCount());
        }
        ArrowBatch filtered = batch.filtered(result, i -> kinds.get(i) != 1);
        ColumnarRecordMetrics.forward(
            output, getMetricGroup(), element.copy(filtered), filtered.rowCount());
      }
    }
  }

  private static Field preserveFieldNames(Field original, Field imported) {
    var children = new ArrayList<Field>();
    for (int i = 0; i < original.getChildren().size(); i++) {
      children.add(preserveFieldNames(original.getChildren().get(i), imported.getChildren().get(i)));
    }
    return new Field(original.getName(), imported.getFieldType(), children);
  }
}

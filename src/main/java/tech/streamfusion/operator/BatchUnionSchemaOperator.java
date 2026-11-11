package tech.streamfusion.operator;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** Gives union branches one column header while forwarding the same owned Arrow vectors. */
public final class BatchUnionSchemaOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {
  private final String[] names;

  public BatchUnionSchemaOperator(List<String> names) {
    this.names = names.toArray(new String[0]);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ArrowBatch batch = element.getValue();
    VectorSchemaRoot root = batch.root();
    boolean handedOff = false;
    try {
      Schema input = root.getSchema();
      var fields = new ArrayList<>(input.getFields());
      for (int i = 0; i < names.length; i++) {
        Field field = fields.get(i);
        if (names[i].equals(field.getName()) && field.isNullable()) continue;
        fields.set(
            i,
            new Field(
                names[i],
                new FieldType(true, field.getType(), field.getDictionary(), field.getMetadata()),
                field.getChildren()));
      }
      var common = new Schema(fields, input.getCustomMetadata());
      var normalized =
          common.equals(input)
              ? root
              : new VectorSchemaRoot(common, root.getFieldVectors(), root.getRowCount());
      // The replacement root takes the vector ownership; closing the old header would free it.
      ArrowBatch result = batch.filtered(normalized, ignored -> true);
      handedOff = true;
      output.collect(element.replace(result));
    } finally {
      if (!handedOff) root.close();
    }
  }
}

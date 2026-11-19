package tech.streamfusion.arrow;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.RowType;

final class FixedBinaryFixtures {
  static Schema schema(RowType rowType) {
    var fields = new ArrayList<>(ArrowConversion.toArrowSchema(rowType).getFields());
    for (int i = 0; i < fields.size(); i++) {
      if (rowType.getTypeAt(i) instanceof BinaryType) {
        var field = fields.get(i);
        fields.set(
            i,
            new Field(
                field.getName(),
                new FieldType(
                    field.isNullable(),
                    new ArrowType.FixedSizeBinary(((BinaryType) rowType.getTypeAt(i)).getLength()),
                    null),
                List.of()));
      }
    }
    return new Schema(fields);
  }
}

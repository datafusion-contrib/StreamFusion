package tech.streamfusion.operator;

import java.io.IOException;
import java.io.Serializable;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;

/** Flink owns complex accumulator serialization; Arrow carries those fields as binary values. */
public final class JvmAggregatePartialCodec implements Serializable {
  private final RowType type;
  private transient RowData.FieldGetter[] getters;
  private transient TypeSerializer<Object>[] serializers;
  private transient DataOutputSerializer output;
  private transient DataInputDeserializer input;

  public JvmAggregatePartialCodec(RowType type) {
    this.type = type;
  }

  public static RowType transportType(RowType type) {
    return new RowType(
        type.isNullable(),
        type.getFields().stream()
            .map(
                field ->
                    new RowType.RowField(
                        field.getName(),
                        encoded(field.getType())
                            ? new VarBinaryType(
                                field.getType().isNullable(), VarBinaryType.MAX_LENGTH)
                            : field.getType()))
            .collect(java.util.stream.Collectors.toList()));
  }

  private static boolean encoded(LogicalType type) {
    switch (type.getTypeRoot()) {
      case ROW:
      case ARRAY:
      case MAP:
      case MULTISET:
      case RAW:
      case STRUCTURED_TYPE:
        return true;
      default:
        return false;
    }
  }

  @SuppressWarnings("unchecked")
  private void initialize() {
    if (getters != null) return;
    getters = new RowData.FieldGetter[type.getFieldCount()];
    serializers = new TypeSerializer[type.getFieldCount()];
    for (int i = 0; i < type.getFieldCount(); i++) {
      getters[i] = RowData.createFieldGetter(type.getTypeAt(i), i);
      if (encoded(type.getTypeAt(i)))
        serializers[i] = InternalSerializers.create(type.getTypeAt(i));
    }
    output = new DataOutputSerializer(128);
    input = new DataInputDeserializer();
  }

  public RowData encode(RowData row) throws IOException {
    initialize();
    var result = new GenericRowData(row.getRowKind(), type.getFieldCount());
    for (int i = 0; i < type.getFieldCount(); i++) {
      Object value = getters[i].getFieldOrNull(row);
      if (value != null && serializers[i] != null) {
        output.clear();
        serializers[i].serialize(value, output);
        value = output.getCopyOfBuffer();
      }
      result.setField(i, value);
    }
    return result;
  }

  public RowData decode(RowData row) throws IOException {
    initialize();
    var result = new GenericRowData(row.getRowKind(), type.getFieldCount());
    for (int i = 0; i < type.getFieldCount(); i++) {
      Object value = null;
      if (!row.isNullAt(i)) {
        if (serializers[i] == null) value = getters[i].getFieldOrNull(row);
        else {
          input.setBuffer(row.getBinary(i));
          value = serializers[i].deserialize(input);
        }
      }
      result.setField(i, value);
    }
    return result;
  }
}

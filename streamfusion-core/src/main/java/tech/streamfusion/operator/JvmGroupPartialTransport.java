package tech.streamfusion.operator;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;

/** Private Arrow partials retain the actual released local accumulator serializer. */
public final class JvmGroupPartialTransport implements Serializable {
  private static final long serialVersionUID = 1L;
  private final TypeSerializer<RowData> serializer;
  private final RowType originalType;
  private final RowType transportType;
  private final int keyFields;

  public JvmGroupPartialTransport(
      TypeSerializer<RowData> serializer, RowType originalType, int keyFields) {
    if (keyFields < 0 || keyFields >= originalType.getFieldCount())
      throw new IllegalArgumentException("A partial requires its stock keys and accumulator");
    this.serializer = serializer.duplicate();
    this.originalType = originalType;
    this.keyFields = keyFields;
    var fields =
        originalType
            .getChildren()
            .subList(0, keyFields)
            .toArray(new org.apache.flink.table.types.logical.LogicalType[0]);
    fields = Arrays.copyOf(fields, keyFields + 1);
    fields[keyFields] = new VarBinaryType(false, VarBinaryType.MAX_LENGTH);
    transportType = RowType.of(fields);
  }

  public RowType type() {
    return transportType;
  }

  RowData encode(RowData partial) throws Exception {
    var out = new DataOutputSerializer(128);
    serializer.serialize(partial, out);
    var row = keys(partial, originalType);
    var transport = new GenericRowData(partial.getRowKind(), keyFields + 1);
    for (int i = 0; i < keyFields; i++) transport.setField(i, row.getField(i));
    transport.setField(keyFields, out.getCopyOfBuffer());
    return transport;
  }

  RowData decode(RowData transport) throws Exception {
    var input = new DataInputDeserializer(transport.getBinary(keyFields));
    var partial = serializer.deserialize(input);
    if (input.available() != 0) throw new IllegalStateException("Trailing stock partial bytes");
    var keyType =
        RowType.of(
            originalType
                .getChildren()
                .subList(0, keyFields)
                .toArray(new org.apache.flink.table.types.logical.LogicalType[0]));
    var keys = new RowDataSerializer(keyType);
    if (!keys.toBinaryRow(keys(partial, originalType))
        .copy()
        .equals(keys.toBinaryRow(keys(transport, transportType))))
      throw new IllegalStateException("Stock partial payload does not match its exchange key");
    return partial;
  }

  private GenericRowData keys(RowData row, RowType type) {
    var key = new GenericRowData(keyFields);
    for (int i = 0; i < keyFields; i++)
      key.setField(i, RowData.createFieldGetter(type.getTypeAt(i), i).getFieldOrNull(row));
    return key;
  }
}

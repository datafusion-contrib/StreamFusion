package tech.streamfusion.operator;

import java.util.Arrays;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/** Uses the released serializer's key layout, matching the native columnar exchange. */
public final class SerializedRowKeySelector implements RowDataKeySelector {
  private final int[] fields;
  private final RowType keyType;
  private transient ProjectedRowData projection;
  private transient RowDataSerializer serializer;

  public SerializedRowKeySelector(RowType inputType, int[] fields) {
    this(
        fields,
        RowType.of(
            Arrays.stream(fields).mapToObj(inputType::getTypeAt).toArray(LogicalType[]::new)));
  }

  private SerializedRowKeySelector(int[] fields, RowType keyType) {
    this.fields = fields.clone();
    this.keyType = keyType;
  }

  @Override
  public RowData getKey(RowData input) {
    if (serializer == null) {
      projection = ProjectedRowData.from(fields);
      serializer = new RowDataSerializer(keyType);
    }
    var key = serializer.toBinaryRow(projection.replaceRow(input));
    key.setRowKind(RowKind.INSERT);
    return key.copy();
  }

  @Override
  public InternalTypeInfo<RowData> getProducedType() {
    return InternalTypeInfo.of(keyType);
  }

  // Flink 2.2 requests independent selector instances; 1.18 has no copy method in its interface.
  public RowDataKeySelector copy() {
    return new SerializedRowKeySelector(fields, keyType);
  }
}

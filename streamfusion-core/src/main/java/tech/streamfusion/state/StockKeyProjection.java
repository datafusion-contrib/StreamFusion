package tech.streamfusion.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

/** Runtime-only equivalent of Flink's generated binary key projection, including NULL slots. */
final class StockKeyProjection {
  private final LogicalType[] types;
  private final RowData.FieldGetter[] getters;
  private final TypeSerializer<?>[] serializers;
  private final BinaryRowData row;
  private final BinaryRowWriter writer;

  StockKeyProjection(RowType type) {
    types = type.getChildren().toArray(new LogicalType[0]);
    getters = new RowData.FieldGetter[types.length];
    serializers = new TypeSerializer<?>[types.length];
    for (int i = 0; i < types.length; i++) {
      getters[i] = RowData.createFieldGetter(types[i], i);
      serializers[i] = InternalSerializers.create(types[i]);
    }
    row = new BinaryRowData(types.length);
    writer = new BinaryRowWriter(row);
  }

  BinaryRowData project(RowData value) {
    if (value.getArity() != types.length)
      throw new IllegalArgumentException("Projected key arity differs from its stock type");
    writer.reset();
    for (int i = 0; i < types.length; i++) {
      if (!value.isNullAt(i)) {
        BinaryWriter.write(writer, i, getters[i].getFieldOrNull(value), types[i], serializers[i]);
      } else {
        switch (types[i].getTypeRoot()) {
          case DECIMAL:
            int decimal = LogicalTypeChecks.getPrecision(types[i]);
            if (!DecimalData.isCompact(decimal)) writer.writeDecimal(i, null, decimal);
            else writer.setNullAt(i);
            break;
          case TIMESTAMP_WITHOUT_TIME_ZONE:
          case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            int timestamp = LogicalTypeChecks.getPrecision(types[i]);
            if (!TimestampData.isCompact(timestamp)) writer.writeTimestamp(i, null, timestamp);
            else writer.setNullAt(i);
            break;
          default:
            writer.setNullAt(i);
        }
      }
    }
    writer.complete();
    return row.copy();
  }
}

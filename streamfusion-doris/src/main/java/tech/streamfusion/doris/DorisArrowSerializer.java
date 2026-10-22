package tech.streamfusion.doris;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.TimestampAccessor;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Consumes native vectors, with columnar timestamp normalization and an optional delete sign. */
public final class DorisArrowSerializer implements DorisRecordSerializer<ArrowBatch> {
  private static final long serialVersionUID = 1L;
  private final RowType type;
  private final boolean deletable;

  public DorisArrowSerializer(RowType type) {
    this(type, false);
  }

  public DorisArrowSerializer(RowType type, boolean deletable) {
    this.type = type;
    this.deletable = deletable;
  }

  @Override
  public DorisRecord serialize(ArrowBatch batch) throws IOException {
    try (VectorSchemaRoot root = batch.root()) {
      if (root.getRowCount() == 0) return DorisRecord.empty;
      if (root.getFieldVectors().size() < type.getFieldCount())
        throw new IOException("Doris Arrow sink received an unexpected column count");
      List<Field> fields = new ArrayList<>();
      var logical = ArrowConversion.toArrowSchema(type).getFields();
      for (int i = 0; i < logical.size(); i++)
        fields.add(wireField(logical.get(i), type.getTypeAt(i)));
      if (deletable)
        fields.add(Field.nullable("__DORIS_DELETE_SIGN__", new ArrowType.Int(8, true)));
      var allocator = root.getVector(0).getAllocator();
      try (var wire = VectorSchemaRoot.create(new Schema(fields), allocator)) {
        boolean representations =
            root.getSchema().getFields().stream()
                .limit(type.getFieldCount())
                .anyMatch(DorisArrowSerializer::representation);
        if (representations) {
          var reader = ArrowConversion.createArrowReader(root, type);
          var writer = ArrowConversion.createRowDataArrowWriter(wire, type);
          for (int i = 0; i < root.getRowCount(); i++) writer.write(reader.read(i));
          writer.finish();
        } else {
          for (int i = 0; i < type.getFieldCount(); i++)
            transfer(root.getVector(i), wire.getVector(i));
        }
        if (deletable) {
          var signs = (TinyIntVector) wire.getVector(type.getFieldCount());
          var kinds = (TinyIntVector) root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
          signs.allocateNew(root.getRowCount());
          for (int i = 0; i < root.getRowCount(); i++) {
            RowKind kind = kinds == null ? RowKind.INSERT : RowKind.fromByteValue(kinds.get(i));
            signs.set(i, kind == RowKind.DELETE || kind == RowKind.UPDATE_BEFORE ? 1 : 0);
          }
        }
        wire.setRowCount(root.getRowCount());
        var bytes = new ByteArrayOutputStream();
        try (var writer = new ArrowStreamWriter(wire, null, bytes)) {
          writer.start();
          writer.writeBatch();
          writer.end();
        }
        return DorisRecord.of(bytes.toByteArray());
      }
    }
  }

  private static boolean representation(Field field) {
    return tech.streamfusion.arrow.NestedRepresentation.isString(field)
        || tech.streamfusion.arrow.NestedRepresentation.isElement(field)
        || tech.streamfusion.arrow.MapListRepresentation.isMapList(field)
        || field.getChildren().stream().anyMatch(DorisArrowSerializer::representation);
  }

  private static Field wireField(
      Field field, org.apache.flink.table.types.logical.LogicalType type) {
    if (TimestampAccessor.isComponentTimestamp(field)) {
      int precision =
          org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision(type);
      TimeUnit unit =
          precision <= 3
              ? TimeUnit.MILLISECOND
              : precision <= 6 ? TimeUnit.MICROSECOND : TimeUnit.NANOSECOND;
      return new Field(
          field.getName(),
          new FieldType(
              field.isNullable(),
              new ArrowType.Timestamp(
                  unit,
                  type.getTypeRoot()
                          == org.apache.flink.table.types.logical.LogicalTypeRoot
                              .TIMESTAMP_WITH_LOCAL_TIME_ZONE
                      ? "UTC"
                      : null),
              null),
          null);
    }
    var data = DorisArrowBuffers.logicalFields(field.getChildren());
    List<Field> children = new ArrayList<>();
    if (field.getType() instanceof ArrowType.Map) {
      var entry = data.get(0);
      List<Field> values = new ArrayList<>();
      for (int i = 0; i < entry.getChildren().size(); i++)
        values.add(wireField(entry.getChildren().get(i), type.getChildren().get(i)));
      children.add(new Field(entry.getName(), entry.getFieldType(), values));
    } else {
      for (int i = 0; i < data.size(); i++)
        children.add(wireField(data.get(i), type.getChildren().get(i)));
    }
    return new Field(field.getName(), field.getFieldType(), children);
  }

  private static void transfer(FieldVector from, FieldVector to) throws IOException {
    if (to instanceof TimeStampVector && TimestampAccessor.isTimestamp(from)) {
      var output = (TimeStampVector) to;
      output.allocateNew(from.getValueCount());
      var values = new TimestampAccessor(from);
      for (int i = 0; i < from.getValueCount(); i++) {
        if (from.isNull(i)) output.setNull(i);
        else {
          var timestamp = values.getTimestamp(i);
          TimeUnit unit = ((ArrowType.Timestamp) to.getField().getType()).getUnit();
          long value;
          if (unit == TimeUnit.MILLISECOND) value = timestamp.getMillisecond();
          else if (unit == TimeUnit.MICROSECOND)
            value =
                Math.addExact(
                    Math.multiplyExact(timestamp.getMillisecond(), 1000),
                    timestamp.getNanoOfMillisecond() / 1000);
          else
            value =
                Math.addExact(
                    Math.multiplyExact(timestamp.getMillisecond(), 1_000_000),
                    timestamp.getNanoOfMillisecond());
          output.setSafe(i, value);
        }
      }
      output.setValueCount(from.getValueCount());
      return;
    }
    if (!from.getField().getType().equals(to.getField().getType())
        || DorisArrowBuffers.logicalFields(from.getField().getChildren()).size()
            != to.getChildrenFromFields().size())
      throw new IOException("Doris Arrow sink received an incompatible column: " + from.getField());
    if (from.getChildrenFromFields().isEmpty()) {
      from.makeTransferPair(to).splitAndTransfer(0, from.getValueCount());
      return;
    }
    to.loadFieldBuffers(
        new ArrowFieldNode(from.getValueCount(), from.getNullCount()), from.getFieldBuffers());
    for (int i = 0; i < to.getChildrenFromFields().size(); i++)
      transfer(from.getChildrenFromFields().get(i), to.getChildrenFromFields().get(i));
  }
}

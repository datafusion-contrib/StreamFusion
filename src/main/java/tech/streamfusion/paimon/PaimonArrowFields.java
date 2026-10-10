package tech.streamfusion.paimon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import tech.streamfusion.arrow.TimestampAccessor;

/**
 * Carries the parts of a Paimon schema that Arrow does not model onto the Arrow fields the native
 * encoder reads: Paimon's Parquet field ids, including the ids it derives for list elements and map
 * keys and values, the per-column timestamp unit its precision selects, and the instant semantics
 * of a local-zoned timestamp, which the encoder writes as adjusted to UTC. The encoder's Paimon
 * schema shape turns these into the same descriptor Paimon's own writer produces. Columns bind by
 * position and take the table's names and nullability, as Paimon's own writer describes a file by
 * the table type: the planner has matched the query's types, but the query may have aliased the
 * columns or lost a NOT NULL through a cast.
 */
final class PaimonArrowFields {
  static final String FIELD_ID_KEY = "PARQUET:field_id";
  static final String TIMESTAMP_UNIT_KEY = "streamfusion:timestamp_unit";
  private static final int MAX_NATIVE_TIMESTAMP_PRECISION = 6;

  private PaimonArrowFields() {}

  static Schema annotate(Schema arrow, RowType paimon) {
    List<Field> fields = new ArrayList<>();
    List<DataField> dataFields = paimon.getFields();
    if (arrow.getFields().size() != dataFields.size()) {
      throw new IllegalArgumentException(
          "Arrow batch has " + arrow.getFields().size() + " columns for " + dataFields.size()
              + " Paimon fields");
    }
    for (int i = 0; i < dataFields.size(); i++) {
      fields.add(annotate(arrow.getFields().get(i), dataFields.get(i)));
    }
    return new Schema(fields, arrow.getCustomMetadata());
  }

  private static Field annotate(Field arrow, DataField paimon) {
    return annotate(arrow, paimon.name(), paimon.type(), paimon.id(), 0);
  }

  private static Field annotate(Field arrow, String name, DataType type, int fieldId, int depth) {
    Map<String, String> metadata = new HashMap<>();
    if (arrow.getMetadata() != null) {
      metadata.putAll(arrow.getMetadata());
    }
    metadata.put(FIELD_ID_KEY, Integer.toString(fieldId));
    Integer precision = timestampPrecision(type);
    if (precision != null) {
      metadata.put(TIMESTAMP_UNIT_KEY, precision <= 3 ? "millis" : "micros");
    }
    List<Field> children = arrow.getChildren();
    switch (type.getTypeRoot()) {
      case ARRAY:
        {
          {
        DataType element = ((ArrayType) type).getElementType();
        children =
            List.of(
                annotate(
                    children.get(0),
                    children.get(0).getName(),
                    element,
                    SpecialFields.getArrayElementFieldId(fieldId, depth + 1),
                    depth + 1));
      }
          break;
        }
      case MAP:
        {
          {
        MapType map = (MapType) type;
        Field entries = children.get(0);
        children =
            List.of(
                new Field(
                    entries.getName(),
                    entries.getFieldType(),
                    List.of(
                        annotate(
                            entries.getChildren().get(0),
                            entries.getChildren().get(0).getName(),
                            map.getKeyType().copy(false),
                            SpecialFields.getMapKeyFieldId(fieldId, depth + 1),
                            depth + 1),
                        annotate(
                            entries.getChildren().get(1),
                            entries.getChildren().get(1).getName(),
                            map.getValueType(),
                            SpecialFields.getMapValueFieldId(fieldId, depth + 1),
                            depth + 1))));
      }
          break;
        }
      case ROW:
        {
          {
        List<DataField> nested = ((RowType) type).getFields();
        List<Field> annotated = new ArrayList<>();
        for (int i = 0; i < nested.size(); i++) {
          annotated.add(annotate(children.get(i), nested.get(i)));
        }
        children = annotated;
      }
          break;
        }
      default:
        {
          {
          }
          break;
        }
    }
    FieldType fieldType = arrow.getFieldType();
    ArrowType arrowType = fieldType.getType();
    Field result = new Field(
        name, new FieldType(type.isNullable(), arrowType, fieldType.getDictionary(), metadata), children);
    return precision == null ? result : TimestampAccessor.withTimezone(result,
        type instanceof LocalZonedTimestampType ? "UTC" : null);
  }

  @Nullable
  private static Integer timestampPrecision(DataType type) {
    if (type instanceof TimestampType) {
      return ((TimestampType) type).getPrecision();
    }
    if (type instanceof LocalZonedTimestampType) {
      return ((LocalZonedTimestampType) type).getPrecision();
    }
    return null;
  }

  /** Why the native encoder cannot write this row type, or null when every column is supported. */
  @Nullable
  static String unsupportedTypeReason(RowType type) {
    for (DataField field : type.getFields()) {
      String reason = unsupportedTypeReason(field.type());
      if (reason != null) {
        return "Paimon column " + field.name() + ": " + reason;
      }
    }
    return null;
  }

  @Nullable
  private static String unsupportedTypeReason(DataType type) {
    Integer precision = timestampPrecision(type);
    if (precision != null) {
      return precision > MAX_NATIVE_TIMESTAMP_PRECISION
          ? "timestamp precision "
              + precision
              + " exceeds the native Paimon limit of 6 (Parquet uses INT96)"
          : null;
    }
    DataTypeRoot root = type.getTypeRoot();
    switch (root) {
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
      case DATE:
        return null;
      case ARRAY:
        return unsupportedTypeReason(((ArrayType) type).getElementType());
      case MAP:
        {
          {
        MapType map = (MapType) type;
        String key = unsupportedTypeReason(map.getKeyType());
            return key != null ? key : unsupportedTypeReason(map.getValueType());
      }
        }
      case ROW:
        return unsupportedTypeReason((RowType) type);
      default:
        return "type " + type + " is not verified by the native writer";
    }
  }
}

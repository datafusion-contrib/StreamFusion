package tech.streamfusion.hudi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.io.storage.row.parquet.ParquetSchemaConverter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.parquet.avro.HoodieAvroParquetSchemaConverter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.ArrowFileBatch;
import tech.streamfusion.arrow.TimestampAccessor;

/** Types shared by the Arrow reader and Rust Parquet writer. */
final class HudiTypes {
  private HudiTypes() {}

  static Schema encoderSchema(RowType type) {
    var arrow = ArrowFileBatch.schema(ArrowConversion.toArrowSchema(type));
    var parquet = ParquetSchemaConverter.convertToParquetMessageType("hudi", type);
    var fields = new ArrayList<Field>();
    for (var field : arrow.getFields())
      fields.add(encoding(field, parquet.getType(field.getName())));
    return new Schema(fields);
  }

  private static Field encoding(Field field, org.apache.parquet.schema.Type parquet) {
    var metadata = new HashMap<>(field.getMetadata());
    var children = new ArrayList<>(field.getChildren());
    if (TimestampAccessor.isComponentTimestamp(field)) {
      if (parquet.asPrimitiveType().getPrimitiveTypeName()
          == PrimitiveType.PrimitiveTypeName.INT96) {
        metadata.put("streamfusion:parquet_int96", "true");
      } else {
        var annotation =
            (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation)
                parquet.getLogicalTypeAnnotation();
        metadata.put(
            "streamfusion:timestamp_unit",
            annotation.getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS ? "millis" : "micros");
        field = TimestampAccessor.withTimezone(field, annotation.isAdjustedToUTC() ? "UTC" : null);
        children = new ArrayList<>(field.getChildren());
      }
    } else if (field.getType() instanceof ArrowType.List) {
      children.set(
          0,
          encoding(
              children.get(0),
              ParquetSchemaConverter.parquetListElementType(parquet.asGroupType())));
    } else if (field.getType() instanceof ArrowType.Map) {
      var types = ParquetSchemaConverter.parquetMapKeyValueType(parquet.asGroupType());
      var entries = children.get(0);
      children.set(
          0,
          new Field(
              entries.getName(),
              entries.getFieldType(),
              List.of(
                  encoding(entries.getChildren().get(0), types.getLeft()),
                  encoding(entries.getChildren().get(1), types.getRight()))));
    } else if (field.getType() instanceof ArrowType.Struct) {
      for (int i = 0; i < children.size(); i++)
        children.set(
            i, encoding(children.get(i), parquet.asGroupType().getType(children.get(i).getName())));
    }
    return new Field(
        field.getName(),
        new FieldType(
            field.isNullable(), field.getType(), field.getFieldType().getDictionary(), metadata),
        children);
  }

  static RowType fileSchema(HoodieStorage storage, StoragePath path) {
    try {
      var configuration = new Configuration(storage.getConf().unwrapAs(Configuration.class));
      configuration.setBoolean("parquet.avro.add-list-element-records", false);
      var schema =
          HoodieAvroParquetSchemaConverter.getAvroSchemaConverter(configuration)
              .convert(new ParquetUtils().readMessageType(storage, path));
      return HoodieSchemaConverter.convertToRowType(schema);
    } catch (RuntimeException failure) {
      throw new IllegalArgumentException("Cannot verify Hudi Parquet schema: " + path, failure);
    }
  }

  static boolean compatible(LogicalType requested, LogicalType stored) {
    if (requested.getTypeRoot() == LogicalTypeRoot.BINARY
        && stored.getTypeRoot() == LogicalTypeRoot.VARBINARY) return true;
    if (requested.getTypeRoot() != stored.getTypeRoot()) return false;
    if (requested instanceof VarCharType || requested instanceof VarBinaryType) return true;
    if (requested instanceof TimeType)
      return ((TimeType) requested).getPrecision() <= 3 && ((TimeType) stored).getPrecision() <= 3;
    if (requested.getChildren().isEmpty()) return requested.copy(true).equals(stored.copy(true));
    if (requested instanceof RowType
        && !((RowType) requested).getFieldNames().equals(((RowType) stored).getFieldNames()))
      return false;
    var left = requested.getChildren();
    var right = stored.getChildren();
    if (left.size() != right.size()) return false;
    for (int i = 0; i < left.size(); i++) if (!compatible(left.get(i), right.get(i))) return false;
    return true;
  }

  static void validate(LogicalType type, boolean nested) {
    switch (type.getTypeRoot()) {
      case BOOLEAN:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
      case VARCHAR:
      case VARBINARY:
      case DATE:
        return;
      case BINARY:
        if (ArrowConversion.binaryEltEnabled() || ArrowConversion.nestedScalarUdfsEnabled()) return;
        break;
      case TIME_WITHOUT_TIME_ZONE:
        if (((TimeType) type).getPrecision() <= 3) return;
        break;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        int precision = LogicalTypeChecks.getPrecision(type);
        if (precision == 3 || precision == 6) return;
        break;
      case ROW:
      case ARRAY:
      case MAP:
        if (nested) {
          for (var child : type.getChildren()) validate(child, true);
          return;
        }
        break;
      default:
        break;
    }
    throw new IllegalArgumentException("Hudi Arrow type is not admitted: " + type);
  }
}

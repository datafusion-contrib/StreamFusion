package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.flink.connector.datagen.table.DataGenTableSource;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalBoundedStreamScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDataStreamScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

/** String representation guarantees for the selected Flink row producer. */
public final class HostStringInputs {
  private HostStringInputs() {}

  /** Whether this producer guarantees that all of its string bytes can enter Arrow Utf8. */
  public static boolean guaranteesUtf8(RelNode input) {
    if (!JsonStringIdentity.containsCharacter(input.getRowType())) return true;
    if (areJavaBacked(input) || input instanceof Values) return true;
    if (input instanceof TableScan) {
      var table = ((TableScan) input).getTable().unwrap(TableSourceTable.class);
      return table != null && guaranteesUtf8(table.tableSource());
    }
    if (input instanceof Calc) {
      var program = ((Calc) input).getProgram();
      return guaranteesUtf8(input.getInput(0))
          && program.getProjectList().stream()
              .map(program::expandLocalRef)
              .allMatch(HostStringInputs::preservesUtf8);
    }
    return (input instanceof Filter
            || input instanceof Sort
            || input instanceof Exchange
            || input instanceof Union)
        && input.getInputs().stream().allMatch(HostStringInputs::guaranteesUtf8);
  }

  /** Whether this stock projection preserves the UTF-8 contract of its inputs. */
  public static boolean preservesUtf8(RexNode expression) {
    if (!JsonStringIdentity.containsCharacter(expression.getType())
        || expression instanceof RexInputRef
        || expression instanceof RexLiteral) return true;
    if (expression instanceof RexFieldAccess)
      return preservesUtf8(((RexFieldAccess) expression).getReferenceExpr());
    if (!(expression instanceof RexCall)) return false;
    var call = (RexCall) expression;
    switch (call.getKind()) {
      case ROW:
      case ARRAY_VALUE_CONSTRUCTOR:
      case MAP_VALUE_CONSTRUCTOR:
        break;
      case CAST:
        var input = call.getOperands().get(0).getType();
        if (!(input.getSqlTypeName() == SqlTypeName.NULL
            || SqlTypeUtil.sameNamedType(input, call.getType())
            || SqlTypeFamily.CHARACTER.contains(call.getType())
                && SqlTypeFamily.NUMERIC.contains(input))) return false;
        break;
      default:
        var operator = call.getOperator();
        if (operator != SqlStdOperatorTable.UPPER && operator != SqlStdOperatorTable.LOWER)
          return false;
    }
    return call.getOperands().stream().allMatch(HostStringInputs::preservesUtf8);
  }

  /** Whether a stock dynamic source emits verified string bytes. */
  public static boolean guaranteesUtf8(DynamicTableSource source) {
    // The stock datagen's character generator creates a Java string of hexadecimal digits.
    return source.getClass() == DataGenTableSource.class
        || source instanceof Utf8StringSource && ((Utf8StringSource) source).guaranteesUtf8Strings()
        || filesystemCsv(source);
  }

  private static boolean filesystemCsv(DynamicTableSource source) {
    var type = source.getClass();
    if (!type.getName().equals("org.apache.flink.connector.file.table.FileSystemTableSource"))
      return false;
    // The released CSV decoders build StringData from Jackson's Java text. Inspect the selected
    // decoder because a catalog or module can supply a factory with the same identifier.
    try {
      var bulk = readField(source, "bulkReaderFormat");
      if (bulk != null) {
        return bulk.getClass().getName().equals(
            "org.apache.flink.formats.csv.CsvFileFormatFactory$CsvBulkDecodingFormat");
      }
      var decoder = readField(source, "deserializationFormat");
      return decoder != null
          && decoder.getClass().getName().equals("org.apache.flink.formats.csv.CsvFormatFactory$1");
    } catch (ReflectiveOperationException | RuntimeException unavailable) {
      return false;
    }
  }

  static boolean areJavaBacked(RelNode input) {
    if (input instanceof BatchPhysicalBoundedStreamScan) {
      var scan = (BatchPhysicalBoundedStreamScan) input;
      return externalConversionGuaranteesUtf8(scan.boundedStreamTable().dataType());
    }
    if (input instanceof StreamPhysicalDataStreamScan) {
      var scan = (StreamPhysicalDataStreamScan) input;
      return externalConversionGuaranteesUtf8(scan.dataStreamTable().dataType());
    }
    if (!(input instanceof TableScan)) return false;
    var scan = (TableScan) input;
    TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
    if (table == null) return false;
    Object source = table.tableSource();
    if (!source.getClass().getName().equals(
        "org.apache.flink.table.planner.connectors.ExternalDynamicSource")) return false;
    try {
      // ExternalDynamicSource is package-private and exposes no physical-conversion accessor.
      return externalConversionGuaranteesUtf8((DataType) readField(source, "physicalDataType"));
    } catch (ReflectiveOperationException | RuntimeException unavailable) {
      return false;
    }
  }

  private static Object readField(Object object, String name) throws ReflectiveOperationException {
    var field = object.getClass().getDeclaredField(name);
    if (!field.trySetAccessible()) throw new IllegalAccessException(name);
    return field.get(object);
  }

  /** Whether Flink's external-to-internal converter encodes every character field from Java text. */
  public static boolean externalConversionGuaranteesUtf8(DataType type) {
    if (org.apache.flink.table.data.RowData.class.isAssignableFrom(type.getConversionClass())
        || org.apache.flink.table.data.ArrayData.class.isAssignableFrom(type.getConversionClass())
        || org.apache.flink.table.data.MapData.class.isAssignableFrom(type.getConversionClass())) {
      return false;
    }
    if (type.getLogicalType().is(LogicalTypeFamily.CHARACTER_STRING)) {
      return type.getConversionClass() == String.class;
    }
    return type.getChildren().stream().allMatch(HostStringInputs::externalConversionGuaranteesUtf8);
  }
}

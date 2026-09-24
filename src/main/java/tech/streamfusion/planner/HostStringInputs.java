package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDataStreamScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

/** Proof for functions whose Flink behavior depends on Java-backed versus binary-backed strings. */
final class HostStringInputs {
  private HostStringInputs() {}

  static boolean areJavaBacked(RelNode input) {
    if (input instanceof StreamPhysicalDataStreamScan scan) {
      return hasExternalStrings(scan.dataStreamTable().dataType());
    }
    if (!(input instanceof TableScan scan)) return false;
    TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
    if (table == null) return false;
    Object source = table.tableSource();
    if (!source.getClass().getName().equals(
        "org.apache.flink.table.planner.connectors.ExternalDynamicSource")) return false;
    try {
      // ExternalDynamicSource is package-private and exposes no physical-conversion accessor.
      var field = source.getClass().getDeclaredField("physicalDataType");
      if (!field.trySetAccessible()) return false;
      return hasExternalStrings((DataType) field.get(source));
    } catch (ReflectiveOperationException | RuntimeException unavailable) {
      return false;
    }
  }

  private static boolean hasExternalStrings(DataType type) {
    if (org.apache.flink.table.data.RowData.class.isAssignableFrom(type.getConversionClass())
        || org.apache.flink.table.data.ArrayData.class.isAssignableFrom(type.getConversionClass())
        || org.apache.flink.table.data.MapData.class.isAssignableFrom(type.getConversionClass())) {
      return false;
    }
    if (type.getLogicalType().is(LogicalTypeFamily.CHARACTER_STRING)) {
      return type.getConversionClass() == String.class;
    }
    return type.getChildren().stream().allMatch(HostStringInputs::hasExternalStrings);
  }
}

package tech.streamfusion.format;

import java.util.stream.Collectors;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Renders one logical type per sink column as the {@code streamfusion.flink.logical-type} sidecar
 * the native encoder walks in lockstep with the Arrow type tree. Containers use a name-free
 * grammar ({@code ROW<...>}, {@code ARRAY<...>}, {@code MAP<key,value>}) so the descriptor parses
 * unambiguously — Flink's own summary strings interleave arbitrary user field names. Scalars keep
 * their Flink summary spelling, which carries the one distinction Arrow's own types drop: a
 * TIMESTAMP_LTZ leaf and a plain TIMESTAMP both arrive as timezone-less nanoseconds. A MULTISET is
 * rendered as the {@code MAP<element,INT>} it crosses the boundary as.
 */
public final class LogicalTypeDescriptors {

  private LogicalTypeDescriptors() {}

  public static String[] of(RowType rowType) {
    return rowType.getChildren().stream()
        .map(LogicalTypeDescriptors::descriptor)
        .toArray(String[]::new);
  }

  public static String descriptor(LogicalType type) {
    switch (type.getTypeRoot()) {
      case ROW:
        return ((RowType) type)
            .getFields().stream()
                .map(field -> descriptor(field.getType()))
                .collect(Collectors.joining(",", "ROW<", ">"));
      case ARRAY:
        return "ARRAY<" + descriptor(((ArrayType) type).getElementType()) + ">";
      case MAP:
        MapType map = (MapType) type;
        return "MAP<" + descriptor(map.getKeyType()) + "," + descriptor(map.getValueType()) + ">";
      case MULTISET:
        return "MAP<" + descriptor(((MultisetType) type).getElementType()) + ",INT>";
      default:
        return type.asSummaryString();
    }
  }
}

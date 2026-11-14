package tech.streamfusion.state;

import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;

/** Keeps the stock binary representation of ROW-bearing keys in every transfer direction. */
final class StockKeyedRowSchema {
  private StockKeyedRowSchema() {}

  static boolean containsRow(LogicalType type) {
    return type.is(LogicalTypeRoot.ROW)
        || type.getChildren().stream().anyMatch(StockKeyedRowSchema::containsRow);
  }

  static Schema schema(RowType row, RowType keys) {
    return keys.getChildren().stream().anyMatch(StockKeyedRowSchema::containsRow)
        ? ArrowConversion.toBinaryRowKeySchema(row)
        : ArrowConversion.toArrowSchema(row);
  }

  static VectorSchemaRoot write(List<RowData> rows, RowType row, RowType keys) {
    return write(rows, row, schema(row, keys));
  }

  static VectorSchemaRoot write(List<RowData> rows, RowType row, Schema schema) {
    var root = VectorSchemaRoot.create(schema, NativeAllocator.SHARED);
    try {
      var writer = ArrowConversion.createRowDataArrowWriter(root, row);
      for (var value : rows) writer.write(value);
      writer.finish();
      return root;
    } catch (RuntimeException | Error failure) {
      root.close();
      throw failure;
    }
  }
}

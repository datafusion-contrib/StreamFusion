package tech.streamfusion.planner;

import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;

/** Probes retained Arrow buffer ownership and availability of the bounded native engine. */
final class BatchSortTypeCheck {
  private BatchSortTypeCheck() {}

  static boolean supported(RelDataType rowType) {
    return supported(
        ArrowConversion.toArrowInputSchema(
            FlinkTypeFactory$.MODULE$.toLogicalRowType(rowType), false));
  }

  static boolean keysSupported(RelDataType rowType, int[] columns, int[] flags) {
    var schema =
        ArrowConversion.toArrowInputSchema(
            FlinkTypeFactory$.MODULE$.toLogicalRowType(rowType), false);
    try (var allocator = new RootAllocator();
        var exported = ArrowSchema.allocateNew(allocator)) {
      Data.exportSchema(allocator, schema, null, exported);
      try {
        return Native.supportsBoundedSorterKeys(exported.memoryAddress(), columns, flags);
      } finally {
        exported.release();
      }
    }
  }

  static boolean supported(Schema schema) {
    try (var allocator = new RootAllocator();
        var exported = ArrowSchema.allocateNew(allocator)) {
      Data.exportSchema(allocator, schema, null, exported);
      try {
        return Native.supportsBoundedSorterSchema(exported.memoryAddress());
      } finally {
        exported.release();
      }
    }
  }
}

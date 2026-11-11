package tech.streamfusion.planner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hudi.common.table.log.InstantRange;
import tech.streamfusion.Native;
import tech.streamfusion.format.ColumnarFileCodec;
import tech.streamfusion.operator.NativeAllocator;

/**
 * Translates Java's selected commit range into the existing Rust Calc, dropping its hidden input.
 */
public final class HudiCommitFilter implements AutoCloseable {
  private long handle;

  public HudiCommitFilter(InstantRange range, List<String> output) {
    var types = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    var builder = new RexBuilder(types);
    var commit = builder.makeInputRef(types.createSqlType(SqlTypeName.VARCHAR), output.size());
    var encoded = RexExpression.encode(predicate(builder, commit, range));
    if (encoded == null) throw new IllegalArgumentException("Unsupported Hudi commit range");
    int size = encoded.kinds().length;
    int[] kinds = Arrays.copyOf(encoded.kinds(), size + output.size());
    int[] payload = Arrays.copyOf(encoded.payload(), kinds.length);
    int[] children = Arrays.copyOf(encoded.childCounts(), kinds.length);
    int[] projections = new int[output.size()];
    for (int i = 0; i < projections.length; i++) {
      projections[i] = size + i;
      payload[size + i] = i;
    }
    handle =
        Native.createCalcExpression(
            kinds,
            payload,
            children,
            encoded.longs(),
            encoded.doubles(),
            encoded.strings(),
            projections,
            0,
            output.toArray(new String[0]));
  }

  private static RexNode predicate(RexBuilder builder, RexNode commit, InstantRange range) {
    if (!(range instanceof InstantRange.ExactMatchRange))
      throw new IllegalArgumentException(
          "Hudi continuous reads require Java's selected instant set");
    var terms = new ArrayList<RexNode>();
    for (String instant : ((InstantRange.ExactMatchRange) range).getInstants())
      terms.add(builder.makeCall(SqlStdOperatorTable.EQUALS, commit, builder.makeLiteral(instant)));
    return org.apache.calcite.rex.RexUtil.composeDisjunction(builder, terms);
  }

  /**
   * Consumes the input root; native Calc filters and projects the payload without row conversion.
   */
  public VectorSchemaRoot apply(VectorSchemaRoot input) {
    if (handle == 0) throw new IllegalStateException("Hudi commit filter is closed");
    try (var owned = input;
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      try {
        Data.exportVectorSchemaRoot(
            NativeAllocator.SHARED, input, NativeAllocator.DICTIONARIES, array, schema);
        return ColumnarFileCodec.importBatch(
            (outArray, outSchema) -> {
              Native.calcExpression(
                  handle, array.memoryAddress(), schema.memoryAddress(), outArray, outSchema);
              return true;
            });
      } finally {
        if (array.snapshot().release != 0) array.release();
        if (schema.snapshot().release != 0) schema.release();
      }
    }
  }

  @Override
  public void close() {
    if (handle != 0) {
      Native.closeCalcExpression(handle);
      handle = 0;
    }
  }
}

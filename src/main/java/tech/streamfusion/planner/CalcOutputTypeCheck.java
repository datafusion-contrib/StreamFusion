package tech.streamfusion.planner;

import java.util.List;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.Native;
import tech.streamfusion.NativeException;
import tech.streamfusion.arrow.ArrowConversion;

/**
 * Plan-time agreement between the types the native engine will produce for an encoded Calc and the
 * types the Flink plan declares for it.
 *
 * <p>The encoder admits expressions node by node, but the result type of a tree is decided by
 * DataFusion's coercion rules when the tree is compiled, and those rules do not always agree with
 * Calcite's (single-precision arithmetic against a decimal, for one). The columnar boundary builds
 * the host's column vectors from the Arrow batch it receives, so a disagreement would surface deep
 * inside Flink as a class cast on the first row read. Compiling the trees against the input schema
 * here — types only, no data — turns that into an ordinary planning-time fallback with a reason.
 */
final class CalcOutputTypeCheck {

  private CalcOutputTypeCheck() {}

  /**
   * Why the native output of {@code encoded} would not be read as {@code outputType}, or null when
   * every projection compiles to a type the boundary reads as its declared column and the condition
   * compiles to a boolean.
   */
  static String mismatch(RexExpression encoded, RelDataType inputType, RelDataType outputType) {
    Schema input =
        ArrowConversion.toArrowSchema(FlinkTypeFactory$.MODULE$.toLogicalRowType(inputType));
    RowType declared = FlinkTypeFactory$.MODULE$.toLogicalRowType(outputType);
    try (BufferAllocator allocator = new RootAllocator();
        ArrowSchema inputSchema = ArrowSchema.allocateNew(allocator);
        ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportSchema(allocator, input, null, inputSchema);
      String notInferable =
          Native.inferCalcOutputSchema(
              encoded.kinds(),
              encoded.payload(),
              encoded.childCounts(),
              encoded.longs(),
              encoded.doubles(),
              encoded.strings(),
              encoded.projectionRoots(),
              encoded.conditionRoot(),
              encoded.outputNames(),
              inputSchema.memoryAddress(),
              outputSchema.memoryAddress());
      if (notInferable != null) {
        return notInferable;
      }
      return mismatch(Data.importSchema(allocator, outputSchema, null).getFields(), declared);
    } catch (NativeException compileFailure) {
      return "expression does not compile natively: " + compileFailure.getMessage();
    }
  }

  private static String mismatch(List<Field> inferred, RowType declared) {
    if (inferred.size() != declared.getFieldCount()) {
      return inferred.size() + " projections for " + declared.getFieldCount() + " declared columns";
    }
    for (int i = 0; i < inferred.size(); i++) {
      Field actual = inferred.get(i);
      if (!ArrowConversion.readsAs(actual, declared.getTypeAt(i))) {
        return String.format(
            "projection `%s` evaluates natively as %s but the plan declares %s",
            actual.getName(), actual.getType(), declared.getTypeAt(i));
      }
    }
    return null;
  }
}

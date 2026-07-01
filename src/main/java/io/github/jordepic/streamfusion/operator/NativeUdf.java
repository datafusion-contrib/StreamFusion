package io.github.jordepic.streamfusion.operator;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * The JVM half of native UDF support (the StreamFusion analog of datafusion-comet's {@code
 * CometScalaUDF}/{@code CometUdfBridge}). A Flink {@link ScalarFunction} the native expression engine
 * cannot implement itself is registered here at plan time and invoked from native code per batch: the
 * {@code JvmUdf} native expression node exports its argument columns over the Arrow C Data Interface and
 * upcalls {@link #invokeUdf}, which runs the actual {@code eval} over the whole batch (columnar in and
 * out, one JNI crossing per batch — no per-row boundary) and hands the result column back. Because it
 * runs the same JVM {@code eval} Flink would, the result is byte-identical by construction.
 *
 * <p>The registry is process-global and populated at plan time, so this covers execution that shares the
 * planner's JVM (the local/embedded and benchmark path). Distributed task managers would need the
 * function serialized into the operator instead — a separate step, not wired here.
 */
public final class NativeUdf {

  private NativeUdf() {}

  // Native value-type codes for a UDF argument or result column (mirrored by the JVM encoder in
  // RexExpression). Kept independent of the aggregate value codes — this is the UDF marshalling ABI.
  public static final int TYPE_STRING = 0;
  public static final int TYPE_LONG = 1;
  public static final int TYPE_INT = 2;
  public static final int TYPE_DOUBLE = 3;
  public static final int TYPE_BOOLEAN = 4;
  public static final int TYPE_FLOAT = 5;
  public static final int TYPE_SHORT = 6;
  public static final int TYPE_BYTE = 7;

  private static final class Registered {
    final ScalarFunction function;
    final Method eval;
    final int[] argTypes;
    final int returnType;

    Registered(ScalarFunction function, Method eval, int[] argTypes, int returnType) {
      this.function = function;
      this.eval = eval;
      this.argTypes = argTypes;
      this.returnType = returnType;
    }
  }

  private static final ConcurrentHashMap<Integer, Registered> REGISTRY = new ConcurrentHashMap<>();
  private static final AtomicInteger NEXT_ID = new AtomicInteger();

  /**
   * Registers a scalar function for native invocation and returns its id (baked into the encoded
   * expression). {@code argTypes}/{@code returnType} are the {@code TYPE_*} codes of the operands and
   * result; {@code eval} is the resolved {@code eval} method for that arity.
   */
  public static int register(ScalarFunction function, Method eval, int[] argTypes, int returnType) {
    eval.setAccessible(true);
    int id = NEXT_ID.getAndIncrement();
    REGISTRY.put(id, new Registered(function, eval, argTypes, returnType));
    return id;
  }

  /**
   * Registers a builtin scalar function backed by a {@code static} method (no receiver) for native
   * invocation. Used to route a SQL builtin whose native semantics can't be guaranteed to match the
   * host — e.g. {@code REGEXP_EXTRACT}, whose Rust {@code regex} result may differ from Java's {@code
   * java.util.regex} — through the host's own implementation, keeping it byte-identical while the rest
   * of the expression stays native.
   */
  public static int registerBuiltin(Method staticMethod, int[] argTypes, int returnType) {
    staticMethod.setAccessible(true);
    int id = NEXT_ID.getAndIncrement();
    REGISTRY.put(id, new Registered(null, staticMethod, argTypes, returnType));
    return id;
  }

  /**
   * Invoked from native code (via JNI) once per batch: imports the argument columns from the Rust-owned
   * C Data pointers, runs {@code eval} row by row, and exports the single result column to the output
   * pointers. Called on the task thread that drove the native evaluation, so it is JVM-attached already.
   */
  public static void invokeUdf(
      int id, long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress) {
    Registered udf = REGISTRY.get(id);
    if (udf == null) {
      throw new IllegalStateException("no registered UDF for id " + id);
    }
    try (ArrowArray inArray = ArrowArray.wrap(inArrayAddress);
        ArrowSchema inSchema = ArrowSchema.wrap(inSchemaAddress);
        VectorSchemaRoot in =
            Data.importVectorSchemaRoot(
                NativeAllocator.SHARED, inArray, inSchema, NativeAllocator.DICTIONARIES)) {
      int rows = in.getRowCount();
      int arity = udf.argTypes.length;
      FieldVector[] argVectors = new FieldVector[arity];
      for (int a = 0; a < arity; a++) {
        argVectors[a] = in.getFieldVectors().get(a);
      }
      try (VectorSchemaRoot out = resultRoot(udf.returnType, rows)) {
        FieldVector result = out.getFieldVectors().get(0);
        Object[] args = new Object[arity];
        for (int row = 0; row < rows; row++) {
          for (int a = 0; a < arity; a++) {
            args[a] = readValue(argVectors[a], udf.argTypes[a], row);
          }
          Object value = udf.eval.invoke(udf.function, args);
          writeValue(result, udf.returnType, row, value);
        }
        out.setRowCount(rows);
        try (ArrowArray outArray = ArrowArray.wrap(outArrayAddress);
            ArrowSchema outSchema = ArrowSchema.wrap(outSchemaAddress)) {
          Data.exportVectorSchemaRoot(
              NativeAllocator.SHARED, out, NativeAllocator.DICTIONARIES, outArray, outSchema);
        }
      }
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("UDF invocation failed", e);
    }
  }

  private static VectorSchemaRoot resultRoot(int returnType, int rows) {
    Field field = new Field("result", FieldType.nullable(arrowType(returnType)), null);
    VectorSchemaRoot root =
        VectorSchemaRoot.create(new Schema(java.util.List.of(field)), NativeAllocator.SHARED);
    root.getFieldVectors().get(0).setInitialCapacity(rows);
    root.allocateNew();
    return root;
  }

  private static ArrowType arrowType(int code) {
    switch (code) {
      case TYPE_STRING:
        return ArrowType.Utf8.INSTANCE;
      case TYPE_LONG:
        return new ArrowType.Int(64, true);
      case TYPE_INT:
        return new ArrowType.Int(32, true);
      case TYPE_SHORT:
        return new ArrowType.Int(16, true);
      case TYPE_BYTE:
        return new ArrowType.Int(8, true);
      case TYPE_DOUBLE:
        return new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
      case TYPE_FLOAT:
        return new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE);
      case TYPE_BOOLEAN:
        return ArrowType.Bool.INSTANCE;
      default:
        throw new IllegalArgumentException("unsupported UDF type code " + code);
    }
  }

  private static Object readValue(FieldVector vector, int code, int row) {
    if (vector.isNull(row)) {
      return null;
    }
    switch (code) {
      case TYPE_STRING:
        return new String(((VarCharVector) vector).get(row), StandardCharsets.UTF_8);
      case TYPE_LONG:
        return ((BigIntVector) vector).get(row);
      case TYPE_INT:
        return ((IntVector) vector).get(row);
      case TYPE_SHORT:
        return ((SmallIntVector) vector).get(row);
      case TYPE_BYTE:
        return ((TinyIntVector) vector).get(row);
      case TYPE_DOUBLE:
        return ((Float8Vector) vector).get(row);
      case TYPE_FLOAT:
        return ((Float4Vector) vector).get(row);
      case TYPE_BOOLEAN:
        return ((BitVector) vector).get(row) != 0;
      default:
        throw new IllegalArgumentException("unsupported UDF type code " + code);
    }
  }

  private static void writeValue(FieldVector vector, int code, int row, Object value) {
    if (value == null) {
      vector.setNull(row);
      return;
    }
    switch (code) {
      case TYPE_STRING:
        ((VarCharVector) vector).setSafe(row, value.toString().getBytes(StandardCharsets.UTF_8));
        break;
      case TYPE_LONG:
        ((BigIntVector) vector).setSafe(row, ((Number) value).longValue());
        break;
      case TYPE_INT:
        ((IntVector) vector).setSafe(row, ((Number) value).intValue());
        break;
      case TYPE_SHORT:
        ((SmallIntVector) vector).setSafe(row, ((Number) value).shortValue());
        break;
      case TYPE_BYTE:
        ((TinyIntVector) vector).setSafe(row, ((Number) value).byteValue());
        break;
      case TYPE_DOUBLE:
        ((Float8Vector) vector).setSafe(row, ((Number) value).doubleValue());
        break;
      case TYPE_FLOAT:
        ((Float4Vector) vector).setSafe(row, ((Number) value).floatValue());
        break;
      case TYPE_BOOLEAN:
        ((BitVector) vector).setSafe(row, ((Boolean) value) ? 1 : 0);
        break;
      default:
        throw new IllegalArgumentException("unsupported UDF type code " + code);
    }
  }
}

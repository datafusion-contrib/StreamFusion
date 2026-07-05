package io.github.jordepic.streamfusion.operator;

import java.io.Serializable;
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
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.binary.BinaryStringData;
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
 * <p>The registry is process-global but populated at operator {@code open()}, not at plan time: the
 * planner encodes a {@link Descriptor} per UDF (the {@code Serializable} {@link ScalarFunction} or a
 * builtin's class + method) into the operator's {@link Binding}, and {@code open()} registers it into
 * <em>this</em> JVM's registry, obtaining a task-local id that patches the encoded call. This works on a
 * distributed task manager (a different JVM from the planner) as well as the local/benchmark path, and
 * {@code close()} unregisters, so the registry doesn't leak across a task's lifetime.
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
  // A TIMESTAMP argument, marshalled to the eval as epoch millis (a boxed Long). Native pins every
  // timestamp column to Timestamp(nanos, no-tz); the upcall reads that instant as millis. Used by the
  // LTZ DATE_FORMAT/EXTRACT parity path — an argument-only code (no UDF returns a timestamp today).
  public static final int TYPE_TIMESTAMP = 8;

  // DECIMAL(p, s) argument/result values, marshalled as BigDecimal. The precision and scale ride in
  // the code itself so one int carries the full type: 1000 + p*100 + s. Used by the host-exact
  // number<->string CAST upcall.
  private static final int DECIMAL_BASE = 1000;

  /** The packed value-type code for a {@code DECIMAL(precision, scale)} argument or result. */
  public static int decimalType(int precision, int scale) {
    return DECIMAL_BASE + precision * 100 + scale;
  }

  private static final class Registered {
    final ScalarFunction function;
    final Method eval;
    final int[] argTypes;
    final int returnType;
    // Per argument, whether the eval method declares Flink's byte-backed string (BinaryStringData):
    // those args are wrapped from the Arrow column's UTF-8 bytes with fromBytes — no UTF-16 decode,
    // no java.lang.String — so byte-level builtins (case folding's ASCII fast path) stay in bytes
    // across the whole upcall. A String parameter keeps the materializing path.
    final boolean[] argAsStringData;

    Registered(ScalarFunction function, Method eval, int[] argTypes, int returnType) {
      this.function = function;
      this.eval = eval;
      this.argTypes = argTypes;
      this.returnType = returnType;
      Class<?>[] params = eval.getParameterTypes();
      this.argAsStringData = new boolean[argTypes.length];
      for (int a = 0; a < argTypes.length && a < params.length; a++) {
        argAsStringData[a] = BinaryStringData.class.isAssignableFrom(params[a]);
      }
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

  /** Removes a registration, freeing its id — called when an operator carrying it closes. */
  public static void unregister(int id) {
    REGISTRY.remove(id);
  }

  /**
   * A serializable description of a UDF to register, carried into the operator so a task manager (a
   * different JVM from the planner) can populate its own registry at {@code open()} — the fix for
   * distributed execution (the plan-time registry only exists in the planner JVM). Holds the Flink
   * {@link ScalarFunction} (which is {@code Serializable}) or, for a builtin, its declaring class; the
   * {@code eval}/static method is re-resolved from the (serializable) name + parameter types on the
   * task, since {@link Method} itself is not serializable.
   */
  public static final class Descriptor implements Serializable {
    private static final long serialVersionUID = 1L;

    private final ScalarFunction function; // null => a builtin static method
    private final Class<?> methodClass; // builtin: the declaring class (null for a user function)
    private final String methodName;
    private final Class<?>[] paramTypes;
    private final int[] argTypes;
    private final int returnType;

    private Descriptor(
        ScalarFunction function,
        Class<?> methodClass,
        String methodName,
        Class<?>[] paramTypes,
        int[] argTypes,
        int returnType) {
      this.function = function;
      this.methodClass = methodClass;
      this.methodName = methodName;
      this.paramTypes = paramTypes;
      this.argTypes = argTypes;
      this.returnType = returnType;
    }

    /** A user {@link ScalarFunction}, invoked via the given {@code eval} overload. */
    public static Descriptor forFunction(
        ScalarFunction function, Method eval, int[] argTypes, int returnType) {
      return new Descriptor(
          function, null, eval.getName(), eval.getParameterTypes(), argTypes, returnType);
    }

    /** A builtin backed by a {@code static} method (e.g. {@code UPPER}, {@code REGEXP_EXTRACT}). */
    public static Descriptor forBuiltin(Method staticMethod, int[] argTypes, int returnType) {
      return new Descriptor(
          null,
          staticMethod.getDeclaringClass(),
          staticMethod.getName(),
          staticMethod.getParameterTypes(),
          argTypes,
          returnType);
    }

    /** Resolves the method on this JVM and registers it, returning the task-local runtime id. */
    int registerLocally() {
      Class<?> owner = function != null ? function.getClass() : methodClass;
      Method method;
      try {
        method = owner.getMethod(methodName, paramTypes);
      } catch (NoSuchMethodException e) {
        throw new IllegalStateException(
            "cannot resolve UDF method " + methodName + " on " + owner.getName(), e);
      }
      return function != null
          ? register(function, method, argTypes, returnType)
          : registerBuiltin(method, argTypes, returnType);
    }
  }

  /**
   * The UDF registrations an encoded expression needs, carried alongside its {@code longs} pool from
   * the planner into the operator. At operator {@code open()} {@link #bind} registers each descriptor
   * into this JVM's registry (obtaining task-local ids) and rewrites the id slots of the encoded
   * {@code longs} — which the planner filled with a descriptor's <em>local index</em> — to those ids,
   * so the compiled {@code JvmUdf} nodes call the right registration on this task. {@link #unbind} at
   * {@code close()} frees them.
   */
  public static final class Binding implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final Binding EMPTY = new Binding(new Descriptor[0], new int[0]);

    private final Descriptor[] descriptors; // indexed by local index
    private final int[] idSlots; // positions in the longs pool holding a local index
    private transient int[] runtimeIds;

    public Binding(Descriptor[] descriptors, int[] idSlots) {
      this.descriptors = descriptors;
      this.idSlots = idSlots;
    }

    /**
     * Registers the carried UDFs on this JVM and returns {@code longs} with each id slot patched from
     * its local index to the task-local runtime id (a copy, leaving the encoded array pristine so a
     * re-open rebinds correctly). Returns {@code longs} unchanged when there are no UDFs.
     */
    public long[] bind(long[] longs) {
      runtimeIds = new int[descriptors.length];
      for (int i = 0; i < descriptors.length; i++) {
        runtimeIds[i] = descriptors[i].registerLocally();
      }
      if (idSlots.length == 0) {
        return longs;
      }
      long[] patched = longs.clone();
      for (int slot : idSlots) {
        patched[slot] = runtimeIds[(int) longs[slot]];
      }
      return patched;
    }

    /** Frees the registrations obtained by {@link #bind}. */
    public void unbind() {
      if (runtimeIds != null) {
        for (int id : runtimeIds) {
          unregister(id);
        }
        runtimeIds = null;
      }
    }
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
        // Each argument column is materialized once with a monomorphic typed loop; reading value
        // by value inside the row loop instead put a megamorphic isNull/type dispatch per (row,
        // arg) on the hot path — 14% of q21's parity run in the vector interface calls alone.
        Object[][] columns = new Object[arity][];
        for (int a = 0; a < arity; a++) {
          columns[a] = readColumn(argVectors[a], udf.argTypes[a], udf.argAsStringData[a], rows);
        }
        Object[] args = new Object[arity];
        for (int row = 0; row < rows; row++) {
          for (int a = 0; a < arity; a++) {
            args[a] = columns[a][row];
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
        if (code >= DECIMAL_BASE) {
          return new ArrowType.Decimal((code - DECIMAL_BASE) / 100, (code - DECIMAL_BASE) % 100, 128);
        }
        throw new IllegalArgumentException("unsupported UDF type code " + code);
    }
  }

  /**
   * Materializes one argument column as boxed values (null for null rows) with a single downcast
   * and a monomorphic per-type loop, so the row loop above does no per-value vector dispatch.
   */
  private static Object[] readColumn(FieldVector vector, int code, boolean asStringData, int rows) {
    Object[] out = new Object[rows];
    switch (code) {
      case TYPE_STRING:
        {
          // A byte-capable eval gets the column's UTF-8 bytes wrapped as BinaryStringData — no
          // UTF-16 decode; only a String-typed eval pays the materialization.
          VarCharVector v = (VarCharVector) vector;
          for (int r = 0; r < rows; r++) {
            if (!v.isNull(r)) {
              out[r] =
                  asStringData
                      ? BinaryStringData.fromBytes(v.get(r))
                      : new String(v.get(r), StandardCharsets.UTF_8);
            }
          }
          return out;
        }
      case TYPE_LONG:
        {
          BigIntVector v = (BigIntVector) vector;
          for (int r = 0; r < rows; r++) {
            if (!v.isNull(r)) {
              out[r] = v.get(r);
            }
          }
          return out;
        }
      case TYPE_INT:
        {
          IntVector v = (IntVector) vector;
          for (int r = 0; r < rows; r++) {
            if (!v.isNull(r)) {
              out[r] = v.get(r);
            }
          }
          return out;
        }
      case TYPE_SHORT:
        {
          SmallIntVector v = (SmallIntVector) vector;
          for (int r = 0; r < rows; r++) {
            if (!v.isNull(r)) {
              out[r] = v.get(r);
            }
          }
          return out;
        }
      case TYPE_BYTE:
        {
          TinyIntVector v = (TinyIntVector) vector;
          for (int r = 0; r < rows; r++) {
            if (!v.isNull(r)) {
              out[r] = v.get(r);
            }
          }
          return out;
        }
      case TYPE_DOUBLE:
        {
          Float8Vector v = (Float8Vector) vector;
          for (int r = 0; r < rows; r++) {
            if (!v.isNull(r)) {
              out[r] = v.get(r);
            }
          }
          return out;
        }
      case TYPE_FLOAT:
        {
          Float4Vector v = (Float4Vector) vector;
          for (int r = 0; r < rows; r++) {
            if (!v.isNull(r)) {
              out[r] = v.get(r);
            }
          }
          return out;
        }
      case TYPE_BOOLEAN:
        {
          BitVector v = (BitVector) vector;
          for (int r = 0; r < rows; r++) {
            if (!v.isNull(r)) {
              out[r] = v.get(r) != 0;
            }
          }
          return out;
        }
      case TYPE_TIMESTAMP:
        // Native pins timestamps to Timestamp(nanos, no-tz); hand the eval epoch millis (its instant).
        if (vector instanceof TimeStampNanoVector) {
          TimeStampNanoVector v = (TimeStampNanoVector) vector;
          for (int r = 0; r < rows; r++) {
            if (!v.isNull(r)) {
              out[r] = v.get(r) / 1_000_000L;
            }
          }
          return out;
        }
        if (vector instanceof TimeStampMilliVector) {
          TimeStampMilliVector v = (TimeStampMilliVector) vector;
          for (int r = 0; r < rows; r++) {
            if (!v.isNull(r)) {
              out[r] = v.get(r);
            }
          }
          return out;
        }
        throw new IllegalArgumentException(
            "unexpected timestamp vector for a TIMESTAMP UDF arg: " + vector.getClass());
      default:
        if (code >= DECIMAL_BASE) {
          org.apache.arrow.vector.DecimalVector v = (org.apache.arrow.vector.DecimalVector) vector;
          for (int r = 0; r < rows; r++) {
            if (!v.isNull(r)) {
              out[r] = v.getObject(r);
            }
          }
          return out;
        }
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
        // A BinaryStringData result hands its UTF-8 bytes straight to the vector (toBytes returns
        // the backing array for an aligned heap string — the common fromBytes/ASCII-fold shape).
        ((VarCharVector) vector)
            .setSafe(
                row,
                value instanceof BinaryStringData
                    ? ((BinaryStringData) value).toBytes()
                    : value.toString().getBytes(StandardCharsets.UTF_8));
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
        if (code >= DECIMAL_BASE) {
          ((org.apache.arrow.vector.DecimalVector) vector).setSafe(row, (java.math.BigDecimal) value);
          break;
        }
        throw new IllegalArgumentException("unsupported UDF type code " + code);
    }
  }
}

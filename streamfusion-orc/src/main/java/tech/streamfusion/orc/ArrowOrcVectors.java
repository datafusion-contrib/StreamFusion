package tech.streamfusion.orc;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import tech.streamfusion.arrow.TimestampAccessor;

/**
 * Copies Arrow columns into either host's Hive vectors, without creating rows or value objects.
 * Paimon relocates Hive's classes. Public vector access is resolved once, while the hot loops use
 * ordinary primitive arrays for both hosts.
 */
final class ArrowOrcVectors {
  private final Column[] columns;
  private final Object batch;
  private final Access access;
  private final int[] projection;

  ArrowOrcVectors(List<Field> fields, Object batch, int[] projection, boolean legacyTimestampLtz) {
    this.batch = batch;
    this.access = ACCESS.get(batch.getClass());
    this.projection = projection.clone();
    Object[] vectors = (Object[]) access.get(batch, "cols");
    if (vectors.length != projection.length) throw new IllegalArgumentException("ORC column count");
    columns = new Column[vectors.length];
    for (int c = 0; c < columns.length; c++)
      columns[c] = new Column(fields.get(projection[c]), vectors[c], legacyTimestampLtz);
  }

  void copy(VectorSchemaRoot input, int start, int count) {
    access.invoke(batch, "reset");
    access.set(batch, "size", count);
    for (int c = 0; c < columns.length; c++)
      columns[c].copy(input.getVector(projection[c]), start, count);
  }

  long estimatedBytes() {
    return Arrays.stream(columns).mapToLong(Column::estimatedBytes).sum();
  }

  private static final ClassValue<Access> ACCESS =
      new ClassValue<>() {
        protected Access computeValue(Class<?> type) {
          return new Access(type);
        }
      };

  private static final class Access {
    private final Map<String, java.lang.reflect.Field> fields = new HashMap<>();
    private final Map<String, java.lang.reflect.Method> methods = new HashMap<>();

    Access(Class<?> type) {
      for (var field : type.getFields()) fields.put(field.getName(), field);
      for (var method : type.getMethods()) methods.put(method.getName(), method);
    }

    Object get(Object value, String name) {
      try {
        return fields.get(name).get(value);
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException(e);
      }
    }

    void set(Object value, String name, Object field) {
      try {
        fields.get(name).set(value, field);
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException(e);
      }
    }

    void invoke(Object value, String method, Object... args) {
      try {
        methods.get(method).invoke(value, args);
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private static final class Column {
    private final ArrowType type;
    private final boolean componentTimestamp;
    private final boolean instantTimestamp;
    private final Column[] children;
    private byte[] bytes = new byte[16];
    private int[] integers = new int[0];
    private short[] shorts = new short[0];
    private final Object output;
    private final Access access;
    private final MethodHandle decimalSetter;
    private long vectorBytes;

    long estimatedBytes() {
      return vectorBytes
          + bytes.length
          + (long) integers.length * 4
          + (long) shorts.length * 2
          + Arrays.stream(children).mapToLong(Column::estimatedBytes).sum();
    }

    private void capacity(int length) {
      if (bytes.length < length)
        bytes = new byte[Math.max(length, bytes.length + bytes.length / 2)];
    }

    Column(Field field, Object output, boolean legacyTimestampLtz) {
      this.output = output;
      access = ACCESS.get(output.getClass());
      type = field.getType();
      componentTimestamp = TimestampAccessor.isComponentTimestamp(field);
      instantTimestamp =
          !legacyTimestampLtz
              && (componentTimestamp
                  ? "UTC"
                      .equals(
                          field
                              .getChildren()
                              .get(0)
                              .getMetadata()
                              .get("streamfusion.timestamp.timezone"))
                  : type instanceof ArrowType.Timestamp
                      && "UTC".equals(((ArrowType.Timestamp) type).getTimezone()));
      List<Field> fields = field.getChildren();
      Object[] nested;
      if (componentTimestamp) {
        nested = new Object[0];
      } else {
        switch (type.getTypeID()) {
          case Struct:
            nested = (Object[]) access.get(output, "fields");
            break;
          case List:
            nested = new Object[] {access.get(output, "child")};
            break;
          case Map:
            nested = new Object[] {access.get(output, "keys"), access.get(output, "values")};
            break;
          default:
            nested = new Object[0];
            break;
        }
      }
      if (type instanceof ArrowType.Map) fields = fields.get(0).getChildren();
      children = new Column[nested.length];
      for (int i = 0; i < nested.length; i++) children[i] = new Column(fields.get(i), nested[i], legacyTimestampLtz);
      if (type instanceof ArrowType.Decimal && !(access.get(output, "vector") instanceof long[])) {
        ArrowType.Decimal decimal = ((ArrowType.Decimal) type);

        var elementType = access.get(output, "vector").getClass().getComponentType();
        try {
          boolean small = decimal.getPrecision() <= 18;
          var signature =
              small
                  ? MethodType.methodType(void.class, long.class, int.class)
                  : MethodType.methodType(
                      void.class, byte[].class, int.class, int.class, int.class);
          var method =
              MethodHandles.publicLookup()
                  .findVirtual(
                      elementType,
                      small ? "setFromLongAndScale" : "setFromBigIntegerBytesAndScale",
                      signature);
          decimalSetter = method.asType(method.type().changeParameterType(0, Object.class));
        } catch (ReflectiveOperationException e) {
          throw new IllegalStateException(e);
        }
      } else decimalSetter = null;
      if (componentTimestamp || type instanceof ArrowType.Timestamp)
        access.invoke(output, "setIsUTC", true);
    }

    void copy(FieldVector input, int start, int count) {
      access.invoke(output, "ensureSize", count, false);
      access.set(output, "isRepeating", false);
      boolean[] isNull = (boolean[]) access.get(output, "isNull");
      boolean noNulls = input.getNullCount() == 0;
      if (noNulls) Arrays.fill(isNull, 0, count, false);
      else {
        noNulls = true;
        var validity = input.getValidityBuffer();
        int bits = 0;
        for (int i = 0; i < count; i++) {
          int row = start + i;
          if (i == 0 || (row & 7) == 0) bits = validity.getByte(row >>> 3);
          isNull[i] = (bits & (1 << (row & 7))) == 0;
          noNulls &= !isNull[i];
        }
      }
      access.set(output, "noNulls", noNulls);
      vectorBytes = (long) isNull.length * 128;
      if (componentTimestamp || type instanceof ArrowType.Timestamp) {
        TimestampAccessor timestamp = new TimestampAccessor(input);
        long[] time = (long[]) access.get(output, "time");
        int[] nanos = (int[]) access.get(output, "nanos");
        for (int i = 0; i < count; i++) {
          if (isNull[i]) continue;
          long millis = timestamp.getMillis(start + i);
          // Flink/Paimon's local timestamp writer passes through java.sql.Timestamp, whose
          // pre-1582 calendar differs from java.time. Instant ORC columns keep epoch millis.
          time[i] = !instantTimestamp && millis < -12_219_292_800_000L
              ? timestamp.getTimestamp(start + i).toTimestamp().getTime() : millis;
          nanos[i] = (int) Math.floorMod(millis, 1000L) * 1_000_000
              + timestamp.getNanoOfMillisecond(start + i);
        }
        return;
      }
      org.apache.arrow.memory.ArrowBuf data;
      switch (type.getTypeID()) {
        case List:
        case Map:
        case Struct:
          data = null;
          break;
        default:
          data = input.getDataBuffer();
          break;
      }
      switch (type.getTypeID()) {
        case Int:
        case Date:
          {
            {
              int width =
                  type instanceof ArrowType.Int ? ((ArrowType.Int) type).getBitWidth() / 8 : 4;
          long[] values = (long[]) access.get(output, "vector");
          if (width == 8) {
            data.nioBuffer((long) start * 8, count * 8)
                .order(ByteOrder.LITTLE_ENDIAN)
                .asLongBuffer()
                .get(values, 0, count);
          } else if (width == 4) {
            if (integers.length < count) integers = new int[count];
            data.nioBuffer((long) start * 4, count * 4)
                .order(ByteOrder.LITTLE_ENDIAN)
                .asIntBuffer()
                .get(integers, 0, count);
            for (int i = 0; i < count; i++) values[i] = integers[i];
          } else if (width == 2) {
            if (shorts.length < count) shorts = new short[count];
            data.nioBuffer((long) start * 2, count * 2)
                .order(ByteOrder.LITTLE_ENDIAN)
                .asShortBuffer()
                .get(shorts, 0, count);
            for (int i = 0; i < count; i++) values[i] = shorts[i];
          } else if (width == 1) {
            capacity(count);
            data.getBytes(start, bytes, 0, count);
            for (int i = 0; i < count; i++) values[i] = bytes[i];
          } else throw new IllegalArgumentException(type.toString());
        }
            break;
          }
        case Bool:
          {
            {
          long[] values = (long[]) access.get(output, "vector");
          int bits = 0;
          for (int i = 0; i < count; i++) {
            int row = start + i;
            if (i == 0 || (row & 7) == 0) bits = data.getByte(row >>> 3);
            values[i] = (bits >>> (row & 7)) & 1;
          }
        }
            break;
          }
        case FloatingPoint:
          {
            {
          double[] values = (double[]) access.get(output, "vector");
          if (((ArrowType.FloatingPoint) type).getPrecision()
              == org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE) {
            data.nioBuffer((long) start * 8, count * 8)
                .order(ByteOrder.LITTLE_ENDIAN)
                .asDoubleBuffer()
                .get(values, 0, count);
          } else {
            for (int i = 0; i < count; i++) values[i] = data.getFloat((long) (start + i) * 4);
          }
        }
            break;
          }
        case FixedSizeBinary:
          {
            {
          int width = ((ArrowType.FixedSizeBinary) type).getByteWidth();
          int length = Math.multiplyExact(count, width);
          capacity(length);
          data.getBytes((long) start * width, bytes, 0, length);
          var references = (byte[][]) access.get(output, "vector");
          var starts = (int[]) access.get(output, "start");
          var lengths = (int[]) access.get(output, "length");
          Arrays.fill(references, 0, count, bytes);
          for (int i = 0; i < count; i++) {
            starts[i] = i * width;
            lengths[i] = width;
          }
        }
            break;
          }
        case Utf8:
        case Binary:
          {
            {
          var offsets = input.getOffsetBuffer();
          int begin = offsets.getInt((long) start * 4);
          int length = offsets.getInt((long) (start + count) * 4) - begin;
          capacity(length);
          data.getBytes(begin, bytes, 0, length);
          var references = (byte[][]) access.get(output, "vector");
          var starts = (int[]) access.get(output, "start");
          var lengths = (int[]) access.get(output, "length");
          Arrays.fill(references, 0, count, bytes);
          int from = begin;
          for (int i = 0; i < count; i++) {
            int to = offsets.getInt((long) (start + i + 1) * 4);
            starts[i] = from - begin;
            lengths[i] = to - from;
            from = to;
          }
        }
            break;
          }
        case Decimal:
          {
            {
          var decimal = (ArrowType.Decimal) type;
          if (decimal.getBitWidth() != 128) throw new IllegalArgumentException(type.toString());
              Object valuesCandidate;
              if ((valuesCandidate = access.get(output, "vector")) instanceof long[]) {
                long[] values = ((long[]) valuesCandidate);

            for (int i = 0; i < count; i++) values[i] = data.getLong((long) (start + i) * 16);
            break;
          }
          Object[] values = (Object[]) access.get(output, "vector");
          for (int i = 0; i < count; i++) {
            if (isNull[i]) continue;
            long at = (long) (start + i) * 16;
            if (decimal.getPrecision() <= 18) {
              try {
                decimalSetter.invokeExact(values[i], data.getLong(at), decimal.getScale());
              } catch (Throwable e) {
                throw new IllegalStateException(e);
              }
            } else {
              for (int b = 0; b < 16; b++) bytes[15 - b] = data.getByte(at + b);
              try {
                decimalSetter.invokeExact(values[i], bytes, 0, 16, decimal.getScale());
              } catch (Throwable e) {
                throw new IllegalStateException(e);
              }
            }
          }
        }
            break;
          }
        case List:
        case Map:
          {
            {
          var list = (ListVector) input;
          var offsets = (long[]) access.get(output, "offsets");
          var lengths = (long[]) access.get(output, "lengths");
          int begin = list.getOffsetBuffer().getInt((long) start * 4);
          int end = list.getOffsetBuffer().getInt((long) (start + count) * 4);
          int from = begin;
          var inputOffsets = list.getOffsetBuffer();
          for (int i = 0; i < count; i++) {
            int to = inputOffsets.getInt((long) (start + i + 1) * 4);
            offsets[i] = from - begin;
            lengths[i] = to - from;
            from = to;
          }
          access.set(output, "childCount", end - begin);
          if (input instanceof MapVector) {
            var entries = (StructVector) list.getDataVector();
            children[0].copy(entries.getChildrenFromFields().get(0), begin, end - begin);
            children[1].copy(entries.getChildrenFromFields().get(1), begin, end - begin);
          } else {
            children[0].copy(list.getDataVector(), begin, end - begin);
          }
        }
            break;
          }
        case Struct:
          {
            {
          var fields = ((StructVector) input).getChildrenFromFields();
          for (int c = 0; c < children.length; c++) children[c].copy(fields.get(c), start, count);
        }
            break;
          }
        default:
          throw new IllegalArgumentException("Unsupported ORC Arrow type: " + type);
      }
    }
  }
}

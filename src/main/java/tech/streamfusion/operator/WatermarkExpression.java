package tech.streamfusion.operator;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.TimestampAccessor;

/**
 * A serializable projection in the shared native expression format, producing epoch milliseconds.
 */
public final class WatermarkExpression implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final int TIMESTAMP_MILLIS = 155;
  public static final int SUBTRACT_MILLIS = 156;
  public static final int SUBTRACT_MONTHS = 157;

  private final int[] kinds;
  private final int[] payload;
  private final int[] childCounts;
  private final long[] longs;
  private final double[] doubles;
  private final String[] strings;
  private final String description;

  public WatermarkExpression(
      int[] kinds,
      int[] payload,
      int[] childCounts,
      long[] longs,
      double[] doubles,
      String[] strings,
      String description) {
    this.kinds = kinds.clone();
    this.payload = payload.clone();
    this.childCounts = childCounts.clone();
    this.longs = longs.clone();
    this.doubles = doubles.clone();
    this.strings = strings.clone();
    this.description = description;
  }

  /** A direct timestamp projection; BIGINT inputs already carry epoch milliseconds. */
  public static WatermarkExpression rowtime(int column) {
    return new WatermarkExpression(
        new int[] {6, 0},
        new int[] {TIMESTAMP_MILLIS, column},
        new int[] {1, 0},
        new long[0],
        new double[0],
        new String[0],
        "epochMillis($" + column + ")");
  }

  public static WatermarkExpression subtractMillis(int column, long millis) {
    return subtract(column, millis, SUBTRACT_MILLIS, 1, "MILLISECONDS");
  }

  public static WatermarkExpression subtractMonths(int column, int months) {
    return subtract(column, months, SUBTRACT_MONTHS, 7, "MONTHS");
  }

  private static WatermarkExpression subtract(
      int column, long amount, int op, int literal, String unit) {
    if (amount < 0) {
      throw new IllegalArgumentException("Watermark interval must be nonnegative");
    }
    return new WatermarkExpression(
        new int[] {6, 0, literal},
        new int[] {op, column, 0},
        new int[] {2, 0, 0},
        new long[] {amount},
        new double[0],
        new String[0],
        "$" + column + " - " + amount + " " + unit);
  }

  /** Remap a source projection without changing the serialized plan shared by other scans. */
  public WatermarkExpression remapInput(int oldIndex, int newIndex) {
    int[] remapped = payload.clone();
    for (int i = 0; i < kinds.length; i++) {
      if (kinds[i] == 0 && payload[i] == oldIndex) {
        remapped[i] = newIndex;
      }
    }
    return new WatermarkExpression(
        kinds, remapped, childCounts, longs, doubles, strings, description);
  }

  /**
   * Structural identity for source sharing, including operand types and projected column indices.
   */
  public String digest() {
    return Arrays.toString(kinds)
        + Arrays.toString(payload)
        + Arrays.toString(childCounts)
        + Arrays.toString(longs)
        + Arrays.toString(doubles)
        + Arrays.toString(strings);
  }

  @Override
  public String toString() {
    return description;
  }

  /** Each operator or split reader owns its evaluator; native handles never enter serialization. */
  public Evaluator open() {
    if (Arrays.equals(kinds, new int[] {6, 0}) && payload[0] == TIMESTAMP_MILLIS) {
      return fixedMillis(payload[1], 0);
    }
    if (Arrays.equals(kinds, new int[] {6, 0, 1}) && payload[0] == SUBTRACT_MILLIS) {
      return fixedMillis(payload[1], longs[payload[2]]);
    }
    return new NativeEvaluator();
  }

  private static Evaluator fixedMillis(int column, long delay) {
    return root -> {
      Values input = timestampValues(root.getVector(column));
      return new Values() {
        @Override
        public boolean isNull(int row) {
          return input.isNull(row);
        }

        @Override
        public long getMillis(int row) {
          return input.getMillis(row) - delay;
        }
      };
    };
  }

  static Values timestampValues(FieldVector input) {
    if (input instanceof BigIntVector) {
      BigIntVector millis = ((BigIntVector) input);

      return new Values() {
        @Override
        public boolean isNull(int row) {
          return millis.isNull(row);
        }

        @Override
        public long getMillis(int row) {
          return millis.get(row);
        }
      };
    }
    TimestampAccessor timestamp = new TimestampAccessor(input);
    return new Values() {
      @Override
      public boolean isNull(int row) {
        return timestamp.isNull(row);
      }

      @Override
      public long getMillis(int row) {
        return timestamp.getMillis(row);
      }
    };
  }

  public interface Evaluator extends AutoCloseable {
    /** Read candidates before transferring the input downstream, then close the result. */
    Values evaluate(VectorSchemaRoot input);

    @Override
    default void close() {}
  }

  public interface Values extends AutoCloseable {
    boolean isNull(int row);

    long getMillis(int row);

    @Override
    default void close() {}
  }

  private final class NativeEvaluator implements Evaluator {
    private long handle =
        Native.createCalcExpression(
            kinds,
            payload,
            childCounts,
            longs,
            doubles,
            strings,
            new int[] {0},
            -1,
            new String[] {"watermark_millis"});
    private boolean inputSchemaEstablished;

    @Override
    public Values evaluate(VectorSchemaRoot input) {
      if (handle == 0) {
        throw new IllegalStateException("Watermark evaluator is closed");
      }
      BufferAllocator allocator = input.getVector(0).getAllocator();
      // C Data exports retain their own references. The caller still owns and forwards the input.
      try (ArrowArray inArray = ArrowArray.allocateNew(allocator);
          ArrowArray outArray = ArrowArray.allocateNew(allocator);
          ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
        if (inputSchemaEstablished) {
          Data.exportVectorSchemaRoot(allocator, input, NativeAllocator.DICTIONARIES, inArray);
          Native.calcExpressionArray(
              handle, inArray.memoryAddress(), outArray.memoryAddress(), outSchema.memoryAddress());
        } else {
          try (ArrowSchema inSchema = ArrowSchema.allocateNew(allocator)) {
            Data.exportVectorSchemaRoot(
                allocator, input, NativeAllocator.DICTIONARIES, inArray, inSchema);
            Native.calcExpression(
                handle,
                inArray.memoryAddress(),
                inSchema.memoryAddress(),
                outArray.memoryAddress(),
                outSchema.memoryAddress());
            inputSchemaEstablished = true;
          }
        }
        VectorSchemaRoot result =
            Data.importVectorSchemaRoot(
                allocator, outArray, outSchema, NativeAllocator.DICTIONARIES);
        if (!(result.getVector(0) instanceof BigIntVector)
            || result.getRowCount() != input.getRowCount()) {
          result.close();
          throw new IllegalStateException(
              "Watermark projection must produce one BIGINT candidate per input row");
        }
        BigIntVector values = (BigIntVector) result.getVector(0);
        return new Values() {
          @Override
          public boolean isNull(int row) {
            return values.isNull(row);
          }

          @Override
          public long getMillis(int row) {
            return values.get(row);
          }

          @Override
          public void close() {
            result.close();
          }
        };
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
}

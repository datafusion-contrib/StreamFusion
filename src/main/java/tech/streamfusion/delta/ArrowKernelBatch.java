package tech.streamfusion.delta;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/** A Delta Kernel batch that directly views Arrow vectors. */
public final class ArrowKernelBatch implements ColumnarBatch, AutoCloseable {

  private final VectorSchemaRoot root;
  private final StructType schema;
  private final List<ArrowKernelVector> vectors;
  private final int[] selectedRows;

  public ArrowKernelBatch(VectorSchemaRoot root, StructType schema) {
    this(root, schema, wrap(root.getFieldVectors(), schema, null), null);
  }

  ArrowKernelBatch(VectorSchemaRoot root, StructType schema, int[] selectedRows) {
    this(root, schema, wrap(root.getFieldVectors(), schema, selectedRows), selectedRows.clone());
  }

  private ArrowKernelBatch(
      VectorSchemaRoot root,
      StructType schema,
      List<ArrowKernelVector> vectors,
      int[] selectedRows) {
    this.root = root;
    this.schema = schema;
    this.vectors = vectors;
    this.selectedRows = selectedRows;
  }

  private static List<ArrowKernelVector> wrap(
      List<FieldVector> fields, StructType schema, int[] selectedRows) {
    if (fields.size() != schema.length()) {
      throw new IllegalArgumentException(
          "Arrow width " + fields.size() + " does not match Delta width " + schema.length());
    }
    List<ArrowKernelVector> result = new ArrayList<>(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      result.add(
          new ArrowKernelVector(
              fields.get(i),
              schema.at(i).getDataType(),
              0,
              selectedRows == null ? fields.get(i).getValueCount() : selectedRows.length,
              selectedRows));
    }
    return result;
  }

  @Override
  public StructType getSchema() {
    return schema;
  }

  @Override
  public ColumnVector getColumnVector(int ordinal) {
    return vectors.get(ordinal);
  }

  @Override
  public int getSize() {
    return selectedRows == null ? root.getRowCount() : selectedRows.length;
  }

  @Override
  public ColumnarBatch withNewColumn(
      int ordinal, StructField columnSchema, ColumnVector columnVector) {
    if (!(columnVector instanceof ArrowKernelVector)) {
      throw new UnsupportedOperationException("The native Delta path cannot materialize a column");
    }
    List<StructField> fields = new ArrayList<>(schema.fields());
    fields.add(ordinal, columnSchema);
    List<ArrowKernelVector> next = new ArrayList<>(vectors);
    next.add(ordinal, (ArrowKernelVector) columnVector);
    return new ArrowKernelBatch(root, new StructType(fields), next, selectedRows);
  }

  @Override
  public ColumnarBatch withDeletedColumnAt(int ordinal) {
    List<StructField> fields = new ArrayList<>(schema.fields());
    fields.remove(ordinal);
    List<ArrowKernelVector> next = new ArrayList<>(vectors);
    next.remove(ordinal);
    return new ArrowKernelBatch(root, new StructType(fields), next, selectedRows);
  }

  @Override
  public ColumnarBatch withNewSchema(StructType newSchema) {
    if (newSchema.length() != vectors.size()) {
      throw new IllegalArgumentException("The replacement schema has a different width");
    }
    return new ArrowKernelBatch(root, newSchema, vectors, selectedRows);
  }

  /** Returns a retained Arrow root in the Kernel-transformed column order. */
  public VectorSchemaRoot retainedRoot() {
    List<FieldVector> retained = new ArrayList<>(vectors.size());
    List<Field> fields = new ArrayList<>(vectors.size());
    for (ArrowKernelVector vector : vectors) {
      TransferPair transfer = vector.vector.getTransferPair(vector.vector.getAllocator());
      // A selected batch remains a view until Rust performs one gather for every column. Retain
      // the full top-level vector here: truncating it to selectedRows.length makes a sparse source
      // index (for example row 9 in a two-row selection) point beyond the exported array.
      int retainedRows = selectedRows == null ? vector.size : vector.vector.getValueCount();
      transfer.splitAndTransfer(vector.offset, retainedRows);
      retained.add((FieldVector) transfer.getTo());
      fields.add(deltaTimestampMetadata(vector.vector.getField(), vector.type));
    }
    return new VectorSchemaRoot(fields, retained, root.getRowCount());
  }

  /** Returns a synchronous-use view in Kernel column order; the batch remains its owner. */
  VectorSchemaRoot borrowedRoot() {
    List<FieldVector> borrowed = new ArrayList<>(vectors.size());
    List<Field> fields = new ArrayList<>(vectors.size());
    for (ArrowKernelVector vector : vectors) {
      borrowed.add(vector.vector);
      fields.add(deltaTimestampMetadata(vector.vector.getField(), vector.type));
    }
    return new VectorSchemaRoot(fields, borrowed, root.getRowCount());
  }

  /** Row permutation gathered by the native writer; empty means the full exported batch. */
  public int[] selectedRows() {
    return selectedRows == null ? new int[0] : selectedRows;
  }

  boolean hasSparseSelection() {
    return selectedRows != null && selectedRows.length < root.getRowCount();
  }

  /** Restore the timezone distinction that Flink's internal Arrow representation intentionally loses. */
  private static Field deltaTimestampMetadata(
      Field arrowField, io.delta.kernel.types.DataType deltaType) {
    ArrowType arrowType = arrowField.getType();
    List<Field> children = arrowField.getChildren();
    if (deltaType instanceof io.delta.kernel.types.TimestampType) {
      TimeUnit unit = ((ArrowType.Timestamp) arrowType).getUnit();
      arrowType = new ArrowType.Timestamp(unit, "UTC");
    } else if (deltaType instanceof io.delta.kernel.types.TimestampNTZType) {
      TimeUnit unit = ((ArrowType.Timestamp) arrowType).getUnit();
      arrowType = new ArrowType.Timestamp(unit, null);
    } else if (deltaType instanceof StructType) {
      StructType struct = (StructType) deltaType;
      List<Field> rewritten = new ArrayList<>(children.size());
      for (int i = 0; i < children.size(); i++) {
        rewritten.add(deltaTimestampMetadata(children.get(i), struct.at(i).getDataType()));
      }
      children = rewritten;
    } else if (deltaType instanceof ArrayType && !children.isEmpty()) {
      children =
          List.of(
              deltaTimestampMetadata(
                  children.get(0), ((ArrayType) deltaType).getElementType()));
    } else if (deltaType instanceof MapType && !children.isEmpty()) {
      Field entries = children.get(0);
      List<Field> entriesChildren = entries.getChildren();
      if (entriesChildren.size() == 2) {
        entries =
            copyField(
                entries,
                entries.getType(),
                List.of(
                    deltaTimestampMetadata(
                        entriesChildren.get(0), ((MapType) deltaType).getKeyType()),
                    deltaTimestampMetadata(
                        entriesChildren.get(1), ((MapType) deltaType).getValueType())));
      }
      children = List.of(entries);
    }
    return copyField(arrowField, arrowType, children);
  }

  private static Field copyField(Field source, ArrowType type, List<Field> children) {
    FieldType sourceType = source.getFieldType();
    return new Field(
        source.getName(),
        new FieldType(
            sourceType.isNullable(), type, sourceType.getDictionary(), sourceType.getMetadata()),
        children);
  }

  @Override
  public void close() {
    root.close();
  }

  private static final class ArrowKernelVector implements ColumnVector {
    private final FieldVector vector;
    private final io.delta.kernel.types.DataType type;
    private final int offset;
    private final int size;
    private final int[] selectedRows;

    private ArrowKernelVector(
        FieldVector vector,
        io.delta.kernel.types.DataType type,
        int offset,
        int size,
        int[] selectedRows) {
      this.vector = vector;
      this.type = type;
      this.offset = offset;
      this.size = size;
      this.selectedRows = selectedRows;
    }

    @Override
    public io.delta.kernel.types.DataType getDataType() {
      return type;
    }

    @Override
    public int getSize() {
      return size;
    }

    @Override
    public void close() {}

    @Override
    public boolean isNullAt(int rowId) {
      return vector.isNull(index(rowId));
    }

    @Override
    public boolean getBoolean(int rowId) {
      return ((BitVector) vector).get(index(rowId)) != 0;
    }

    @Override
    public byte getByte(int rowId) {
      return ((TinyIntVector) vector).get(index(rowId));
    }

    @Override
    public short getShort(int rowId) {
      return ((SmallIntVector) vector).get(index(rowId));
    }

    @Override
    public int getInt(int rowId) {
      int index = index(rowId);
      if (vector instanceof DateDayVector) {
        return ((DateDayVector) vector).get(index);
      }
      return ((IntVector) vector).get(index);
    }

    @Override
    public long getLong(int rowId) {
      int index = index(rowId);
      if (vector instanceof TimeStampVector) {
        long value = ((TimeStampVector) vector).get(index);
        org.apache.arrow.vector.types.TimeUnit unit =
            ((org.apache.arrow.vector.types.pojo.ArrowType.Timestamp)
                    vector.getField().getType())
                .getUnit();
        return switch (unit) {
          case SECOND -> Math.multiplyExact(value, 1_000_000L);
          case MILLISECOND -> Math.multiplyExact(value, 1_000L);
          case MICROSECOND -> value;
          case NANOSECOND -> Math.floorDiv(value, 1_000L);
        };
      }
      return ((BigIntVector) vector).get(index);
    }

    @Override
    public float getFloat(int rowId) {
      return ((Float4Vector) vector).get(index(rowId));
    }

    @Override
    public double getDouble(int rowId) {
      return ((Float8Vector) vector).get(index(rowId));
    }

    @Override
    public byte[] getBinary(int rowId) {
      if (vector instanceof BaseVariableWidthVector) {
        return ((BaseVariableWidthVector) vector).get(index(rowId));
      }
      return ((FixedSizeBinaryVector) vector).get(index(rowId));
    }

    @Override
    public String getString(int rowId) {
      return new String(
          ((VarCharVector) vector).get(index(rowId)), java.nio.charset.StandardCharsets.UTF_8);
    }

    @Override
    public BigDecimal getDecimal(int rowId) {
      return ((DecimalVector) vector).getObject(index(rowId));
    }

    @Override
    public ColumnVector getChild(int ordinal) {
      StructVector struct = (StructVector) vector;
      StructType structType = (StructType) type;
      FieldVector child = (FieldVector) struct.getVectorById(ordinal);
      return new ArrowKernelVector(
          child, structType.at(ordinal).getDataType(), offset, size, selectedRows);
    }

    @Override
    public ArrayValue getArray(int rowId) {
      ListVector list = (ListVector) vector;
      int absolute = index(rowId);
      int start = list.getOffsetBuffer().getInt((long) absolute * ListVector.OFFSET_WIDTH);
      int end = list.getOffsetBuffer().getInt((long) (absolute + 1) * ListVector.OFFSET_WIDTH);
      ArrowKernelVector elements =
          new ArrowKernelVector(
              (FieldVector) list.getDataVector(),
              ((ArrayType) type).getElementType(),
              start,
              end - start,
              null);
      return new ArrayValue() {
        @Override
        public int getSize() {
          return elements.getSize();
        }

        @Override
        public ColumnVector getElements() {
          return elements;
        }
      };
    }

    @Override
    public MapValue getMap(int rowId) {
      MapVector map = (MapVector) vector;
      int absolute = index(rowId);
      int start = map.getOffsetBuffer().getInt((long) absolute * MapVector.OFFSET_WIDTH);
      int end = map.getOffsetBuffer().getInt((long) (absolute + 1) * MapVector.OFFSET_WIDTH);
      StructVector entries = (StructVector) map.getDataVector();
      MapType mapType = (MapType) type;
      ArrowKernelVector keys =
          new ArrowKernelVector(
              (FieldVector) entries.getVectorById(0),
              mapType.getKeyType(),
              start,
              end - start,
              null);
      ArrowKernelVector values =
          new ArrowKernelVector(
              (FieldVector) entries.getVectorById(1),
              mapType.getValueType(),
              start,
              end - start,
              null);
      return new MapValue() {
        @Override
        public int getSize() {
          return end - start;
        }

        @Override
        public ColumnVector getKeys() {
          return keys;
        }

        @Override
        public ColumnVector getValues() {
          return values;
        }
      };
    }

    private int index(int rowId) {
      if (rowId < 0 || rowId >= size) {
        throw new IndexOutOfBoundsException("row " + rowId + " outside vector view of " + size);
      }
      return offset + (selectedRows == null ? rowId : selectedRows[rowId]);
    }
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.streamfusion.arrow;

import tech.streamfusion.arrow.vectors.ArrowArrayColumnVector;
import tech.streamfusion.arrow.vectors.ArrowBigIntColumnVector;
import tech.streamfusion.arrow.vectors.ArrowBinaryColumnVector;
import tech.streamfusion.arrow.vectors.ArrowBooleanColumnVector;
import tech.streamfusion.arrow.vectors.ArrowDateColumnVector;
import tech.streamfusion.arrow.vectors.ArrowDecimalColumnVector;
import tech.streamfusion.arrow.vectors.ArrowDoubleColumnVector;
import tech.streamfusion.arrow.vectors.ArrowFloatColumnVector;
import tech.streamfusion.arrow.vectors.ArrowIntColumnVector;
import tech.streamfusion.arrow.vectors.ArrowMapColumnVector;
import tech.streamfusion.arrow.vectors.ArrowNullColumnVector;
import tech.streamfusion.arrow.vectors.ArrowRowColumnVector;
import tech.streamfusion.arrow.vectors.ArrowSmallIntColumnVector;
import tech.streamfusion.arrow.vectors.ArrowTimeColumnVector;
import tech.streamfusion.arrow.vectors.ArrowTimestampColumnVector;
import tech.streamfusion.arrow.vectors.ArrowTinyIntColumnVector;
import tech.streamfusion.arrow.vectors.ArrowVarBinaryColumnVector;
import tech.streamfusion.arrow.vectors.ArrowVarCharColumnVector;
import tech.streamfusion.arrow.writers.ArrayWriter;
import tech.streamfusion.arrow.writers.ArrowFieldWriter;
import tech.streamfusion.arrow.writers.BigIntWriter;
import tech.streamfusion.arrow.writers.BinaryWriter;
import tech.streamfusion.arrow.writers.BooleanWriter;
import tech.streamfusion.arrow.writers.DateWriter;
import tech.streamfusion.arrow.writers.DecimalWriter;
import tech.streamfusion.arrow.writers.DoubleWriter;
import tech.streamfusion.arrow.writers.FloatWriter;
import tech.streamfusion.arrow.writers.IntWriter;
import tech.streamfusion.arrow.writers.MapWriter;
import tech.streamfusion.arrow.writers.NullWriter;
import tech.streamfusion.arrow.writers.RowWriter;
import tech.streamfusion.arrow.writers.SmallIntWriter;
import tech.streamfusion.arrow.writers.TimeWriter;
import tech.streamfusion.arrow.writers.TimestampWriter;
import tech.streamfusion.arrow.writers.TinyIntWriter;
import tech.streamfusion.arrow.writers.VarBinaryWriter;
import tech.streamfusion.arrow.writers.VarCharWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;

/**
 * The Arrow ↔ {@link RowData} type mapping, reader factory, and writer factory, ported (and trimmed) from
 * Flink's {@code org.apache.flink.table.runtime.arrow.ArrowUtils}. Vendored rather than depended on
 * because that class lives in {@code flink-python}, which ships in the distribution's {@code opt/} and is
 * not on a normal Java job's classpath; the per-type readers/writers it drives are vendored alongside
 * (this package), so a clean clone builds without it.
 *
 * <p>One deliberate divergence from upstream: a {@code TIMESTAMP}/{@code TIMESTAMP_LTZ} column maps to a
 * nanosecond Arrow timestamp regardless of declared precision (upstream picks a unit per precision). This
 * matches the unit the native side already produces and consumes, so the mapping needs no change across
 * the JNI boundary.
 */
public final class ArrowConversion {

  private ArrowConversion() {}

  /** The Arrow schema for a row type, with the nanosecond-timestamp convention above. */
  public static Schema toArrowSchema(RowType rowType) {
    List<Field> fields =
        rowType.getFields().stream()
            .map(f -> toArrowField(f.getName(), f.getType()))
            .collect(Collectors.toCollection(ArrayList::new));
    return new Schema(fields);
  }

  /**
   * The (key, value) logical types of a map-like column: a {@link MapType}'s key/value, or — for a
   * {@link MultisetType}, carried as a MAP&lt;element, count&gt; — the element type and a non-null INT
   * count.
   */
  private static LogicalType[] mapKeyValue(LogicalType logicalType) {
    if (logicalType instanceof MapType) {
      MapType mapType = (MapType) logicalType;
      return new LogicalType[] {mapType.getKeyType(), mapType.getValueType()};
    }
    MultisetType multisetType = (MultisetType) logicalType;
    return new LogicalType[] {multisetType.getElementType(), new IntType(false)};
  }

  private static Field toArrowField(String fieldName, LogicalType logicalType) {
    FieldType fieldType =
        new FieldType(
            logicalType.isNullable(), logicalType.accept(TypeConverter.INSTANCE), null);
    List<Field> children = null;
    if (logicalType instanceof ArrayType) {
      children =
          Collections.singletonList(
              toArrowField("element", ((ArrayType) logicalType).getElementType()));
    } else if (logicalType instanceof RowType) {
      RowType rowType = (RowType) logicalType;
      children = new ArrayList<>(rowType.getFieldCount());
      for (RowType.RowField field : rowType.getFields()) {
        children.add(toArrowField(field.getName(), field.getType()));
      }
    } else if (logicalType instanceof MapType || logicalType instanceof MultisetType) {
      // A MAP, or a MULTISET carried as MAP<element, count> (mapKeyValue resolves which).
      LogicalType[] kv = mapKeyValue(logicalType);
      children =
          Collections.singletonList(
              new Field(
                  "items",
                  new FieldType(false, ArrowType.Struct.INSTANCE, null),
                  Arrays.asList(
                      // Map keys are non-null in Flink's data model; force it so the decoder builds a
                      // non-nullable key (a nullable one is rejected when read back as MapData).
                      toArrowField("key", kv[0].copy(false)),
                      toArrowField("value", kv[1]))));
    }
    return new Field(fieldName, fieldType, children);
  }

  /** A reader that exposes the batch's rows as {@link RowData} backed directly by the Arrow buffers. */
  public static ArrowReader createArrowReader(VectorSchemaRoot root, RowType rowType) {
    List<ColumnVector> columnVectors = new ArrayList<>();
    List<FieldVector> fieldVectors = root.getFieldVectors();
    // Iterate the row type's columns, not the root's: a batch may carry trailing system columns (the
    // hidden $row_kind$) the row type does not name, which the caller reads separately.
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      columnVectors.add(createColumnVector(fieldVectors.get(i), rowType.getTypeAt(i)));
    }
    return new ArrowReader(columnVectors.toArray(new ColumnVector[0]));
  }

  /** A writer that appends {@link RowData} rows into the (pre-built, matching) Arrow vectors. */
  public static ArrowWriter<RowData> createRowDataArrowWriter(
      VectorSchemaRoot root, RowType rowType) {
    ArrowFieldWriter<RowData>[] fieldWriters = new ArrowFieldWriter[root.getFieldVectors().size()];
    List<FieldVector> vectors = root.getFieldVectors();
    for (int i = 0; i < vectors.size(); i++) {
      FieldVector vector = vectors.get(i);
      vector.allocateNew();
      fieldWriters[i] = createArrowFieldWriterForRow(vector, rowType.getTypeAt(i));
    }
    return new ArrowWriter<>(root, fieldWriters);
  }

  static ColumnVector createColumnVector(ValueVector vector, LogicalType fieldType) {
    if (vector instanceof TinyIntVector) {
      return new ArrowTinyIntColumnVector((TinyIntVector) vector);
    } else if (vector instanceof SmallIntVector) {
      return new ArrowSmallIntColumnVector((SmallIntVector) vector);
    } else if (vector instanceof IntVector) {
      return new ArrowIntColumnVector((IntVector) vector);
    } else if (vector instanceof BigIntVector) {
      return new ArrowBigIntColumnVector((BigIntVector) vector);
    } else if (vector instanceof BitVector) {
      return new ArrowBooleanColumnVector((BitVector) vector);
    } else if (vector instanceof Float4Vector) {
      return new ArrowFloatColumnVector((Float4Vector) vector);
    } else if (vector instanceof Float8Vector) {
      return new ArrowDoubleColumnVector((Float8Vector) vector);
    } else if (vector instanceof VarCharVector) {
      return new ArrowVarCharColumnVector((VarCharVector) vector);
    } else if (vector instanceof FixedSizeBinaryVector) {
      return new ArrowBinaryColumnVector((FixedSizeBinaryVector) vector);
    } else if (vector instanceof VarBinaryVector) {
      return new ArrowVarBinaryColumnVector((VarBinaryVector) vector);
    } else if (vector instanceof DecimalVector) {
      return new ArrowDecimalColumnVector((DecimalVector) vector);
    } else if (vector instanceof DateDayVector) {
      return new ArrowDateColumnVector((DateDayVector) vector);
    } else if (vector instanceof TimeSecVector
        || vector instanceof TimeMilliVector
        || vector instanceof TimeMicroVector
        || vector instanceof TimeNanoVector) {
      return new ArrowTimeColumnVector(vector);
    } else if (vector instanceof TimeStampVector) {
      return new ArrowTimestampColumnVector(vector);
    } else if (vector instanceof MapVector) {
      MapVector mapVector = (MapVector) vector;
      LogicalType[] kv = mapKeyValue(fieldType);
      LogicalType keyType = kv[0];
      LogicalType valueType = kv[1];
      StructVector structVector = (StructVector) mapVector.getDataVector();
      return new ArrowMapColumnVector(
          mapVector,
          createColumnVector(structVector.getChild(MapVector.KEY_NAME), keyType),
          createColumnVector(structVector.getChild(MapVector.VALUE_NAME), valueType));
    } else if (vector instanceof ListVector) {
      ListVector listVector = (ListVector) vector;
      return new ArrowArrayColumnVector(
          listVector,
          createColumnVector(listVector.getDataVector(), ((ArrayType) fieldType).getElementType()));
    } else if (vector instanceof StructVector) {
      StructVector structVector = (StructVector) vector;
      ColumnVector[] fieldColumns = new ColumnVector[structVector.size()];
      for (int i = 0; i < fieldColumns.length; ++i) {
        fieldColumns[i] =
            createColumnVector(structVector.getVectorById(i), ((RowType) fieldType).getTypeAt(i));
      }
      return new ArrowRowColumnVector(structVector, fieldColumns);
    } else if (vector instanceof NullVector) {
      return ArrowNullColumnVector.INSTANCE;
    } else {
      throw new UnsupportedOperationException(String.format("Unsupported type %s.", fieldType));
    }
  }

  private static ArrowFieldWriter<RowData> createArrowFieldWriterForRow(
      ValueVector vector, LogicalType fieldType) {
    if (vector instanceof TinyIntVector) {
      return TinyIntWriter.forRow((TinyIntVector) vector);
    } else if (vector instanceof SmallIntVector) {
      return SmallIntWriter.forRow((SmallIntVector) vector);
    } else if (vector instanceof IntVector) {
      return IntWriter.forRow((IntVector) vector);
    } else if (vector instanceof BigIntVector) {
      return BigIntWriter.forRow((BigIntVector) vector);
    } else if (vector instanceof BitVector) {
      return BooleanWriter.forRow((BitVector) vector);
    } else if (vector instanceof Float4Vector) {
      return FloatWriter.forRow((Float4Vector) vector);
    } else if (vector instanceof Float8Vector) {
      return DoubleWriter.forRow((Float8Vector) vector);
    } else if (vector instanceof VarCharVector) {
      return VarCharWriter.forRow((VarCharVector) vector);
    } else if (vector instanceof FixedSizeBinaryVector) {
      return BinaryWriter.forRow((FixedSizeBinaryVector) vector);
    } else if (vector instanceof VarBinaryVector) {
      return VarBinaryWriter.forRow((VarBinaryVector) vector);
    } else if (vector instanceof DecimalVector) {
      DecimalVector decimalVector = (DecimalVector) vector;
      return DecimalWriter.forRow(decimalVector, getPrecision(decimalVector), decimalVector.getScale());
    } else if (vector instanceof DateDayVector) {
      return DateWriter.forRow((DateDayVector) vector);
    } else if (vector instanceof TimeSecVector
        || vector instanceof TimeMilliVector
        || vector instanceof TimeMicroVector
        || vector instanceof TimeNanoVector) {
      return TimeWriter.forRow(vector);
    } else if (vector instanceof TimeStampVector) {
      int precision =
          fieldType instanceof LocalZonedTimestampType
              ? ((LocalZonedTimestampType) fieldType).getPrecision()
              : ((TimestampType) fieldType).getPrecision();
      return TimestampWriter.forRow(vector, precision);
    } else if (vector instanceof MapVector) {
      MapVector mapVector = (MapVector) vector;
      LogicalType[] kv = mapKeyValue(fieldType);
      LogicalType keyType = kv[0];
      LogicalType valueType = kv[1];
      StructVector structVector = (StructVector) mapVector.getDataVector();
      return MapWriter.forRow(
          mapVector,
          createArrowFieldWriterForArray(structVector.getChild(MapVector.KEY_NAME), keyType),
          createArrowFieldWriterForArray(structVector.getChild(MapVector.VALUE_NAME), valueType));
    } else if (vector instanceof ListVector) {
      ListVector listVector = (ListVector) vector;
      LogicalType elementType = ((ArrayType) fieldType).getElementType();
      return ArrayWriter.forRow(
          listVector, createArrowFieldWriterForArray(listVector.getDataVector(), elementType));
    } else if (vector instanceof StructVector) {
      RowType rowType = (RowType) fieldType;
      ArrowFieldWriter<RowData>[] fieldsWriters = new ArrowFieldWriter[rowType.getFieldCount()];
      for (int i = 0; i < fieldsWriters.length; i++) {
        fieldsWriters[i] =
            createArrowFieldWriterForRow(((StructVector) vector).getVectorById(i), rowType.getTypeAt(i));
      }
      return RowWriter.forRow((StructVector) vector, fieldsWriters);
    } else if (vector instanceof NullVector) {
      return new NullWriter<>((NullVector) vector);
    } else {
      throw new UnsupportedOperationException(String.format("Unsupported type %s.", fieldType));
    }
  }

  private static ArrowFieldWriter<ArrayData> createArrowFieldWriterForArray(
      ValueVector vector, LogicalType fieldType) {
    if (vector instanceof TinyIntVector) {
      return TinyIntWriter.forArray((TinyIntVector) vector);
    } else if (vector instanceof SmallIntVector) {
      return SmallIntWriter.forArray((SmallIntVector) vector);
    } else if (vector instanceof IntVector) {
      return IntWriter.forArray((IntVector) vector);
    } else if (vector instanceof BigIntVector) {
      return BigIntWriter.forArray((BigIntVector) vector);
    } else if (vector instanceof BitVector) {
      return BooleanWriter.forArray((BitVector) vector);
    } else if (vector instanceof Float4Vector) {
      return FloatWriter.forArray((Float4Vector) vector);
    } else if (vector instanceof Float8Vector) {
      return DoubleWriter.forArray((Float8Vector) vector);
    } else if (vector instanceof VarCharVector) {
      return VarCharWriter.forArray((VarCharVector) vector);
    } else if (vector instanceof FixedSizeBinaryVector) {
      return BinaryWriter.forArray((FixedSizeBinaryVector) vector);
    } else if (vector instanceof VarBinaryVector) {
      return VarBinaryWriter.forArray((VarBinaryVector) vector);
    } else if (vector instanceof DecimalVector) {
      DecimalVector decimalVector = (DecimalVector) vector;
      return DecimalWriter.forArray(decimalVector, getPrecision(decimalVector), decimalVector.getScale());
    } else if (vector instanceof DateDayVector) {
      return DateWriter.forArray((DateDayVector) vector);
    } else if (vector instanceof TimeSecVector
        || vector instanceof TimeMilliVector
        || vector instanceof TimeMicroVector
        || vector instanceof TimeNanoVector) {
      return TimeWriter.forArray(vector);
    } else if (vector instanceof TimeStampVector) {
      int precision =
          fieldType instanceof LocalZonedTimestampType
              ? ((LocalZonedTimestampType) fieldType).getPrecision()
              : ((TimestampType) fieldType).getPrecision();
      return TimestampWriter.forArray(vector, precision);
    } else if (vector instanceof MapVector) {
      MapVector mapVector = (MapVector) vector;
      LogicalType[] kv = mapKeyValue(fieldType);
      LogicalType keyType = kv[0];
      LogicalType valueType = kv[1];
      StructVector structVector = (StructVector) mapVector.getDataVector();
      return MapWriter.forArray(
          mapVector,
          createArrowFieldWriterForArray(structVector.getChild(MapVector.KEY_NAME), keyType),
          createArrowFieldWriterForArray(structVector.getChild(MapVector.VALUE_NAME), valueType));
    } else if (vector instanceof ListVector) {
      ListVector listVector = (ListVector) vector;
      LogicalType elementType = ((ArrayType) fieldType).getElementType();
      return ArrayWriter.forArray(
          listVector, createArrowFieldWriterForArray(listVector.getDataVector(), elementType));
    } else if (vector instanceof StructVector) {
      RowType rowType = (RowType) fieldType;
      // A struct's children are always read as a RowData, even when the struct sits inside an array.
      ArrowFieldWriter<RowData>[] fieldsWriters = new ArrowFieldWriter[rowType.getFieldCount()];
      for (int i = 0; i < fieldsWriters.length; i++) {
        fieldsWriters[i] =
            createArrowFieldWriterForRow(
                ((StructVector) vector).getVectorById(i), rowType.getTypeAt(i));
      }
      return RowWriter.forArray((StructVector) vector, fieldsWriters);
    } else if (vector instanceof NullVector) {
      return new NullWriter<>((NullVector) vector);
    } else {
      throw new UnsupportedOperationException(String.format("Unsupported type %s.", fieldType));
    }
  }

  private static int getPrecision(DecimalVector decimalVector) {
    return decimalVector.getPrecision();
  }

  /** Maps each Flink logical type to its Arrow type — see {@code ArrowUtils.LogicalTypeToArrowTypeConverter},
   * with timestamps pinned to nanoseconds (the divergence documented on the class). */
  private static final class TypeConverter extends LogicalTypeDefaultVisitor<ArrowType> {
    private static final TypeConverter INSTANCE = new TypeConverter();

    @Override
    public ArrowType visit(TinyIntType tinyIntType) {
      return new ArrowType.Int(8, true);
    }

    @Override
    public ArrowType visit(SmallIntType smallIntType) {
      return new ArrowType.Int(2 * 8, true);
    }

    @Override
    public ArrowType visit(IntType intType) {
      return new ArrowType.Int(4 * 8, true);
    }

    @Override
    public ArrowType visit(BigIntType bigIntType) {
      return new ArrowType.Int(8 * 8, true);
    }

    @Override
    public ArrowType visit(BooleanType booleanType) {
      return ArrowType.Bool.INSTANCE;
    }

    @Override
    public ArrowType visit(FloatType floatType) {
      return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    }

    @Override
    public ArrowType visit(DoubleType doubleType) {
      return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    }

    @Override
    public ArrowType visit(CharType charType) {
      return ArrowType.Utf8.INSTANCE;
    }

    @Override
    public ArrowType visit(VarCharType varCharType) {
      return ArrowType.Utf8.INSTANCE;
    }

    @Override
    public ArrowType visit(BinaryType binaryType) {
      return new ArrowType.FixedSizeBinary(binaryType.getLength());
    }

    @Override
    public ArrowType visit(VarBinaryType varBinaryType) {
      return ArrowType.Binary.INSTANCE;
    }

    @Override
    public ArrowType visit(DecimalType decimalType) {
      return new ArrowType.Decimal(decimalType.getPrecision(), decimalType.getScale());
    }

    @Override
    public ArrowType visit(DateType dateType) {
      return new ArrowType.Date(DateUnit.DAY);
    }

    @Override
    public ArrowType visit(TimeType timeType) {
      if (timeType.getPrecision() == 0) {
        return new ArrowType.Time(TimeUnit.SECOND, 32);
      } else if (timeType.getPrecision() <= 3) {
        return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
      } else if (timeType.getPrecision() <= 6) {
        return new ArrowType.Time(TimeUnit.MICROSECOND, 64);
      } else {
        return new ArrowType.Time(TimeUnit.NANOSECOND, 64);
      }
    }

    @Override
    public ArrowType visit(YearMonthIntervalType yearMonthIntervalType) {
      // Flink's internal form is the signed number of months.
      return new ArrowType.Int(4 * 8, true);
    }

    @Override
    public ArrowType visit(DayTimeIntervalType dayTimeIntervalType) {
      // Flink's internal form is the signed number of milliseconds.
      return new ArrowType.Int(8 * 8, true);
    }

    // Timestamps are pinned to nanoseconds (no timezone) to match the unit the native side uses,
    // rather than upstream's per-precision unit. See the class comment.
    @Override
    public ArrowType visit(LocalZonedTimestampType localZonedTimestampType) {
      return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
    }

    @Override
    public ArrowType visit(TimestampType timestampType) {
      return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
    }

    @Override
    public ArrowType visit(ArrayType arrayType) {
      return ArrowType.List.INSTANCE;
    }

    @Override
    public ArrowType visit(RowType rowType) {
      return ArrowType.Struct.INSTANCE;
    }

    @Override
    public ArrowType visit(MapType mapType) {
      return new ArrowType.Map(false);
    }

    @Override
    public ArrowType visit(MultisetType multisetType) {
      // A MULTISET<E> is carried as a MAP<E, INT> (element → occurrence count).
      return new ArrowType.Map(false);
    }

    @Override
    protected ArrowType defaultMethod(LogicalType logicalType) {
      throw new UnsupportedOperationException(
          String.format("Unsupported data type %s currently.", logicalType.asSummaryString()));
    }
  }
}

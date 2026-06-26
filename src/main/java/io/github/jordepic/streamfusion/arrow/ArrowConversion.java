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

package io.github.jordepic.streamfusion.arrow;

import io.github.jordepic.streamfusion.arrow.vectors.ArrowArrayColumnVector;
import io.github.jordepic.streamfusion.arrow.vectors.ArrowBigIntColumnVector;
import io.github.jordepic.streamfusion.arrow.vectors.ArrowBinaryColumnVector;
import io.github.jordepic.streamfusion.arrow.vectors.ArrowBooleanColumnVector;
import io.github.jordepic.streamfusion.arrow.vectors.ArrowDateColumnVector;
import io.github.jordepic.streamfusion.arrow.vectors.ArrowDecimalColumnVector;
import io.github.jordepic.streamfusion.arrow.vectors.ArrowDoubleColumnVector;
import io.github.jordepic.streamfusion.arrow.vectors.ArrowFloatColumnVector;
import io.github.jordepic.streamfusion.arrow.vectors.ArrowIntColumnVector;
import io.github.jordepic.streamfusion.arrow.vectors.ArrowMapColumnVector;
import io.github.jordepic.streamfusion.arrow.vectors.ArrowNullColumnVector;
import io.github.jordepic.streamfusion.arrow.vectors.ArrowRowColumnVector;
import io.github.jordepic.streamfusion.arrow.vectors.ArrowSmallIntColumnVector;
import io.github.jordepic.streamfusion.arrow.vectors.ArrowTimeColumnVector;
import io.github.jordepic.streamfusion.arrow.vectors.ArrowTimestampColumnVector;
import io.github.jordepic.streamfusion.arrow.vectors.ArrowTinyIntColumnVector;
import io.github.jordepic.streamfusion.arrow.vectors.ArrowVarBinaryColumnVector;
import io.github.jordepic.streamfusion.arrow.vectors.ArrowVarCharColumnVector;
import io.github.jordepic.streamfusion.arrow.writers.ArrayWriter;
import io.github.jordepic.streamfusion.arrow.writers.ArrowFieldWriter;
import io.github.jordepic.streamfusion.arrow.writers.BigIntWriter;
import io.github.jordepic.streamfusion.arrow.writers.BinaryWriter;
import io.github.jordepic.streamfusion.arrow.writers.BooleanWriter;
import io.github.jordepic.streamfusion.arrow.writers.DateWriter;
import io.github.jordepic.streamfusion.arrow.writers.DecimalWriter;
import io.github.jordepic.streamfusion.arrow.writers.DoubleWriter;
import io.github.jordepic.streamfusion.arrow.writers.FloatWriter;
import io.github.jordepic.streamfusion.arrow.writers.IntWriter;
import io.github.jordepic.streamfusion.arrow.writers.MapWriter;
import io.github.jordepic.streamfusion.arrow.writers.NullWriter;
import io.github.jordepic.streamfusion.arrow.writers.RowWriter;
import io.github.jordepic.streamfusion.arrow.writers.SmallIntWriter;
import io.github.jordepic.streamfusion.arrow.writers.TimeWriter;
import io.github.jordepic.streamfusion.arrow.writers.TimestampWriter;
import io.github.jordepic.streamfusion.arrow.writers.TinyIntWriter;
import io.github.jordepic.streamfusion.arrow.writers.VarBinaryWriter;
import io.github.jordepic.streamfusion.arrow.writers.VarCharWriter;
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
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
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
    } else if (logicalType instanceof MapType) {
      MapType mapType = (MapType) logicalType;
      children =
          Collections.singletonList(
              new Field(
                  "items",
                  new FieldType(false, ArrowType.Struct.INSTANCE, null),
                  Arrays.asList(
                      // Map keys are non-null in Flink's data model; force it so the decoder builds a
                      // non-nullable key (a nullable one is rejected when read back as MapData).
                      toArrowField("key", mapType.getKeyType().copy(false)),
                      toArrowField("value", mapType.getValueType()))));
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
    } else if (vector instanceof TimeStampVector
        && ((ArrowType.Timestamp) vector.getField().getType()).getTimezone() == null) {
      return new ArrowTimestampColumnVector(vector);
    } else if (vector instanceof MapVector) {
      MapVector mapVector = (MapVector) vector;
      LogicalType keyType = ((MapType) fieldType).getKeyType();
      LogicalType valueType = ((MapType) fieldType).getValueType();
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
    } else if (vector instanceof TimeStampVector
        && ((ArrowType.Timestamp) vector.getField().getType()).getTimezone() == null) {
      int precision =
          fieldType instanceof LocalZonedTimestampType
              ? ((LocalZonedTimestampType) fieldType).getPrecision()
              : ((TimestampType) fieldType).getPrecision();
      return TimestampWriter.forRow(vector, precision);
    } else if (vector instanceof MapVector) {
      MapVector mapVector = (MapVector) vector;
      LogicalType keyType = ((MapType) fieldType).getKeyType();
      LogicalType valueType = ((MapType) fieldType).getValueType();
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
    } else if (vector instanceof TimeStampVector
        && ((ArrowType.Timestamp) vector.getField().getType()).getTimezone() == null) {
      int precision =
          fieldType instanceof LocalZonedTimestampType
              ? ((LocalZonedTimestampType) fieldType).getPrecision()
              : ((TimestampType) fieldType).getPrecision();
      return TimestampWriter.forArray(vector, precision);
    } else if (vector instanceof MapVector) {
      MapVector mapVector = (MapVector) vector;
      LogicalType keyType = ((MapType) fieldType).getKeyType();
      LogicalType valueType = ((MapType) fieldType).getValueType();
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
    protected ArrowType defaultMethod(LogicalType logicalType) {
      throw new UnsupportedOperationException(
          String.format("Unsupported data type %s currently.", logicalType.asSummaryString()));
    }
  }
}

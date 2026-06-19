package io.github.jordepic.streamfusion.operator;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

/**
 * Converts whole {@link RowData} rows of a fixed schema to and from an Arrow {@link
 * VectorSchemaRoot}, one Arrow vector per column. Unlike the window operators, which extract only
 * the few columns they aggregate, this carries an entire row across the boundary — the foundation
 * for stateless operators (filter, projection) and, later, keeping columnar chains native.
 *
 * <p>Only the column types below are supported; {@link #supports} reports whether a schema is
 * convertible so the planner can fall back otherwise. Nulls are preserved per cell.
 */
public final class RowDataArrowConverter {

  private RowDataArrowConverter() {}

  /** Whether every column of the schema has a type this converter handles. */
  public static boolean supports(RowType rowType) {
    for (LogicalType type : rowType.getChildren()) {
      switch (type.getTypeRoot()) {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
        case CHAR:
        case VARCHAR:
        case TIMESTAMP_WITHOUT_TIME_ZONE:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        case DATE:
        case DECIMAL:
          break;
        default:
          return false;
      }
    }
    return true;
  }

  /** A timestamp column's declared precision, for {@link RowData#getTimestamp}. */
  private static int timestampPrecision(LogicalType type) {
    return type instanceof TimestampType
        ? ((TimestampType) type).getPrecision()
        : ((LocalZonedTimestampType) type).getPrecision();
  }

  /** Builds an Arrow batch holding {@code rows} under {@code rowType}. The caller closes the root. */
  public static VectorSchemaRoot write(
      List<RowData> rows, RowType rowType, BufferAllocator allocator) {
    int rowCount = rows.size();
    List<FieldVector> vectors = new ArrayList<>();
    for (int column = 0; column < rowType.getFieldCount(); column++) {
      vectors.add(writeColumn(rowType.getTypeAt(column), column, rows, allocator));
    }
    VectorSchemaRoot root = new VectorSchemaRoot(vectors);
    root.setRowCount(rowCount);
    return root;
  }

  /** Reads the rows of an Arrow batch back into {@link RowData}s under {@code rowType}. */
  public static List<RowData> read(VectorSchemaRoot root, RowType rowType) {
    int rowCount = root.getRowCount();
    List<GenericRowData> rows = new ArrayList<>(rowCount);
    for (int i = 0; i < rowCount; i++) {
      rows.add(new GenericRowData(rowType.getFieldCount()));
    }
    for (int column = 0; column < rowType.getFieldCount(); column++) {
      readColumn(root.getVector(column), rowType.getTypeAt(column), rows, column);
    }
    return new ArrayList<>(rows);
  }

  private static FieldVector writeColumn(
      LogicalType type, int column, List<RowData> rows, BufferAllocator allocator) {
    String name = "f" + column;
    int n = rows.size();
    switch (type.getTypeRoot()) {
      case TINYINT: {
        TinyIntVector v = new TinyIntVector(name, allocator);
        for (int i = 0; i < n; i++) {
          if (rows.get(i).isNullAt(column)) v.setNull(i);
          else v.setSafe(i, rows.get(i).getByte(column));
        }
        v.setValueCount(n);
        return v;
      }
      case SMALLINT: {
        SmallIntVector v = new SmallIntVector(name, allocator);
        for (int i = 0; i < n; i++) {
          if (rows.get(i).isNullAt(column)) v.setNull(i);
          else v.setSafe(i, rows.get(i).getShort(column));
        }
        v.setValueCount(n);
        return v;
      }
      case INTEGER: {
        IntVector v = new IntVector(name, allocator);
        for (int i = 0; i < n; i++) {
          if (rows.get(i).isNullAt(column)) v.setNull(i);
          else v.setSafe(i, rows.get(i).getInt(column));
        }
        v.setValueCount(n);
        return v;
      }
      case BIGINT: {
        BigIntVector v = new BigIntVector(name, allocator);
        for (int i = 0; i < n; i++) {
          if (rows.get(i).isNullAt(column)) v.setNull(i);
          else v.setSafe(i, rows.get(i).getLong(column));
        }
        v.setValueCount(n);
        return v;
      }
      case FLOAT: {
        Float4Vector v = new Float4Vector(name, allocator);
        for (int i = 0; i < n; i++) {
          if (rows.get(i).isNullAt(column)) v.setNull(i);
          else v.setSafe(i, rows.get(i).getFloat(column));
        }
        v.setValueCount(n);
        return v;
      }
      case DOUBLE: {
        Float8Vector v = new Float8Vector(name, allocator);
        for (int i = 0; i < n; i++) {
          if (rows.get(i).isNullAt(column)) v.setNull(i);
          else v.setSafe(i, rows.get(i).getDouble(column));
        }
        v.setValueCount(n);
        return v;
      }
      case BOOLEAN: {
        BitVector v = new BitVector(name, allocator);
        for (int i = 0; i < n; i++) {
          if (rows.get(i).isNullAt(column)) v.setNull(i);
          else v.setSafe(i, rows.get(i).getBoolean(column) ? 1 : 0);
        }
        v.setValueCount(n);
        return v;
      }
      case CHAR:
      case VARCHAR: {
        VarCharVector v = new VarCharVector(name, allocator);
        for (int i = 0; i < n; i++) {
          if (rows.get(i).isNullAt(column)) v.setNull(i);
          else v.setSafe(i, rows.get(i).getString(column).toBytes());
        }
        v.setValueCount(n);
        return v;
      }
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
        int precision = timestampPrecision(type);
        TimeStampNanoVector v = new TimeStampNanoVector(name, allocator);
        for (int i = 0; i < n; i++) {
          if (rows.get(i).isNullAt(column)) {
            v.setNull(i);
          } else {
            TimestampData ts = rows.get(i).getTimestamp(column, precision);
            v.setSafe(i, ts.getMillisecond() * 1_000_000L + ts.getNanoOfMillisecond());
          }
        }
        v.setValueCount(n);
        return v;
      }
      case DATE: {
        // Flink represents DATE as an int day count, as does Arrow's day-precision date vector.
        DateDayVector v = new DateDayVector(name, allocator);
        for (int i = 0; i < n; i++) {
          if (rows.get(i).isNullAt(column)) v.setNull(i);
          else v.setSafe(i, rows.get(i).getInt(column));
        }
        v.setValueCount(n);
        return v;
      }
      case DECIMAL: {
        int precision = ((DecimalType) type).getPrecision();
        int scale = ((DecimalType) type).getScale();
        DecimalVector v = new DecimalVector(name, allocator, precision, scale);
        for (int i = 0; i < n; i++) {
          if (rows.get(i).isNullAt(column)) {
            v.setNull(i);
          } else {
            v.setSafe(i, rows.get(i).getDecimal(column, precision, scale).toBigDecimal());
          }
        }
        v.setValueCount(n);
        return v;
      }
      default:
        throw new IllegalArgumentException("unsupported column type: " + type);
    }
  }

  private static void readColumn(
      FieldVector vector, LogicalType type, List<GenericRowData> rows, int column) {
    int n = rows.size();
    for (int i = 0; i < n; i++) {
      if (vector.isNull(i)) {
        rows.get(i).setField(column, null);
        continue;
      }
      Object value;
      switch (type.getTypeRoot()) {
        case TINYINT:
          value = ((TinyIntVector) vector).get(i);
          break;
        case SMALLINT:
          value = ((SmallIntVector) vector).get(i);
          break;
        case INTEGER:
          value = ((IntVector) vector).get(i);
          break;
        case BIGINT:
          value = ((BigIntVector) vector).get(i);
          break;
        case FLOAT:
          value = ((Float4Vector) vector).get(i);
          break;
        case DOUBLE:
          value = ((Float8Vector) vector).get(i);
          break;
        case BOOLEAN:
          value = ((BitVector) vector).get(i) != 0;
          break;
        case CHAR:
        case VARCHAR:
          value = StringData.fromBytes(((VarCharVector) vector).get(i));
          break;
        case TIMESTAMP_WITHOUT_TIME_ZONE:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
          long nanos = ((TimeStampNanoVector) vector).get(i);
          value = TimestampData.fromEpochMillis(nanos / 1_000_000L, (int) (nanos % 1_000_000L));
          break;
        }
        case DATE:
          value = ((DateDayVector) vector).get(i);
          break;
        case DECIMAL: {
          BigDecimal decimal = ((DecimalVector) vector).getObject(i);
          value =
              DecimalData.fromBigDecimal(
                  decimal, ((DecimalType) type).getPrecision(), ((DecimalType) type).getScale());
          break;
        }
        default:
          throw new IllegalArgumentException("unsupported column type: " + type);
      }
      rows.get(i).setField(column, value);
    }
  }
}

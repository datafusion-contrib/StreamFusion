package tech.streamfusion.paimon;

import java.nio.ByteOrder;
import java.util.List;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.paimon.shade.org.apache.hadoop.hive.ql.exec.vector.*;

/** Test-only column adapter. No RowData objects or per-value string/decimal allocations. */
final class ArrowOrcVectors {
  private final Column[] columns;

  ArrowOrcVectors(List<Field> fields) {
    columns = fields.stream().map(Column::new).toArray(Column[]::new);
  }

  void copy(VectorSchemaRoot input, VectorizedRowBatch output) {
    output.reset();
    output.size = input.getRowCount();
    for (int c = 0; c < columns.length; c++)
      columns[c].copy(input.getVector(c), output.cols[c], 0, output.size);
  }

  private static final class Column {
    private final ArrowType type;
    private final Column[] children;
    private byte[] bytes = new byte[16];

    Column(Field field) {
      type = field.getType();
      children = field.getChildren().stream().map(Column::new).toArray(Column[]::new);
    }

    void copy(FieldVector input, ColumnVector output, int start, int count) {
      output.ensureSize(count, false);
      output.isRepeating = false;
      output.noNulls = true;
      for (int i = 0; i < count; i++) {
        output.isNull[i] = input.isNull(start + i);
        output.noNulls &= !output.isNull[i];
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
          long[] values = ((LongColumnVector) output).vector;
          if (width == 8) {
            data.nioBuffer((long) start * 8, count * 8)
                .order(ByteOrder.LITTLE_ENDIAN)
                .asLongBuffer()
                .get(values, 0, count);
          } else {
            for (int i = 0; i < count; i++) {
              long at = (long) (start + i) * width;
                  switch (width) {
                    case 1:
                      values[i] = data.getByte(at);
                      break;
                    case 2:
                      values[i] = data.getShort(at);
                      break;
                    case 4:
                      values[i] = data.getInt(at);
                      break;
                    default:
                      throw new IllegalArgumentException(type.toString());
                  }
            }
          }
        }
            break;
          }
        case Bool:
          {
            {
          long[] values = ((LongColumnVector) output).vector;
          for (int i = 0; i < count; i++) {
            int row = start + i;
            values[i] = (data.getByte(row / 8) >>> (row % 8)) & 1;
          }
        }
            break;
          }
        case FloatingPoint:
          {
            {
          double[] values = ((DoubleColumnVector) output).vector;
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
          if (bytes.length < length) bytes = new byte[length];
          data.getBytes((long) start * width, bytes, 0, length);
          var target = (BytesColumnVector) output;
          for (int i = 0; i < count; i++) target.setRef(i, bytes, i * width, width);
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
          if (bytes.length < length) bytes = new byte[length];
          data.getBytes(begin, bytes, 0, length);
          var target = (BytesColumnVector) output;
          for (int i = 0; i < count; i++) {
            int from = offsets.getInt((long) (start + i) * 4);
            int to = offsets.getInt((long) (start + i + 1) * 4);
            target.setRef(i, bytes, from - begin, to - from);
          }
        }
            break;
          }
        case Decimal:
          {
            {
          var decimal = (ArrowType.Decimal) type;
          if (decimal.getBitWidth() != 128) throw new IllegalArgumentException(type.toString());
              if (output instanceof Decimal64ColumnVector) {
                Decimal64ColumnVector target = ((Decimal64ColumnVector) output);

            for (int i = 0; i < count; i++)
              target.vector[i] = data.getLong((long) (start + i) * 16);
            break;
          }
          var target = (DecimalColumnVector) output;
          for (int i = 0; i < count; i++) {
            if (output.isNull[i]) continue;
            long at = (long) (start + i) * 16;
            if (decimal.getPrecision() <= 18) {
              target.vector[i].setFromLongAndScale(data.getLong(at), decimal.getScale());
            } else {
              for (int b = 0; b < 16; b++) bytes[15 - b] = data.getByte(at + b);
              target.vector[i].setFromBigIntegerBytesAndScale(bytes, 0, 16, decimal.getScale());
            }
          }
        }
            break;
          }
        case Timestamp:
          {
            {
              long units;
              switch (((ArrowType.Timestamp) type).getUnit()) {
                case SECOND:
                  units = 1;
                  break;
                case MILLISECOND:
                  units = 1000;
                  break;
                case MICROSECOND:
                  units = 1_000_000;
                  break;
                case NANOSECOND:
                  units = 1_000_000_000;
                  break;
                default:
                  throw new IncompatibleClassChangeError();
              }
          var target = (TimestampColumnVector) output;
          target.setIsUTC(true);
          for (int i = 0; i < count; i++) {
            long value = data.getLong((long) (start + i) * 8);
            long seconds = Math.floorDiv(value, units);
            int nanos = (int) (Math.floorMod(value, units) * (1_000_000_000 / units));
            target.time[i] = seconds * 1000 + nanos / 1_000_000;
            target.nanos[i] = nanos;
          }
        }
            break;
          }
        case List:
        case Map:
          {
            {
          var list = (ListVector) input;
          var target = (MultiValuedColumnVector) output;
          int begin = list.getOffsetBuffer().getInt((long) start * 4);
          int end = list.getOffsetBuffer().getInt((long) (start + count) * 4);
          for (int i = 0; i < count; i++) {
            int from = list.getOffsetBuffer().getInt((long) (start + i) * 4);
            int to = list.getOffsetBuffer().getInt((long) (start + i + 1) * 4);
            target.offsets[i] = from - begin;
            target.lengths[i] = to - from;
          }
          target.childCount = end - begin;
          if (input instanceof MapVector) {
            var entries = (StructVector) list.getDataVector();
            var map = (MapColumnVector) output;
            children[0].children[0].copy(
                entries.getChildrenFromFields().get(0), map.keys, begin, end - begin);
            children[0].children[1].copy(
                entries.getChildrenFromFields().get(1), map.values, begin, end - begin);
          } else {
            children[0].copy(
                list.getDataVector(), ((ListColumnVector) output).child, begin, end - begin);
          }
        }
            break;
          }
        case Struct:
          {
            {
          var fields = ((StructVector) input).getChildrenFromFields();
          var target = (StructColumnVector) output;
          for (int c = 0; c < children.length; c++)
            children[c].copy(fields.get(c), target.fields[c], start, count);
        }
            break;
          }
        default:
          throw new IllegalArgumentException("Benchmark adapter: " + type);
      }
    }
  }
}

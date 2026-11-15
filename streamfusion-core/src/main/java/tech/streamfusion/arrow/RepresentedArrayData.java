package tech.streamfusion.arrow;

import org.apache.flink.table.data.*;
import org.apache.flink.table.data.columnar.ColumnarArrayData;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import tech.streamfusion.arrow.vectors.ArrowRowColumnVector;

/** A fresh borrowed slice, retaining nested Java string caches and ROW kinds. */
public final class RepresentedArrayData extends tech.streamfusion.compat.ArrayDataCompat {
  private final ColumnVector vector;
  private final int offset;
  private final ArrayData values;

  public RepresentedArrayData(ColumnVector vector, int offset, int size) {
    this.vector = vector;
    this.offset = offset;
    values = new ColumnarArrayData(vector, offset, size);
  }

  @Override
  public StringData getString(int pos) {
    return vector instanceof NestedStringColumnVector
        ? ((NestedStringColumnVector) vector).getString(offset + pos)
        : values.getString(pos);
  }

  @Override
  public RowData getRow(int pos, int fields) {
    return vector instanceof ArrowRowColumnVector
        ? ((ArrowRowColumnVector) vector).getRowData(offset + pos)
        : values.getRow(pos, fields);
  }

  @Override
  protected ArrayData sourceArray() {
    return values;
  }

  @Override
  public int size() {
    return values.size();
  }

  @Override
  public boolean isNullAt(int pos) {
    return values.isNullAt(pos);
  }

  @Override
  public boolean getBoolean(int pos) {
    return values.getBoolean(pos);
  }

  @Override
  public byte getByte(int pos) {
    return values.getByte(pos);
  }

  @Override
  public short getShort(int pos) {
    return values.getShort(pos);
  }

  @Override
  public int getInt(int pos) {
    return values.getInt(pos);
  }

  @Override
  public long getLong(int pos) {
    return values.getLong(pos);
  }

  @Override
  public float getFloat(int pos) {
    return values.getFloat(pos);
  }

  @Override
  public double getDouble(int pos) {
    return values.getDouble(pos);
  }

  @Override
  public DecimalData getDecimal(int pos, int precision, int scale) {
    return values.getDecimal(pos, precision, scale);
  }

  @Override
  public TimestampData getTimestamp(int pos, int precision) {
    return values.getTimestamp(pos, precision);
  }

  @Override
  public <T> RawValueData<T> getRawValue(int pos) {
    return values.getRawValue(pos);
  }

  @Override
  public byte[] getBinary(int pos) {
    return values.getBinary(pos);
  }

  @Override
  public ArrayData getArray(int pos) {
    return values.getArray(pos);
  }

  @Override
  public MapData getMap(int pos) {
    return values.getMap(pos);
  }

  @Override
  public boolean[] toBooleanArray() {
    return values.toBooleanArray();
  }

  @Override
  public byte[] toByteArray() {
    return values.toByteArray();
  }

  @Override
  public short[] toShortArray() {
    return values.toShortArray();
  }

  @Override
  public int[] toIntArray() {
    return values.toIntArray();
  }

  @Override
  public long[] toLongArray() {
    return values.toLongArray();
  }

  @Override
  public float[] toFloatArray() {
    return values.toFloatArray();
  }

  @Override
  public double[] toDoubleArray() {
    return values.toDoubleArray();
  }
}

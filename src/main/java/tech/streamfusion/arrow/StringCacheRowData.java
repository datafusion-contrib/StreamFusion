package tech.streamfusion.arrow;

import org.apache.arrow.vector.complex.ListVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

/** Reusable boundary row preserving explicit Java string caches over borrowed Arrow buffers. */
final class StringCacheRowData extends tech.streamfusion.compat.ProjectedRowDataCompat {
  private final ListVector[] caches;
  private final BorrowedStringValues[] borrowed;
  private RowData row;
  private int rowIndex;

  StringCacheRowData(ListVector[] caches, BorrowedStringValues[] borrowed) {
    this.caches = caches;
    this.borrowed = borrowed;
  }

  RowData replace(RowData row, int rowIndex) {
    this.row = row;
    this.rowIndex = rowIndex;
    return this;
  }

  @Override
  public RowData getRow(int pos, int fields) {
    return row.getRow(pos, fields);
  }

  @Override
  public int getArity() {
    return row.getArity();
  }

  @Override
  public RowKind getRowKind() {
    return row.getRowKind();
  }

  @Override
  public void setRowKind(RowKind kind) {
    row.setRowKind(kind);
  }

  @Override
  public boolean isNullAt(int pos) {
    return row.isNullAt(pos);
  }

  @Override
  public boolean getBoolean(int pos) {
    return row.getBoolean(pos);
  }

  @Override
  public byte getByte(int pos) {
    return row.getByte(pos);
  }

  @Override
  public short getShort(int pos) {
    return row.getShort(pos);
  }

  @Override
  public int getInt(int pos) {
    return row.getInt(pos);
  }

  @Override
  public long getLong(int pos) {
    return row.getLong(pos);
  }

  @Override
  public float getFloat(int pos) {
    return row.getFloat(pos);
  }

  @Override
  public double getDouble(int pos) {
    return row.getDouble(pos);
  }

  @Override
  public StringData getString(int pos) {
    var binary =
        borrowed != null && borrowed[pos] != null
            ? borrowed[pos].value(rowIndex)
            : (org.apache.flink.table.data.binary.BinaryStringData) row.getString(pos);
    if (caches == null) return binary;
    var restored = StringCacheColumns.read(caches[pos], rowIndex, binary);
    return restored == null ? binary : restored;
  }

  @Override
  public DecimalData getDecimal(int pos, int precision, int scale) {
    return row.getDecimal(pos, precision, scale);
  }

  @Override
  public TimestampData getTimestamp(int pos, int precision) {
    return row.getTimestamp(pos, precision);
  }

  @Override
  public <T> RawValueData<T> getRawValue(int pos) {
    return row.getRawValue(pos);
  }

  @Override
  public byte[] getBinary(int pos) {
    return row.getBinary(pos);
  }

  @Override
  protected RowData sourceRow() {
    return row;
  }

  @Override
  protected int sourceIndex(int pos) {
    return pos;
  }

  @Override
  public ArrayData getArray(int pos) {
    return row.getArray(pos);
  }

  @Override
  public MapData getMap(int pos) {
    return row.getMap(pos);
  }
}

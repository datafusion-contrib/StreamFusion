package tech.streamfusion.delta;

import io.delta.flink.sink.KernelBatchRowData;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.columnar.vector.BytesColumnVector.Bytes;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.variant.Variant;
import tech.streamfusion.arrow.ArrowConversion;

/** Retained row views used only for Delta changelog and primary-key bookkeeping. */
public final class ArrowKernelRows {

  private final VectorSchemaRoot root;
  private final StructType deltaSchema;
  private final VectorizedColumnBatch flinkBatch;
  private final AtomicInteger remaining;

  public ArrowKernelRows(VectorSchemaRoot root, RowType flinkType, StructType deltaSchema) {
    this.root = root;
    this.deltaSchema = deltaSchema;
    this.flinkBatch =
        new VectorizedColumnBatch(
            ArrowConversion.createArrowReader(root, flinkType).getColumnVectors());
    this.remaining = new AtomicInteger(root.getRowCount());
  }

  public KernelBatchRowData row(int rowId, RowKind kind) {
    return new Row(this, rowId, kind);
  }

  private FilteredColumnarBatch select(int[] rowIds) {
    List<FieldVector> retained = new ArrayList<>(deltaSchema.length());
    for (int column = 0; column < deltaSchema.length(); column++) {
      FieldVector source = root.getVector(column);
      TransferPair transfer = source.getTransferPair(source.getAllocator());
      transfer.splitAndTransfer(0, root.getRowCount());
      retained.add((FieldVector) transfer.getTo());
    }
    return new FilteredColumnarBatch(
        new ArrowKernelBatch(
            new VectorSchemaRoot(
                retained.stream().map(FieldVector::getField).toList(),
                retained,
                root.getRowCount()),
            deltaSchema,
            rowIds),
        Optional.empty());
  }

  private void release() {
    if (remaining.decrementAndGet() == 0) {
      root.close();
    }
  }

  private static final class Row implements KernelBatchRowData {
    private final ArrowKernelRows owner;
    private final int rowId;
    private RowKind rowKind;
    private boolean closed;

    private Row(ArrowKernelRows owner, int rowId, RowKind kind) {
      this.owner = owner;
      this.rowId = rowId;
      this.rowKind = kind;
    }

    @Override public Object batchIdentity() { return owner; }
    @Override public int rowId() { return rowId; }
    @Override public FilteredColumnarBatch selectRows(int[] rowIds) { return owner.select(rowIds); }
    @Override public RowKind getRowKind() { return rowKind; }
    @Override public void setRowKind(RowKind kind) { rowKind = kind; }
    @Override public int getArity() { return owner.flinkBatch.getArity(); }
    @Override public boolean isNullAt(int pos) { return owner.flinkBatch.isNullAt(rowId, pos); }
    @Override public boolean getBoolean(int pos) { return owner.flinkBatch.getBoolean(rowId, pos); }
    @Override public byte getByte(int pos) { return owner.flinkBatch.getByte(rowId, pos); }
    @Override public short getShort(int pos) { return owner.flinkBatch.getShort(rowId, pos); }
    @Override public int getInt(int pos) { return owner.flinkBatch.getInt(rowId, pos); }
    @Override public long getLong(int pos) { return owner.flinkBatch.getLong(rowId, pos); }
    @Override public float getFloat(int pos) { return owner.flinkBatch.getFloat(rowId, pos); }
    @Override public double getDouble(int pos) { return owner.flinkBatch.getDouble(rowId, pos); }
    @Override public StringData getString(int pos) {
      Bytes bytes = owner.flinkBatch.getByteArray(rowId, pos);
      return StringData.fromBytes(bytes.data, bytes.offset, bytes.len);
    }
    @Override public DecimalData getDecimal(int pos, int precision, int scale) { return owner.flinkBatch.getDecimal(rowId, pos, precision, scale); }
    @Override public TimestampData getTimestamp(int pos, int precision) { return owner.flinkBatch.getTimestamp(rowId, pos, precision); }
    @Override public <T> RawValueData<T> getRawValue(int pos) { throw new UnsupportedOperationException("RawValueData is not supported"); }
    @Override public byte[] getBinary(int pos) {
      Bytes bytes = owner.flinkBatch.getByteArray(rowId, pos);
      if (bytes.offset == 0 && bytes.len == bytes.data.length) {
        return bytes.data;
      }
      return java.util.Arrays.copyOfRange(bytes.data, bytes.offset, bytes.offset + bytes.len);
    }
    @Override public RowData getRow(int pos, int numFields) { return owner.flinkBatch.getRow(rowId, pos); }
    @Override public ArrayData getArray(int pos) { return owner.flinkBatch.getArray(rowId, pos); }
    @Override public MapData getMap(int pos) { return owner.flinkBatch.getMap(rowId, pos); }
    @Override public Variant getVariant(int pos) { return owner.flinkBatch.getVariant(rowId, pos); }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        owner.release();
      }
    }
  }
}

package io.github.jordepic.streamfusion.operator;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/**
 * A reusable, zero-copy view that presents a wide source {@link RowData} as a narrower pruned schema,
 * recursively — top-level columns and, within {@code ROW} columns, nested sub-fields. Each field is
 * matched to the source by name (names are unique within a row type), so {@link #getRow} returns a
 * child {@code PrunedRowData} over the source struct rather than the full struct.
 *
 * <p>Used by the entry transpose for nested projection pushdown: the converter, driven by the pruned
 * schema, builds and fills only the Arrow columns the native region actually reads — the unread fields
 * of a wide source row (e.g. a Nexmark {@code bid.channel}/{@code bid.url}) never touch Arrow. Reusable
 * like Flink's {@link org.apache.flink.table.data.utils.ProjectedRowData} (which is top-level only):
 * {@link #replaceRow} repoints it, and the converter reads each row inline before the next.
 */
public final class PrunedRowData implements RowData {

  private final int[] indexMapping;
  private final PrunedRowData[] children; // non-null only for nested ROW fields
  private final int[] sourceArity; // source struct field count, where children[k] != null
  private RowData row;

  private PrunedRowData(int[] indexMapping, PrunedRowData[] children, int[] sourceArity) {
    this.indexMapping = indexMapping;
    this.children = children;
    this.sourceArity = sourceArity;
  }

  /** Builds a projector mapping each field of {@code pruned} to the like-named field of {@code source}. */
  public static PrunedRowData of(RowType source, RowType pruned) {
    int n = pruned.getFieldCount();
    int[] indexMapping = new int[n];
    PrunedRowData[] children = new PrunedRowData[n];
    int[] sourceArity = new int[n];
    for (int k = 0; k < n; k++) {
      int src = source.getFieldNames().indexOf(pruned.getFieldNames().get(k));
      indexMapping[k] = src;
      LogicalType prunedType = pruned.getTypeAt(k);
      LogicalType sourceType = source.getTypeAt(src);
      if (prunedType.getTypeRoot() == LogicalTypeRoot.ROW
          && sourceType.getTypeRoot() == LogicalTypeRoot.ROW) {
        children[k] = of((RowType) sourceType, (RowType) prunedType);
        sourceArity[k] = ((RowType) sourceType).getFieldCount();
      }
    }
    return new PrunedRowData(indexMapping, children, sourceArity);
  }

  public PrunedRowData replaceRow(RowData row) {
    this.row = row;
    return this;
  }

  @Override
  public RowData getRow(int pos, int numFields) {
    if (children[pos] != null) {
      return children[pos].replaceRow(row.getRow(indexMapping[pos], sourceArity[pos]));
    }
    return row.getRow(indexMapping[pos], numFields);
  }

  @Override
  public int getArity() {
    return indexMapping.length;
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
    return row.isNullAt(indexMapping[pos]);
  }

  @Override
  public boolean getBoolean(int pos) {
    return row.getBoolean(indexMapping[pos]);
  }

  @Override
  public byte getByte(int pos) {
    return row.getByte(indexMapping[pos]);
  }

  @Override
  public short getShort(int pos) {
    return row.getShort(indexMapping[pos]);
  }

  @Override
  public int getInt(int pos) {
    return row.getInt(indexMapping[pos]);
  }

  @Override
  public long getLong(int pos) {
    return row.getLong(indexMapping[pos]);
  }

  @Override
  public float getFloat(int pos) {
    return row.getFloat(indexMapping[pos]);
  }

  @Override
  public double getDouble(int pos) {
    return row.getDouble(indexMapping[pos]);
  }

  @Override
  public StringData getString(int pos) {
    return row.getString(indexMapping[pos]);
  }

  @Override
  public DecimalData getDecimal(int pos, int precision, int scale) {
    return row.getDecimal(indexMapping[pos], precision, scale);
  }

  @Override
  public TimestampData getTimestamp(int pos, int precision) {
    return row.getTimestamp(indexMapping[pos], precision);
  }

  @Override
  public <T> RawValueData<T> getRawValue(int pos) {
    return row.getRawValue(indexMapping[pos]);
  }

  @Override
  public byte[] getBinary(int pos) {
    return row.getBinary(indexMapping[pos]);
  }

  @Override
  public org.apache.flink.types.variant.Variant getVariant(int pos) {
    return row.getVariant(indexMapping[pos]);
  }

  @Override
  public ArrayData getArray(int pos) {
    return row.getArray(indexMapping[pos]);
  }

  @Override
  public MapData getMap(int pos) {
    return row.getMap(indexMapping[pos]);
  }
}

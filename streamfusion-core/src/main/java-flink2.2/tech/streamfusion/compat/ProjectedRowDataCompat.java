package tech.streamfusion.compat;

import org.apache.flink.table.data.RowData;

/** The RowData methods introduced after Flink 1.18, backed by the shared projection. */
public abstract class ProjectedRowDataCompat implements RowData {
  protected abstract RowData sourceRow();

  protected abstract int sourceIndex(int pos);

  @Override
  public org.apache.flink.types.variant.Variant getVariant(int pos) {
    return sourceRow().getVariant(sourceIndex(pos));
  }
}

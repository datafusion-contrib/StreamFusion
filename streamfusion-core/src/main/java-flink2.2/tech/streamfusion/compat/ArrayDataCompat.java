package tech.streamfusion.compat;

public abstract class ArrayDataCompat implements org.apache.flink.table.data.ArrayData {
  protected abstract org.apache.flink.table.data.ArrayData sourceArray();

  @Override
  public org.apache.flink.types.variant.Variant getVariant(int pos) {
    return sourceArray().getVariant(pos);
  }
}

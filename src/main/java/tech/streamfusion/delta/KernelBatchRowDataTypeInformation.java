package tech.streamfusion.delta;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;

/** Ownership-transfer type used only on the chained Arrow-view-to-Delta-writer edge. */
public final class KernelBatchRowDataTypeInformation extends TypeInformation<RowData> {
  public static final KernelBatchRowDataTypeInformation INSTANCE =
      new KernelBatchRowDataTypeInformation();

  private KernelBatchRowDataTypeInformation() {}

  @Override public boolean isBasicType() { return false; }
  @Override public boolean isTupleType() { return false; }
  @Override public int getArity() { return 1; }
  @Override public int getTotalFields() { return 1; }
  @Override public Class<RowData> getTypeClass() { return RowData.class; }
  @Override public boolean isKeyType() { return false; }
  @Override public TypeSerializer<RowData> createSerializer(SerializerConfig config) {
    return new KernelBatchRowDataSerializer();
  }
  @Override public String toString() { return "KernelBatchRowData"; }
  @Override public boolean equals(Object other) {
    return other instanceof KernelBatchRowDataTypeInformation;
  }
  @Override public int hashCode() { return KernelBatchRowDataTypeInformation.class.hashCode(); }
  @Override public boolean canEqual(Object other) {
    return other instanceof KernelBatchRowDataTypeInformation;
  }
}

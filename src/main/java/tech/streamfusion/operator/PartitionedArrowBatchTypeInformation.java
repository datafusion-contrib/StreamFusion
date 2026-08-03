package tech.streamfusion.operator;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * The stream element type on the edge from the sink's partition splitter to its file writer: one
 * bucket-routed {@link PartitionedArrowBatch} per record.
 */
public final class PartitionedArrowBatchTypeInformation
    extends TypeInformation<PartitionedArrowBatch> {

  public static final PartitionedArrowBatchTypeInformation INSTANCE =
      new PartitionedArrowBatchTypeInformation();

  @Override
  public boolean isBasicType() {
    return false;
  }

  @Override
  public boolean isTupleType() {
    return false;
  }

  @Override
  public int getArity() {
    return 1;
  }

  @Override
  public int getTotalFields() {
    return 1;
  }

  @Override
  public Class<PartitionedArrowBatch> getTypeClass() {
    return PartitionedArrowBatch.class;
  }

  @Override
  public boolean isKeyType() {
    return false;
  }

  @Override
  public TypeSerializer<PartitionedArrowBatch> createSerializer(SerializerConfig config) {
    return new PartitionedArrowBatchSerializer();
  }

  @Override
  public String toString() {
    return "PartitionedArrowBatch";
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof PartitionedArrowBatchTypeInformation;
  }

  @Override
  public int hashCode() {
    return PartitionedArrowBatchTypeInformation.class.hashCode();
  }

  @Override
  public boolean canEqual(Object obj) {
    return obj instanceof PartitionedArrowBatchTypeInformation;
  }
}

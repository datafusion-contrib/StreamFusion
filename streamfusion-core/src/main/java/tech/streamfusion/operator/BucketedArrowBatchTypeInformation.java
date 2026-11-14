package tech.streamfusion.operator;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import tech.streamfusion.compat.FixedSerializerTypeInformation;

/**
 * The stream element type on the edge from a sink's bucket router to its writer: one
 * destination-routed {@link BucketedArrowBatch} per record.
 */
public final class BucketedArrowBatchTypeInformation
    extends FixedSerializerTypeInformation<BucketedArrowBatch> {

  public static final BucketedArrowBatchTypeInformation INSTANCE =
      new BucketedArrowBatchTypeInformation();

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
  public Class<BucketedArrowBatch> getTypeClass() {
    return BucketedArrowBatch.class;
  }

  @Override
  public boolean isKeyType() {
    return false;
  }

  @Override
  public TypeSerializer<BucketedArrowBatch> createSerializer() {
    return new BucketedArrowBatchSerializer();
  }

  @Override
  public String toString() {
    return "BucketedArrowBatch";
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof BucketedArrowBatchTypeInformation;
  }

  @Override
  public int hashCode() {
    return BucketedArrowBatchTypeInformation.class.hashCode();
  }

  @Override
  public boolean canEqual(Object obj) {
    return obj instanceof BucketedArrowBatchTypeInformation;
  }
}

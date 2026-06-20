package io.github.jordepic.streamfusion.operator;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * The stream element type for the columnar edges between native operators: one {@link ArrowBatch}
 * per record. Declaring this on a native operator's output transformation is what tells Flink to
 * carry Arrow batches (via {@link ArrowBatchSerializer}) rather than rows on that edge.
 */
public final class ArrowBatchTypeInformation extends TypeInformation<ArrowBatch> {

  public static final ArrowBatchTypeInformation INSTANCE = new ArrowBatchTypeInformation();

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
  public Class<ArrowBatch> getTypeClass() {
    return ArrowBatch.class;
  }

  @Override
  public boolean isKeyType() {
    return false;
  }

  @Override
  public TypeSerializer<ArrowBatch> createSerializer(SerializerConfig config) {
    return new ArrowBatchSerializer();
  }

  @Override
  public String toString() {
    return "ArrowBatch";
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof ArrowBatchTypeInformation;
  }

  @Override
  public int hashCode() {
    return ArrowBatchTypeInformation.class.hashCode();
  }

  @Override
  public boolean canEqual(Object obj) {
    return obj instanceof ArrowBatchTypeInformation;
  }
}

package tech.streamfusion.operator;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer;

/**
 * The stream element type for the raw-message edge between Flink's byte source and the native
 * decode operator. A Kafka record's value can be null (a compacted-topic tombstone), and each
 * format decoder owns that record's semantics — the CDC envelopes and the Avro/JSON formats skip
 * it, raw decodes it to a null field — so the edge must carry null through the chain's serializer
 * copy, which Flink's plain {@code byte[]} serializer cannot.
 */
public final class NullableBytesTypeInformation extends TypeInformation<byte[]> {

  public static final NullableBytesTypeInformation INSTANCE = new NullableBytesTypeInformation();

  private NullableBytesTypeInformation() {}

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
  public Class<byte[]> getTypeClass() {
    return byte[].class;
  }

  @Override
  public boolean isKeyType() {
    return false;
  }

  @Override
  public TypeSerializer<byte[]> createSerializer(SerializerConfig config) {
    return NullableSerializer.wrap(BytePrimitiveArraySerializer.INSTANCE, false);
  }

  @Override
  public String toString() {
    return "NullableBytes";
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof NullableBytesTypeInformation;
  }

  @Override
  public int hashCode() {
    return NullableBytesTypeInformation.class.hashCode();
  }

  @Override
  public boolean canEqual(Object other) {
    return other instanceof NullableBytesTypeInformation;
  }
}

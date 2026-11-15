package tech.streamfusion.compat;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/** Adapts Flink's serializer factory signature for types with an explicit serializer. */
public abstract class FixedSerializerTypeInformation<T> extends TypeInformation<T> {
  @Override
  public final TypeSerializer<T> createSerializer(SerializerConfig config) {
    return createSerializer();
  }

  protected abstract TypeSerializer<T> createSerializer();
}

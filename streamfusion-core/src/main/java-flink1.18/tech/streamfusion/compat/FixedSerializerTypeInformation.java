package tech.streamfusion.compat;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/** Adapts Flink's serializer factory signature for types with an explicit serializer. */
public abstract class FixedSerializerTypeInformation<T> extends TypeInformation<T> {
  @Override
  public final TypeSerializer<T> createSerializer(ExecutionConfig config) {
    return createSerializer();
  }

  protected abstract TypeSerializer<T> createSerializer();
}

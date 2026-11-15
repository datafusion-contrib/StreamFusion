package tech.streamfusion.state;

import java.io.IOException;
import java.util.Base64;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import tech.streamfusion.compat.StateCompat;

/** Observes heap copy-on-write for each native OVER partition through released keyed-state APIs. */
public final class OverHeapSnapshotProbe {
  private static final ValueStateDescriptor<Probe> DESCRIPTOR =
      new ValueStateDescriptor<>("__streamfusion_over_heap_copy_probe_v1", new ProbeSerializer());
  private static final ValueStateDescriptor<Probe> VALUE_DESCRIPTOR =
      new ValueStateDescriptor<>("__streamfusion_over_value_copy_probe_v1", new ProbeSerializer());
  private final CheckpointableKeyedStateBackend<Object> backend;

  @SuppressWarnings("unchecked")
  public OverHeapSnapshotProbe(CheckpointableKeyedStateBackend<?> backend) {
    if (!(backend instanceof AbstractKeyedStateBackend)) {
      throw new IllegalArgumentException("OVER heap copy probe requires a heap keyed backend");
    }
    this.backend = (CheckpointableKeyedStateBackend<Object>) backend;
  }

  /** Called once per touched partition, before the native input map is read or changed. */
  public boolean touch(int keyGroup, byte[] partition) throws Exception {
    return touchState(keyGroup, partition, DESCRIPTOR);
  }

  /**
   * Value maps are first read at firing, which can follow input-map copy and snapshot completion.
   */
  public boolean touchValues(int keyGroup, byte[] partition) throws Exception {
    return touchState(keyGroup, partition, VALUE_DESCRIPTOR);
  }

  private boolean touchState(int keyGroup, byte[] partition, ValueStateDescriptor<Probe> descriptor)
      throws Exception {
    var keyed = (AbstractKeyedStateBackend<?>) backend;
    Object previousKey = keyed.getCurrentKey();
    int previousGroup = keyed.getCurrentKeyGroupIndex();
    try {
      StateCompat.setCurrentKeyAndGroup(backend, keyGroup, keyGroup);
      ValueState<Probe> state =
          backend.getPartitionedState(
              Base64.getEncoder().encodeToString(partition), StringSerializer.INSTANCE, descriptor);
      Probe value = state.value();
      if (value == null) {
        state.update(new Probe(false));
        return false;
      }
      boolean copied = value.copied;
      value.copied = false;
      return copied;
    } finally {
      restoreKey(keyed, previousKey, previousGroup);
    }
  }

  /** A cleared native frame also drops its managed copy-on-write marker. */
  public void clear(int keyGroup, byte[] partition) throws Exception {
    var keyed = (AbstractKeyedStateBackend<?>) backend;
    Object previousKey = keyed.getCurrentKey();
    int previousGroup = keyed.getCurrentKeyGroupIndex();
    try {
      StateCompat.setCurrentKeyAndGroup(backend, keyGroup, keyGroup);
      backend
          .getPartitionedState(
              Base64.getEncoder().encodeToString(partition), StringSerializer.INSTANCE, DESCRIPTOR)
          .clear();
      backend
          .getPartitionedState(
              Base64.getEncoder().encodeToString(partition),
              StringSerializer.INSTANCE,
              VALUE_DESCRIPTOR)
          .clear();
    } finally {
      restoreKey(keyed, previousKey, previousGroup);
    }
  }

  private void restoreKey(
      AbstractKeyedStateBackend<?> keyed, Object previousKey, int previousGroup) {
    if (previousKey == null) CanonicalNativeState.clearKeyContext(keyed);
    else StateCompat.setCurrentKeyAndGroup(backend, previousKey, previousGroup);
  }

  private static final class Probe {
    private boolean copied;

    private Probe(boolean copied) {
      this.copied = copied;
    }
  }

  private static final class ProbeSerializer extends TypeSerializer<Probe> {
    @Override
    public boolean isImmutableType() {
      return false;
    }

    @Override
    public TypeSerializer<Probe> duplicate() {
      return this;
    }

    @Override
    public Probe createInstance() {
      return new Probe(false);
    }

    @Override
    public Probe copy(Probe from) {
      return new Probe(true);
    }

    @Override
    public Probe copy(Probe from, Probe reuse) {
      return copy(from);
    }

    @Override
    public int getLength() {
      return 1;
    }

    @Override
    public void serialize(Probe value, DataOutputView target) throws IOException {
      target.writeBoolean(false);
    }

    @Override
    public Probe deserialize(DataInputView source) throws IOException {
      source.readBoolean();
      return new Probe(false);
    }

    @Override
    public Probe deserialize(Probe reuse, DataInputView source) throws IOException {
      return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
      source.readBoolean();
      target.writeBoolean(false);
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof ProbeSerializer;
    }

    @Override
    public int hashCode() {
      return ProbeSerializer.class.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<Probe> snapshotConfiguration() {
      return new SerializerSnapshot();
    }
  }

  public static final class SerializerSnapshot extends SimpleTypeSerializerSnapshot<Probe> {
    public SerializerSnapshot() {
      super(ProbeSerializer::new);
    }
  }
}

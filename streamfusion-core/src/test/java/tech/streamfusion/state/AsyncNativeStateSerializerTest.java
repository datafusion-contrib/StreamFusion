package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

class AsyncNativeStateSerializerTest {

  private final AsyncNativeStateSerializer serializer = new AsyncNativeStateSerializer();

  @Test
  void restoredPartitionRoundTripsWithChecksum() throws Exception {
    byte[] partition = new byte[] {1, 2, 3, 4, 5};
    DataOutputSerializer output = new DataOutputSerializer(32);
    serializer.serialize(AsyncNativeStateSnapshot.restored(partition), output);

    AsyncNativeStateSnapshot restored =
        serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));

    assertArrayEquals(partition, restored.materialize());
  }

  @Test
  void corruptPartitionIsRejected() throws Exception {
    DataOutputSerializer output = new DataOutputSerializer(32);
    serializer.serialize(
        AsyncNativeStateSnapshot.restored(new byte[] {9, 8, 7}), output);
    byte[] corrupted = output.getCopyOfBuffer();
    corrupted[Integer.BYTES] ^= 1;

    assertThrows(
        IOException.class,
        () -> serializer.deserialize(new DataInputDeserializer(corrupted)));
  }
}

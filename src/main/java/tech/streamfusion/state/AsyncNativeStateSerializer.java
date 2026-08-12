package tech.streamfusion.state;

import java.io.IOException;
import java.util.zip.CRC32C;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/** Serializer that deliberately calls {@link AsyncNativeStateSnapshot#materialize()} asynchronously. */
final class AsyncNativeStateSerializer extends TypeSerializer<AsyncNativeStateSnapshot> {

  @Override
  public boolean isImmutableType() {
    return true;
  }

  @Override
  public TypeSerializer<AsyncNativeStateSnapshot> duplicate() {
    return this;
  }

  @Override
  public AsyncNativeStateSnapshot createInstance() {
    return AsyncNativeStateSnapshot.restored(new byte[0]);
  }

  @Override
  public AsyncNativeStateSnapshot copy(AsyncNativeStateSnapshot from) {
    return from;
  }

  @Override
  public AsyncNativeStateSnapshot copy(
      AsyncNativeStateSnapshot from, AsyncNativeStateSnapshot reuse) {
    return from;
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(AsyncNativeStateSnapshot value, DataOutputView target) throws IOException {
    byte[] bytes = value.materialize();
    target.writeInt(bytes.length);
    target.write(bytes);
    target.writeInt(checksum(bytes));
  }

  @Override
  public AsyncNativeStateSnapshot deserialize(DataInputView source) throws IOException {
    int length = source.readInt();
    if (length < 0) {
      throw new IOException("negative asynchronous native-state partition length " + length);
    }
    byte[] bytes = new byte[length];
    source.readFully(bytes);
    int expected = source.readInt();
    if (checksum(bytes) != expected) {
      throw new IOException("asynchronous native-state partition checksum mismatch");
    }
    return AsyncNativeStateSnapshot.restored(bytes);
  }

  @Override
  public AsyncNativeStateSnapshot deserialize(
      AsyncNativeStateSnapshot reuse, DataInputView source) throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    int length = source.readInt();
    if (length < 0) {
      throw new IOException("negative asynchronous native-state partition length " + length);
    }
    target.writeInt(length);
    byte[] buffer = new byte[Math.min(length, 64 * 1024)];
    int remaining = length;
    while (remaining > 0) {
      int chunk = Math.min(remaining, buffer.length);
      source.readFully(buffer, 0, chunk);
      target.write(buffer, 0, chunk);
      remaining -= chunk;
    }
    target.writeInt(source.readInt());
  }

  private static int checksum(byte[] bytes) {
    CRC32C checksum = new CRC32C();
    checksum.update(bytes, 0, bytes.length);
    return (int) checksum.getValue();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof AsyncNativeStateSerializer;
  }

  @Override
  public int hashCode() {
    return AsyncNativeStateSerializer.class.hashCode();
  }

  @Override
  public TypeSerializerSnapshot<AsyncNativeStateSnapshot> snapshotConfiguration() {
    return new SerializerSnapshot();
  }

  public static final class SerializerSnapshot
      extends SimpleTypeSerializerSnapshot<AsyncNativeStateSnapshot> {
    public SerializerSnapshot() {
      super(AsyncNativeStateSerializer::new);
    }
  }
}

package tech.streamfusion.operator;

import java.io.IOException;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Type serializer for {@link BucketedArrowBatch}: the {@link ArrowBatchSerializer} contract —
 * identity {@link #copy} within a chained task, Arrow IPC across a network edge — with the
 * partition bytes and bucket carried alongside the framed batch.
 */
public final class BucketedArrowBatchSerializer extends TypeSerializer<BucketedArrowBatch> {

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public TypeSerializer<BucketedArrowBatch> duplicate() {
    return new BucketedArrowBatchSerializer();
  }

  @Override
  public BucketedArrowBatch createInstance() {
    return null;
  }

  @Override
  public BucketedArrowBatch copy(BucketedArrowBatch from) {
    return from;
  }

  @Override
  public BucketedArrowBatch copy(BucketedArrowBatch from, BucketedArrowBatch reuse) {
    return from;
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(BucketedArrowBatch batch, DataOutputView target) throws IOException {
    target.writeInt(batch.partition().length);
    target.write(batch.partition());
    target.writeInt(batch.bucket());
    ArrowIpcFrame.write(batch.root(), target);
  }

  @Override
  public BucketedArrowBatch deserialize(DataInputView source) throws IOException {
    byte[] partition = new byte[source.readInt()];
    source.readFully(partition);
    int bucket = source.readInt();
    return new BucketedArrowBatch(
        ArrowIpcFrame.read(source, NativeAllocator.SHARED), partition, bucket);
  }

  @Override
  public BucketedArrowBatch deserialize(BucketedArrowBatch reuse, DataInputView source)
      throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    byte[] partition = new byte[source.readInt()];
    source.readFully(partition);
    target.writeInt(partition.length);
    target.write(partition);
    target.writeInt(source.readInt());
    ArrowIpcFrame.copy(source, target);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof BucketedArrowBatchSerializer;
  }

  @Override
  public int hashCode() {
    return BucketedArrowBatchSerializer.class.hashCode();
  }

  @Override
  public TypeSerializerSnapshot<BucketedArrowBatch> snapshotConfiguration() {
    return new BucketedArrowBatchSerializerSnapshot();
  }

  /** Snapshot for the stateless {@link BucketedArrowBatchSerializer}. */
  public static final class BucketedArrowBatchSerializerSnapshot
      extends SimpleTypeSerializerSnapshot<BucketedArrowBatch> {
    public BucketedArrowBatchSerializerSnapshot() {
      super(BucketedArrowBatchSerializer::new);
    }
  }
}

package tech.streamfusion.operator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Type serializer for {@link ArrowBatch}. Within a chained task it is never asked to serialize to
 * bytes — {@link #copy} is the only path, and it is identity because operators emit a fresh batch
 * per record and never retain or mutate it after emit. Across a network edge it has two formats,
 * dispatched by the frame tag on the read side:
 *
 * <ul>
 *   <li>the IPC format — Arrow's IPC stream encoding, preserving the columnar exchange's
 *       key-group tag before the length-framed payload — valid across any process boundary;
 *   <li>the zero-copy format (write side opt-in via the constructor flag) — the batch is parked in
 *       {@link ArrowBatchHandles} and only a token-guarded handle crosses the wire, so a shuffle
 *       whose endpoints share the JVM moves Arrow buffers by ownership transfer, not by bytes.
 * </ul>
 */
public final class ArrowBatchSerializer extends TypeSerializer<ArrowBatch> {

  // Negative so they cannot be confused with the non-negative IPC length in the legacy format.
  // Keep the existing wire tag: only the meaning of its integer changes from channel to key group.
  private static final int KEY_GROUP_TAG = 0xD5A5_0001;
  private static final int ZERO_COPY_TAG = 0xD5A5_0002;
  private static final int ORDERED_KEY_GROUP_TAG = 0xD5A5_0003;

  private final boolean zeroCopy;

  public ArrowBatchSerializer() {
    this(false);
  }

  public ArrowBatchSerializer(boolean zeroCopy) {
    this.zeroCopy = zeroCopy;
  }

  private BufferAllocator allocator() {
    return NativeAllocator.SHARED;
  }

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public TypeSerializer<ArrowBatch> duplicate() {
    return new ArrowBatchSerializer(zeroCopy);
  }

  @Override
  public ArrowBatch createInstance() {
    return null;
  }

  // Identity: a batch is produced fresh and handed off, so the consumer can take it as-is.
  @Override
  public ArrowBatch copy(ArrowBatch from) {
    return from;
  }

  @Override
  public ArrowBatch copy(ArrowBatch from, ArrowBatch reuse) {
    return from;
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(ArrowBatch batch, DataOutputView target) throws IOException {
    if (zeroCopy) {
      // Ownership moves to the handle table; the claiming deserializer takes it back untouched.
      target.writeInt(ZERO_COPY_TAG);
      target.writeLong(ArrowBatchHandles.TOKEN_HI);
      target.writeLong(ArrowBatchHandles.TOKEN_LO);
      target.writeLong(ArrowBatchHandles.register(batch));
      return;
    }
    long started = System.nanoTime();
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    // One root() take per serialization: under a shared batch each take is a distinct retained
    // view, so taking once and closing that same root keeps the reference counts balanced.
    VectorSchemaRoot root = batch.root();
    try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, bytes)) {
      writer.start();
      writer.writeBatch();
      writer.end();
    } finally {
      // Serializing ships the batch onto the network edge — its terminal use on the write side, so
      // release the off-heap buffers here (the read side allocates a fresh batch on deserialize).
      root.close();
    }
    byte[] encoded = bytes.toByteArray();
    target.writeInt(batch.isOrderedKeyGroupFragment() ? ORDERED_KEY_GROUP_TAG : KEY_GROUP_TAG);
    target.writeInt(batch.keyGroup());
    if (batch.isOrderedKeyGroupFragment()) {
      target.writeLong(batch.parentEpochHigh());
      target.writeLong(batch.parentEpochLow());
      target.writeLong(batch.parentSequence());
      writeInts(target, batch.rowOrdinals());
      writeInts(target, batch.parentKeyGroups());
    }
    target.writeInt(encoded.length);
    target.write(encoded);
    batch.recordEncodeNanos(System.nanoTime() - started);
  }

  @Override
  public ArrowBatch deserialize(DataInputView source) throws IOException {
    int tagOrLength = source.readInt();
    if (tagOrLength == ZERO_COPY_TAG) {
      return ArrowBatchHandles.claim(source.readLong(), source.readLong(), source.readLong());
    }
    boolean ordered = tagOrLength == ORDERED_KEY_GROUP_TAG;
    boolean tagged = tagOrLength == KEY_GROUP_TAG || ordered;
    int keyGroup = tagged ? source.readInt() : -1;
    long epochHigh = ordered ? source.readLong() : 0;
    long epochLow = ordered ? source.readLong() : 0;
    long sequence = ordered ? source.readLong() : -1;
    int[] ordinals = ordered ? readInts(source) : null;
    int[] parentKeyGroups = ordered ? readInts(source) : null;
    int length = tagged ? source.readInt() : tagOrLength;
    byte[] encoded = new byte[length];
    source.readFully(encoded);
    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(encoded), allocator())) {
      reader.loadNextBatch();
      VectorSchemaRoot read = reader.getVectorSchemaRoot();
      // Transfer the buffers out of the reader so the batch outlives it (closing the reader then
      // frees nothing — the vectors are now owned by the returned root).
      List<FieldVector> transferred = new ArrayList<>();
      for (FieldVector vector : read.getFieldVectors()) {
        TransferPair pair = vector.getTransferPair(allocator());
        pair.transfer();
        transferred.add((FieldVector) pair.getTo());
      }
      VectorSchemaRoot root = new VectorSchemaRoot(transferred);
      root.setRowCount(read.getRowCount());
      return ordered
          ? new ArrowBatch(
              root,
              keyGroup,
              ArrowBatch.NO_HANDLE_OWNER,
              null,
              epochHigh,
              epochLow,
              sequence,
              ordinals,
              parentKeyGroups)
          : new ArrowBatch(root, keyGroup);
    }
  }

  @Override
  public ArrowBatch deserialize(ArrowBatch reuse, DataInputView source) throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    int tagOrLength = source.readInt();
    if (tagOrLength == ZERO_COPY_TAG) {
      target.writeInt(ZERO_COPY_TAG);
      target.writeLong(source.readLong());
      target.writeLong(source.readLong());
      target.writeLong(source.readLong());
      return;
    }
    boolean ordered = tagOrLength == ORDERED_KEY_GROUP_TAG;
    boolean tagged = tagOrLength == KEY_GROUP_TAG || ordered;
    int keyGroup = tagged ? source.readInt() : -1;
    long epochHigh = ordered ? source.readLong() : 0;
    long epochLow = ordered ? source.readLong() : 0;
    long sequence = ordered ? source.readLong() : -1;
    int[] ordinals = ordered ? readInts(source) : null;
    int[] parentKeyGroups = ordered ? readInts(source) : null;
    int length = tagged ? source.readInt() : tagOrLength;
    byte[] encoded = new byte[length];
    source.readFully(encoded);
    target.writeInt(ordered ? ORDERED_KEY_GROUP_TAG : KEY_GROUP_TAG);
    target.writeInt(keyGroup);
    if (ordered) {
      target.writeLong(epochHigh);
      target.writeLong(epochLow);
      target.writeLong(sequence);
      writeInts(target, ordinals);
      writeInts(target, parentKeyGroups);
    }
    target.writeInt(length);
    target.write(encoded);
  }

  private static void writeInts(DataOutputView target, int[] values) throws IOException {
    target.writeInt(values.length);
    for (int value : values) {
      target.writeInt(value);
    }
  }

  private static int[] readInts(DataInputView source) throws IOException {
    int[] values = new int[source.readInt()];
    for (int i = 0; i < values.length; i++) {
      values[i] = source.readInt();
    }
    return values;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof ArrowBatchSerializer && ((ArrowBatchSerializer) obj).zeroCopy == zeroCopy;
  }

  @Override
  public int hashCode() {
    return ArrowBatchSerializer.class.hashCode() + (zeroCopy ? 1 : 0);
  }

  @Override
  public TypeSerializerSnapshot<ArrowBatch> snapshotConfiguration() {
    return new ArrowBatchSerializerSnapshot();
  }

  /** Snapshot for the stateless {@link ArrowBatchSerializer}. */
  public static final class ArrowBatchSerializerSnapshot
      extends SimpleTypeSerializerSnapshot<ArrowBatch> {
    public ArrowBatchSerializerSnapshot() {
      super(ArrowBatchSerializer::new);
    }
  }
}

package tech.streamfusion.state;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.util.CloseableIterator;
import tech.streamfusion.compat.FlinkStateBackendCompat;

/** Stages map-list rows once and lets the released stock serializer iterate each bucket once. */
final class StockMapListSpool<K, V> implements AutoCloseable {
  private static final int ROWS = 1, COUNTS = 2, METADATA = 3;
  private final TypeSerializer<RowData> owners;
  private final TypeSerializer<V> rows;
  private final TypeSerializer<K> mapKeys;
  private final StockStateSpool spool;

  StockMapListSpool(
      TypeSerializer<RowData> owners, TypeSerializer<K> mapKeys, TypeSerializer<V> rows)
      throws Exception {
    this(owners, mapKeys, rows, Path.of(System.getProperty("java.io.tmpdir")));
  }

  StockMapListSpool(
      TypeSerializer<RowData> owners,
      TypeSerializer<K> mapKeys,
      TypeSerializer<V> rows,
      Path directory)
      throws Exception {
    this.owners = owners.duplicate();
    this.mapKeys = mapKeys.duplicate();
    this.rows = rows.duplicate();
    spool =
        new StockStateSpool(
            new RowDataSerializer(
                RowType.of(
                    new VarBinaryType(VarBinaryType.MAX_LENGTH),
                    new VarBinaryType(VarBinaryType.MAX_LENGTH))),
            directory);
  }

  private static <V> byte[] encode(TypeSerializer<V> serializer, V value) throws Exception {
    var output = new DataOutputSerializer(64);
    serializer.serialize(value, output);
    return output.getCopyOfBuffer();
  }

  private RowData bucket(RowData owner, K mapKey) throws Exception {
    return GenericRowData.of(encode(owners, owner), encode(mapKeys, mapKey));
  }

  boolean start(RowData owner, K mapKey, long metadata) throws Exception {
    var bucket = bucket(owner, mapKey);
    if (spool.contains(COUNTS, bucket, 0)) return false;
    spool.put(COUNTS, bucket, 0, ByteBuffer.allocate(4).putInt(0).array());
    spool.put(METADATA, bucket, 0, ByteBuffer.allocate(8).putLong(metadata).array());
    spool.addOwner(bucket);
    return true;
  }

  Long metadata(RowData owner, K mapKey) throws Exception {
    byte[] value = spool.get(METADATA, bucket(owner, mapKey), 0);
    return value == null ? null : ByteBuffer.wrap(value).getLong();
  }

  boolean append(RowData owner, K mapKey, V row, long metadata) throws Exception {
    boolean first = append(owner, mapKey, row);
    spool.put(METADATA, bucket(owner, mapKey), 0, ByteBuffer.allocate(8).putLong(metadata).array());
    return first;
  }

  /** Returns true only for the first row of this owner/map-key bucket. */
  boolean append(RowData owner, K mapKey, V row) throws Exception {
    var bucket = bucket(owner, mapKey);
    byte[] count = spool.get(COUNTS, bucket, 0);
    int position = count == null ? 0 : ByteBuffer.wrap(count).getInt();
    int next = Math.addExact(position, 1);
    byte[] encoded = encode(rows, row);
    spool.put(ROWS, bucket, position, encoded);
    spool.put(COUNTS, bucket, 0, ByteBuffer.allocate(4).putInt(next).array());
    if (count == null) spool.addOwner(bucket);
    return count == null;
  }

  @FunctionalInterface
  interface BucketReceiver<K, V> {
    void accept(RowData owner, K mapKey, List<V> rows) throws Exception;
  }

  void visit(CheckpointableKeyedStateBackend<RowData> backend, BucketReceiver<K, V> receiver)
      throws Exception {
    visit(FlinkStateBackendCompat.serializesKeyedValues(backend), receiver);
  }

  @FunctionalInterface
  interface MetadataReceiver<K, V> {
    void accept(RowData owner, K mapKey, List<V> rows, long metadata) throws Exception;
  }

  void visitWithMetadata(
      CheckpointableKeyedStateBackend<RowData> backend, MetadataReceiver<K, V> receiver)
      throws Exception {
    visitWithMetadata(FlinkStateBackendCompat.serializesKeyedValues(backend), receiver);
  }

  void visit(boolean serialized, BucketReceiver<K, V> receiver) throws Exception {
    visitWithMetadata(
        serialized, (owner, key, rows, metadata) -> receiver.accept(owner, key, rows));
  }

  void visitWithMetadata(boolean serialized, MetadataReceiver<K, V> receiver) throws Exception {
    try (var buckets = spool.owners()) {
      while (buckets.hasNext()) {
        var bucket = buckets.next();
        var owner = owners.deserialize(new DataInputDeserializer(bucket.getBinary(0)));
        K mapKey = mapKeys.deserialize(new DataInputDeserializer(bucket.getBinary(1)));
        int size = ByteBuffer.wrap(spool.get(COUNTS, bucket, 0)).getInt();
        try (var values = new BucketList(bucket, size)) {
          byte[] metadata = spool.get(METADATA, bucket, 0);
          receiver.accept(
              owner,
              mapKey,
              serialized ? values : new ArrayList<>(values),
              metadata == null ? 0 : ByteBuffer.wrap(metadata).getLong());
        }
      }
    }
  }

  private final class BucketList extends AbstractList<V> implements AutoCloseable {
    private final RowData bucket;
    private final int size;
    private final List<CloseableIterator<V>> readers = new ArrayList<>();
    private boolean closed;

    BucketList(RowData bucket, int size) {
      this.bucket = bucket;
      this.size = size;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public V get(int index) {
      if (closed) throw new IllegalStateException("Canonical bucket is closed");
      if (index < 0 || index >= size) throw new IndexOutOfBoundsException(index);
      try {
        byte[] value = spool.get(ROWS, bucket, index);
        if (value == null) throw new IllegalStateException("Missing canonical bucket row");
        return rows.deserialize(new DataInputDeserializer(value));
      } catch (Exception failure) {
        throw new IllegalStateException("Cannot read canonical bucket", failure);
      }
    }

    @Override
    public Iterator<V> iterator() {
      if (closed) throw new IllegalStateException("Canonical bucket is closed");
      try {
        var reader = spool.values(ROWS, bucket, rows);
        readers.add(reader);
        return new Iterator<>() {
          private int position;

          @Override
          public boolean hasNext() {
            boolean next = reader.hasNext();
            if (next != (position < size))
              throw new IllegalStateException("Canonical bucket row count changed");
            return next;
          }

          @Override
          public V next() {
            if (!hasNext()) throw new java.util.NoSuchElementException();
            position++;
            return reader.next();
          }
        };
      } catch (Exception failure) {
        throw new IllegalStateException("Cannot open canonical bucket", failure);
      }
    }

    @Override
    public void close() throws Exception {
      if (closed) return;
      closed = true;
      Exception failure = null;
      for (var reader : readers) {
        try {
          reader.close();
        } catch (Exception error) {
          if (failure == null) failure = error;
          else failure.addSuppressed(error);
        }
      }
      if (failure != null) throw failure;
    }
  }

  @Override
  public void close() throws Exception {
    spool.close();
  }
}

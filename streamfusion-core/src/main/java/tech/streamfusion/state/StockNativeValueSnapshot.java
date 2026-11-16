package tech.streamfusion.state;

import java.io.IOException;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.ttl.TtlStateFactory.TtlSerializer;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;

/**
 * Converts a pinned native read snapshot only when Flink's asynchronous writer requests entries.
 */
final class StockNativeValueSnapshot<V> implements FullSnapshotResources<RowData> {
  @FunctionalInterface
  interface Entry<V> {
    void write(RowData key, V value, long written) throws Exception;
  }

  @FunctionalInterface
  interface Decoder<V> {
    void decode(RowData row, Entry<V> entry) throws Exception;
  }

  private final TypeSerializer<RowData> keys;
  private final KeyGroupRange groups;
  private final int maximum;
  private final TypeSerializer<Object> values;
  private final boolean ttl;
  private final RowType transfer;
  private final Decoder<V> decoder;
  private final List<StateMetaInfoSnapshot> metadata;
  private long snapshot;
  private Entries iterator;
  private AutoCloseable lease;

  @SuppressWarnings("unchecked")
  StockNativeValueSnapshot(
      TypeSerializer<RowData> keys,
      int maximum,
      KeyGroupRange groups,
      String state,
      TypeSerializer<V> values,
      boolean ttl,
      long snapshot,
      RowType transfer,
      Decoder<V> decoder,
      AutoCloseable lease) {
    this.lease = lease;
    this.keys = keys.duplicate();
    this.maximum = maximum;
    this.groups = groups;
    this.values =
        (TypeSerializer<Object>)
            (ttl
                ? new TtlSerializer<>(LongSerializer.INSTANCE, values.duplicate())
                : values.duplicate());
    this.ttl = ttl;
    this.snapshot = snapshot;
    this.transfer = transfer;
    this.decoder = decoder;
    metadata =
        List.of(
            new RegisteredKeyValueStateBackendMetaInfo<>(
                    StateDescriptor.Type.VALUE,
                    state,
                    VoidNamespaceSerializer.INSTANCE,
                    this.values)
                .snapshot());
  }

  @Override
  public List<StateMetaInfoSnapshot> getMetaInfoSnapshots() {
    return metadata;
  }

  @Override
  public KeyGroupRange getKeyGroupRange() {
    return groups;
  }

  @Override
  public TypeSerializer<RowData> getKeySerializer() {
    return keys;
  }

  @Override
  public StreamCompressionDecorator getStreamCompressionDecorator() {
    return UncompressedStreamCompressionDecorator.INSTANCE;
  }

  @Override
  public synchronized KeyValueStateIterator createKVStateIterator() throws IOException {
    if (snapshot == 0 || iterator != null) throw new IOException("Native snapshot is unavailable");
    iterator = new Entries();
    try {
      iterator.next();
      return iterator;
    } catch (IOException | RuntimeException | Error failure) {
      try {
        iterator.close();
      } catch (Throwable cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  @Override
  public synchronized void release() {
    long owned = snapshot;
    snapshot = 0;
    AutoCloseable retained = lease;
    lease = null;
    Throwable failure = null;
    try {
      if (iterator != null) iterator.close();
    } catch (Throwable error) {
      failure = error;
    }
    try {
      if (owned != 0) Native.closeCanonicalSnapshot(owned);
    } catch (Throwable error) {
      if (failure == null) failure = error;
      else failure.addSuppressed(error);
    }
    try {
      if (retained != null) retained.close();
    } catch (Throwable error) {
      if (failure == null) failure = error;
      else failure.addSuppressed(error);
    }
    if (failure != null) org.apache.flink.util.ExceptionUtils.rethrow(failure);
  }

  private final class Entries implements KeyValueStateIterator {
    private final SerializedCompositeKeyBuilder<RowData> composite =
        new SerializedCompositeKeyBuilder<>(
            keys.duplicate(),
            CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(maximum),
            64);
    private final DataOutputSerializer encoded = new DataOutputSerializer(64);
    private VectorSchemaRoot root;
    private tech.streamfusion.arrow.ArrowReader reader;
    private int position;
    private int group = -1;
    private boolean newGroup;
    private byte[] key, value;
    private boolean closed;

    @Override
    public void next() throws IOException {
      synchronized (StockNativeValueSnapshot.this) {
        if (snapshot == 0 || closed) throw new IOException("Native snapshot was released");
        key = value = null;
        if (root == null || position == root.getRowCount()) {
          closeFrame();
          boolean more =
              Native.nextCanonicalSnapshot(
                  snapshot,
                  (arrayAddress, schemaAddress) -> {
                    try (var array = ArrowArray.wrap(arrayAddress);
                        var schema = ArrowSchema.wrap(schemaAddress)) {
                      root =
                          Data.importVectorSchemaRoot(
                              NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES);
                    }
                  });
          if (!more) return;
          if (root == null || root.getRowCount() == 0)
            throw new IOException("Missing native snapshot frame");
          reader = ArrowConversion.createArrowReader(root, transfer);
          position = 0;
        }
        try {
          decoder.decode(
              reader.read(position++),
              (owner, retained, written) -> {
                if (key != null || owner == null || retained == null)
                  throw new IOException("Native snapshot must supply one present value per row");
                int nextGroup = KeyGroupRangeAssignment.assignToKeyGroup(owner, maximum);
                if (!groups.contains(nextGroup) || nextGroup < group)
                  throw new IOException("Native snapshot has invalid key-group order or ownership");
                newGroup = nextGroup != group;
                group = nextGroup;
                composite.setKeyAndKeyGroup(owner, group);
                key =
                    composite.buildCompositeKeyNamespace(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE);
                encoded.clear();
                values.serialize(ttl ? new TtlValue<>(retained, written) : retained, encoded);
                value = encoded.getCopyOfBuffer();
              });
          if (key == null) throw new IOException("Native snapshot row has no stock value");
        } catch (Exception failure) {
          throw new IOException("Cannot convert native snapshot entry", failure);
        }
      }
    }

    private void closeFrame() {
      var owned = root;
      root = null;
      reader = null;
      if (owned != null) owned.close();
    }

    @Override
    public boolean isValid() {
      return key != null;
    }

    @Override
    public int keyGroup() {
      return group;
    }

    @Override
    public byte[] key() {
      return key;
    }

    @Override
    public byte[] value() {
      return value;
    }

    @Override
    public int kvStateId() {
      return 0;
    }

    @Override
    public boolean isNewKeyValueState() {
      return newGroup;
    }

    @Override
    public boolean isNewKeyGroup() {
      return newGroup;
    }

    @Override
    public void close() {
      synchronized (StockNativeValueSnapshot.this) {
        closed = true;
        key = value = null;
        closeFrame();
      }
    }
  }
}

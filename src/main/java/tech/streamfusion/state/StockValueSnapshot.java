package tech.streamfusion.state;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.FullSnapshotResources;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyValueStateIterator;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.SnapshotExecutionType;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.ttl.TtlStateFactory.TtlSerializer;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.table.data.RowData;

/** Bounded staging of stock key/value bytes for Flink's asynchronous canonical writer. */
public final class StockValueSnapshot implements FullSnapshotResources<RowData> {
  @FunctionalInterface
  public interface Entry<V> {
    void write(RowData key, V value, long written) throws Exception;
  }

  @FunctionalInterface
  public interface Export<V> {
    void visit(Entry<V> receiver) throws Exception;
  }

  private final TypeSerializer<RowData> keys;
  private final KeyGroupRange groups;
  private final List<StateMetaInfoSnapshot> metadata;
  private final Path file;
  private final long[] heads;
  private final AtomicBoolean released = new AtomicBoolean();

  private StockValueSnapshot(
      TypeSerializer<RowData> keys,
      KeyGroupRange groups,
      StateMetaInfoSnapshot metadata,
      Path file,
      long[] heads) {
    this.keys = keys.duplicate();
    this.groups = groups;
    this.metadata = List.of(metadata);
    this.file = file;
    this.heads = heads;
  }

  @SuppressWarnings("unchecked")
  public static <V> SavepointResources<RowData> capture(
      TypeSerializer<RowData> keys,
      int maximum,
      KeyGroupRange groups,
      Path directory,
      String state,
      TypeSerializer<V> values,
      boolean ttl,
      Export<V> exporter)
      throws Exception {
    TypeSerializer<Object> serializedValues =
        (TypeSerializer<Object>)
            (ttl
                ? new TtlSerializer<>(LongSerializer.INSTANCE, values.duplicate())
                : values.duplicate());
    var metadata =
        new RegisteredKeyValueStateBackendMetaInfo<>(
                StateDescriptor.Type.VALUE,
                state,
                VoidNamespaceSerializer.INSTANCE,
                serializedValues)
            .snapshot();
    long[] heads = new long[groups.getNumberOfKeyGroups()];
    long[] tails = new long[heads.length];
    Arrays.fill(heads, -1);
    Arrays.fill(tails, -1);
    Path file = Files.createTempFile(directory, "streamfusion-canonical-", ".entries");
    var resources = new StockValueSnapshot(keys, groups, metadata, file, heads);
    try {
      try (var output = new RandomAccessFile(file.toFile(), "rw")) {
        var composite =
            new SerializedCompositeKeyBuilder<>(
                keys.duplicate(),
                CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(maximum),
                64);
        var encodedValue = new DataOutputSerializer(64);
        exporter.visit(
            (key, value, written) -> {
              if (key == null || value == null)
                throw new IllegalArgumentException("Absent stock values must be omitted");
              int group = KeyGroupRangeAssignment.assignToKeyGroup(key, maximum);
              if (!groups.contains(group))
                throw new IllegalStateException("Canonical state is outside its owned key groups");
              composite.setKeyAndKeyGroup(key, group);
              byte[] encodedKey =
                  composite.buildCompositeKeyNamespace(
                      VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE);
              encodedValue.clear();
              serializedValues.serialize(
                  ttl ? new TtlValue<>(value, written) : value, encodedValue);
              long position = output.length();
              int index = group - groups.getStartKeyGroup();
              if (tails[index] >= 0) {
                output.seek(tails[index]);
                output.writeLong(position);
              } else heads[index] = position;
              tails[index] = position;
              output.seek(position);
              output.writeLong(-1);
              output.writeInt(encodedKey.length);
              output.writeInt(encodedValue.length());
              output.write(encodedKey);
              output.write(encodedValue.getSharedBuffer(), 0, encodedValue.length());
            });
      }
      return new SavepointResources<>(resources, SnapshotExecutionType.ASYNCHRONOUS);
    } catch (Exception | Error failure) {
      try {
        resources.release();
      } catch (Exception | Error cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
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
  public KeyValueStateIterator createKVStateIterator() throws IOException {
    if (released.get()) throw new IOException("Canonical snapshot was released");
    return new Entries();
  }

  @Override
  public void release() {
    if (!released.compareAndSet(false, true)) return;
    try {
      Files.deleteIfExists(file);
    } catch (IOException failure) {
      throw new UncheckedIOException("Cannot remove canonical snapshot staging", failure);
    }
  }

  private final class Entries implements KeyValueStateIterator {
    private final RandomAccessFile input;
    private int group = -1;
    private long next = -1;
    private byte[] key, value;
    private boolean newGroup;

    Entries() throws IOException {
      input = new RandomAccessFile(file.toFile(), "r");
      try {
        next();
      } catch (IOException | RuntimeException | Error failure) {
        try {
          input.close();
        } catch (IOException cleanup) {
          failure.addSuppressed(cleanup);
        }
        throw failure;
      }
    }

    @Override
    public void next() throws IOException {
      newGroup = next < 0;
      while (next < 0 && ++group < heads.length) next = heads[group];
      if (next < 0) {
        key = value = null;
        return;
      }
      input.seek(next);
      next = input.readLong();
      int keyLength = input.readInt();
      int valueLength = input.readInt();
      if (keyLength < 0
          || valueLength < 0
          || (long) keyLength + valueLength > input.length() - input.getFilePointer())
        throw new IOException("Invalid canonical staging record");
      key = new byte[keyLength];
      value = new byte[valueLength];
      input.readFully(key);
      input.readFully(value);
    }

    @Override
    public boolean isValid() {
      return key != null;
    }

    @Override
    public int keyGroup() {
      return groups.getStartKeyGroup() + group;
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
      key = value = null;
      try {
        input.close();
      } catch (IOException failure) {
        throw new UncheckedIOException(failure);
      }
    }
  }
}

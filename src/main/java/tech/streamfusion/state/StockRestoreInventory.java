package tech.streamfusion.state;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.NoSuchElementException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.FileUtils;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** Scoped disk inventory for validating restored owners and timer relationships before import. */
final class StockRestoreInventory implements AutoCloseable {
  private static final byte[] PRESENT = new byte[0];
  private static final int WRITE_BUFFER_BYTES = 1024 * 1024, WRITE_BUFFERS = 2;
  private final TypeSerializer<RowData> keys;
  private final File directory;
  private final Options options;
  private final WriteOptions writes;
  private final RocksDB db;
  private final TaskOffHeapMemory.Reservation memory;
  private boolean closed;

  StockRestoreInventory(TypeSerializer<RowData> keys) throws Exception {
    this(keys, Path.of(System.getProperty("java.io.tmpdir")));
  }

  StockRestoreInventory(TypeSerializer<RowData> keys, Path temporaryDirectory) throws Exception {
    this.keys = keys.duplicate();
    RocksDB.loadLibrary();
    memory =
        TaskOffHeapMemory.reserve(
            "state",
            "canonical restore inventory write buffers",
            WRITE_BUFFER_BYTES * WRITE_BUFFERS);
    File scratch = null;
    Options allocatedOptions = null;
    WriteOptions allocatedWrites = null;
    try {
      scratch =
          Files.createTempDirectory(temporaryDirectory, "streamfusion-restore-inventory-").toFile();
      allocatedOptions = new Options();
      allocatedWrites = new WriteOptions();
      allocatedOptions
          .setCreateIfMissing(true)
          .setWriteBufferSize(WRITE_BUFFER_BYTES)
          .setMaxWriteBufferNumber(WRITE_BUFFERS)
          .setMaxBackgroundJobs(1)
          .setMaxOpenFiles(32)
          .setTableFormatConfig(new BlockBasedTableConfig().setNoBlockCache(true));
      allocatedWrites.setDisableWAL(true);
      db = RocksDB.open(allocatedOptions, scratch.getAbsolutePath());
      directory = scratch;
      options = allocatedOptions;
      writes = allocatedWrites;
    } catch (Exception | Error failure) {
      if (allocatedWrites != null) allocatedWrites.close();
      if (allocatedOptions != null) allocatedOptions.close();
      try {
        if (scratch != null) FileUtils.deleteDirectory(scratch);
      } catch (Exception cleanup) {
        failure.addSuppressed(cleanup);
      } finally {
        memory.close();
      }
      throw failure;
    }
  }

  private byte[] owner(RowData key) throws Exception {
    var out = new DataOutputSerializer(64);
    keys.serialize(key, out);
    return out.getCopyOfBuffer();
  }

  void addOwner(RowData key) throws Exception {
    byte[] owner = owner(key);
    var encoded = new byte[1 + owner.length];
    System.arraycopy(owner, 0, encoded, 1, owner.length);
    db.put(writes, encoded, PRESENT);
  }

  void collectOwners(CheckpointableKeyedStateBackend<RowData> backend, Iterable<String> states)
      throws Exception {
    for (String state : states)
      try (var stream = backend.getKeys(state, VoidNamespace.INSTANCE)) {
        var iterator = stream.iterator();
        while (iterator.hasNext()) addOwner(iterator.next());
      }
  }

  private byte[] prefix(int section, RowData key) throws Exception {
    if (section <= 0 || section > 255)
      throw new IllegalArgumentException("Invalid inventory section");
    byte[] owner = owner(key);
    return ByteBuffer.allocate(5 + owner.length)
        .put((byte) section)
        .putInt(owner.length)
        .put(owner)
        .array();
  }

  private byte[] entry(int section, RowData key, long value) throws Exception {
    var prefix = prefix(section, key);
    return ByteBuffer.allocate(prefix.length + Long.BYTES)
        .put(prefix)
        .putLong(value ^ Long.MIN_VALUE)
        .array();
  }

  boolean add(int section, RowData key, long value) throws Exception {
    var encoded = entry(section, key, value);
    if (db.get(encoded) != null) return false;
    db.put(writes, encoded, PRESENT);
    return true;
  }

  boolean contains(int section, RowData key, long value) throws Exception {
    return db.get(entry(section, key, value)) != null;
  }

  boolean hasAny(int section, RowData key) throws Exception {
    try (var values = times(section, key)) {
      return values.hasNext();
    }
  }

  CloseableIterator<RowData> owners() {
    return scan(
        new byte[] {0}, key -> keys.deserialize(new DataInputDeserializer(key, 1, key.length - 1)));
  }

  CloseableIterator<Long> times(int section, RowData key) throws Exception {
    return scan(
        prefix(section, key),
        encoded ->
            ByteBuffer.wrap(encoded, encoded.length - Long.BYTES, Long.BYTES).getLong()
                ^ Long.MIN_VALUE);
  }

  @FunctionalInterface
  private interface Decoder<T> {
    T decode(byte[] key) throws Exception;
  }

  private <T> CloseableIterator<T> scan(byte[] prefix, Decoder<T> decoder) {
    RocksIterator iterator = db.newIterator();
    iterator.seek(prefix);
    return new CloseableIterator<>() {
      @Override
      public boolean hasNext() {
        if (!iterator.isValid()) {
          try {
            iterator.status();
          } catch (Exception failure) {
            throw new IllegalStateException("Cannot read restore inventory", failure);
          }
          return false;
        }
        byte[] key = iterator.key();
        return key.length >= prefix.length
            && Arrays.equals(key, 0, prefix.length, prefix, 0, prefix.length);
      }

      @Override
      public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        try {
          T result = decoder.decode(iterator.key());
          iterator.next();
          return result;
        } catch (Exception failure) {
          throw new IllegalStateException("Cannot decode restore inventory", failure);
        }
      }

      @Override
      public void close() {
        iterator.close();
      }
    };
  }

  @Override
  public void close() throws Exception {
    if (closed) return;
    closed = true;
    try {
      db.close();
    } finally {
      writes.close();
      options.close();
      try {
        FileUtils.deleteDirectory(directory);
      } finally {
        memory.close();
      }
    }
  }
}

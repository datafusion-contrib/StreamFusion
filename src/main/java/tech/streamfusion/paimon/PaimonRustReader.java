package tech.streamfusion.paimon;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.SerializationUtils;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.ArrowFileBatch;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Released paimon-rust reads Java-selected files through Java's authenticated FileIO. */
final class PaimonRustReader implements AutoCloseable {
  private final Input io;
  private long handle;

  static boolean supports(FileStoreTable table, DataFileMeta file) {
    return PaimonCodecs.available(file.fileFormat())
        && file.externalPath().map(path -> path.endsWith("." + file.fileFormat())).orElse(true)
        && (!file.fileFormat().equals("orc")
            || (!hasChar(LogicalTypeConversion.toLogicalType(table.rowType()))
                && timestampsInRange(table, file)));
  }

  private static boolean timestampsInRange(FileStoreTable table, DataFileMeta file) {
    var fields = table.rowType().getFields();
    var stats = file.valueStats();
    var names = file.valueStatsCols();
    for (int field = 0; field < fields.size(); field++) {
      var type = LogicalTypeConversion.toLogicalType(fields.get(field).type());
      if (!hasTimestamp(type)) continue;
      if (!type.getChildren().isEmpty()) return false;
      int index = names == null ? field : names.indexOf(fields.get(field).name());
      if (index < 0 || index >= stats.minValues().getFieldCount()) return false;
      if (stats.minValues().isNullAt(index) || stats.maxValues().isNullAt(index)) {
        if (index < stats.nullCounts().size()
            && !stats.nullCounts().isNullAt(index)
            && stats.nullCounts().getLong(index) == file.rowCount()) continue;
        return false;
      }
      int precision = LogicalTypeChecks.getPrecision(type);
      // Released orc-rust decodes nanoseconds before casting to the requested precision.
      if (stats.minValues().getTimestamp(index, precision).getMillisecond() < -9_000_000_000_000L
          || stats.maxValues().getTimestamp(index, precision).getMillisecond() > 9_000_000_000_000L)
        return false;
    }
    return true;
  }

  private static boolean hasTimestamp(LogicalType type) {
    return type.getTypeRoot() == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
        || type.getTypeRoot() == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE
        || type.getChildren().stream().anyMatch(PaimonRustReader::hasTimestamp);
  }

  private static boolean hasChar(LogicalType type) {
    return type.getTypeRoot() == LogicalTypeRoot.CHAR
        || type.getChildren().stream().anyMatch(PaimonRustReader::hasChar);
  }

  PaimonRustReader(FileStoreTable table, DataSplit split, RowType output, int rows, boolean raw)
      throws IOException {
    this(table, split, output, output, rows, raw, null, false);
  }

  static PaimonRustReader compaction(
      FileStoreTable table,
      DataSplit split,
      PaimonKeyValueLayout layout,
      int rows,
      boolean dropDelete)
      throws IOException {
    return new PaimonRustReader(
        table,
        split,
        LogicalTypeConversion.toLogicalType(table.rowType()),
        layout.flinkWriteType,
        rows,
        false,
        layout.keyColumns,
        dropDelete);
  }

  private PaimonRustReader(
      FileStoreTable table,
      DataSplit split,
      RowType projection,
      RowType output,
      int rows,
      boolean raw,
      int[] physicalKeys,
      boolean dropDelete)
      throws IOException {
    io = new Input(table.fileIO());
    boolean kinds = !table.primaryKeys().isEmpty();
    List<Field> fields =
        new ArrayList<>(ArrowFileBatch.schema(ArrowConversion.toArrowSchema(output)).getFields());
    if (kinds && physicalKeys == null)
      fields.add(Field.nullable(RowDataArrowConverter.ROW_KIND_COLUMN, new ArrowType.Int(8, true)));
    try (ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportSchema(
          NativeAllocator.SHARED, new Schema(fields), NativeAllocator.DICTIONARIES, schema);
      handle =
          NativePaimon.createReader(
              io,
              descriptor(table, split, projection, raw, physicalKeys, dropDelete),
              schema.memoryAddress(),
              rows,
              table.coreOptions().sortSpillBufferSize(),
              kinds);
    } catch (Throwable failure) {
      try {
        io.close();
      } catch (Throwable closing) {
        failure.addSuppressed(closing);
      }
      throw failure;
    }
  }

  VectorSchemaRoot next() throws IOException {
    if (handle == 0) throw new IllegalStateException("Paimon reader is closed");
    if (Thread.currentThread().isInterrupted())
      throw new InterruptedIOException("Paimon read cancelled");
    return NativePaimonFileReader.importBatch(
        (array, schema) -> NativePaimon.readerNext(handle, array, schema));
  }

  long reservedBytes() {
    if (handle == 0) throw new IllegalStateException("Paimon reader is closed");
    return NativePaimon.readerReservedBytes(handle);
  }

  int filesOpened() {
    return io.opened.size();
  }

  private static String descriptor(
      FileStoreTable table,
      DataSplit split,
      RowType output,
      boolean raw,
      int[] physicalKeys,
      boolean dropDelete) {
    Map<String, Object> value = new LinkedHashMap<>();
    Map<String, Object> schema = JsonSerdeUtil.fromJson(table.schema().toString(), Map.class);
    schema.putIfAbsent("timeMillis", 0L);
    value.put("schema", schema);
    value.put("location", table.location().toString());
    value.put(
        "projection",
        output.getFieldNames().stream()
            .map(
                name ->
                    table.rowType().getFields().stream()
                        .filter(field -> field.name().equals(name))
                        .findFirst()
                        .orElseThrow())
            .collect(Collectors.toList()));
    value.put("partition", bytes(SerializationUtils.serializeBinaryRow(split.partition())));
    value.put("bucket", split.bucket());
    value.put("snapshot", split.snapshotId());
    value.put("bucketPath", split.bucketPath());
    value.put("streaming", split.isStreaming());
    value.put("raw", raw);
    if (physicalKeys != null)
      value.put("physical", Map.of("keys", physicalKeys, "dropDelete", dropDelete));
    value.put(
        "files",
        split.dataFiles().stream().map(file -> file(file, split)).collect(Collectors.toList()));
    return JsonSerdeUtil.toJson(value);
  }

  private static Map<String, Object> file(DataFileMeta file, DataSplit split) {
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("name", file.fileName());
    value.put("size", file.fileSize());
    value.put("rows", file.rowCount());
    value.put("schema", file.schemaId());
    value.put("level", file.level());
    value.put("minKey", bytes(SerializationUtils.serializeBinaryRow(file.minKey())));
    value.put("maxKey", bytes(SerializationUtils.serializeBinaryRow(file.maxKey())));
    value.put("minSequence", file.minSequenceNumber());
    value.put("maxSequence", file.maxSequenceNumber());
    value.put("deletes", file.deleteRowCount().orElse(null));
    value.put("path", file.externalPath().orElse(split.bucketPath() + "/" + file.fileName()));
    return value;
  }

  private static List<Integer> bytes(byte[] bytes) {
    List<Integer> result = new ArrayList<>(bytes.length);
    for (byte value : bytes) result.add(Byte.toUnsignedInt(value));
    return result;
  }

  @Override
  public void close() throws IOException {
    try (Input ignored = io) {
      if (handle != 0) {
        long closing = handle;
        handle = 0;
        NativePaimon.closeReader(closing);
      }
    }
  }

  static final class Input implements AutoCloseable {
    private final FileIO io;
    private final AtomicLong ids = new AtomicLong();
    private final Map<Long, OpenFile> files = new ConcurrentHashMap<>();
    private final Set<String> opened = ConcurrentHashMap.newKeySet();

    Input(FileIO io) {
      this.io = io;
    }

    public long length(String path) throws IOException {
      return io.getFileStatus(new Path(path)).getLen();
    }

    public long openFile(String path) throws IOException {
      if (Thread.currentThread().isInterrupted())
        throw new InterruptedIOException("Paimon read cancelled");
      long id = ids.incrementAndGet();
      files.put(id, new OpenFile(io.newInputStream(new Path(path))));
      opened.add(path);
      return id;
    }

    public void readFully(long id, long position, ByteBuffer target) throws IOException {
      if (Thread.currentThread().isInterrupted())
        throw new InterruptedIOException("Paimon read cancelled");
      OpenFile file = files.get(id);
      if (file == null) throw new IOException("Paimon input is closed");
      file.readFully(position, target);
    }

    public void closeFile(long id) throws IOException {
      OpenFile file = files.remove(id);
      if (file != null) file.stream.close();
    }

    @Override
    public void close() throws IOException {
      IOException failure = null;
      for (long id : List.copyOf(files.keySet())) {
        try {
          closeFile(id);
        } catch (IOException error) {
          if (failure == null) failure = error;
          else failure.addSuppressed(error);
        }
      }
      if (failure != null) throw failure;
    }
  }

  private static final class OpenFile {
    final SeekableInputStream stream;
    final byte[] chunk = new byte[64 * 1024];

    OpenFile(SeekableInputStream stream) {
      this.stream = stream;
    }

    synchronized void readFully(long position, ByteBuffer target) throws IOException {
      stream.seek(position);
      while (target.hasRemaining()) {
        int count = stream.read(chunk, 0, Math.min(chunk.length, target.remaining()));
        if (count < 0) throw new EOFException("Truncated Paimon input");
        if (count == 0) throw new IOException("Paimon input made no progress");
        target.put(chunk, 0, count);
      }
    }
  }
}

package tech.streamfusion.paimon;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.Pair;
import tech.streamfusion.Native;
import tech.streamfusion.operator.KeyedUpsertBuffer;
import tech.streamfusion.operator.NativeAllocator;

/** Arrow key-value files with Java metadata and Paimon's rolling and abort lifecycle. */
public final class NativePaimonKeyValueFileWriter {
  private static final int ROWS_PER_CHUNK = 4096;

  private final FileStoreTable table;
  private final PaimonKeyValueLayout layout;

  public NativePaimonKeyValueFileWriter(FileStoreTable table, PaimonKeyValueLayout layout) {
    this.table = table;
    this.layout = layout;
  }

  public DataIncrement write(BinaryRow partition, int bucket, KeyedUpsertBuffer.Flushed flushed)
      throws IOException {
    return write(partition, bucket, flushed, () -> Timestamp.now().toMillisTimestamp());
  }

  DataIncrement write(
      BinaryRow partition,
      int bucket,
      KeyedUpsertBuffer.Flushed flushed,
      Supplier<Timestamp> creationTime)
      throws IOException {
    try (flushed;
        Rolling data = rolling(partition, bucket, 0, FileSource.APPEND, false, creationTime);
        Rolling changelog =
            flushed.changelog == null
                ? null
                : rolling(partition, bucket, 0, FileSource.APPEND, true, creationTime)) {
      try {
        data.write(flushed.root);
        if (changelog != null) changelog.write(flushed.changelog);
        data.close();
        if (changelog != null) changelog.close();
        return new DataIncrement(
            data.result(),
            Collections.emptyList(),
            changelog == null ? Collections.emptyList() : changelog.result());
      } catch (Throwable failure) {
        data.abort();
        if (changelog != null) changelog.abort();
        throw failure;
      }
    }
  }

  Rolling compaction(BinaryRow partition, int bucket, int level) {
    return rolling(
        partition,
        bucket,
        level,
        FileSource.COMPACT,
        false,
        () -> Timestamp.now().toMillisTimestamp());
  }

  private Rolling rolling(
      BinaryRow partition,
      int bucket,
      int level,
      FileSource source,
      boolean changelog,
      Supplier<Timestamp> creationTime) {
    CoreOptions options = table.coreOptions();
    PaimonKeyValueLayout fileLayout =
        changelog
            ? PaimonKeyValueLayout.changelog(table)
            : level == 0 ? layout : PaimonKeyValueLayout.of(table, level);
    String compression =
        options.fileCompressionPerLevel().getOrDefault(level, options.fileCompression());
    if (changelog && PaimonVersion.changelogCompression(options) != null)
      compression = PaimonVersion.changelogCompression(options);
    String format = options.fileFormatPerLevel().getOrDefault(level, options.fileFormatString());
    if (changelog && PaimonVersion.changelogFormat(options) != null)
      format = PaimonVersion.changelogFormat(options);
    FormatWriterFactory factory =
        FileFormat.fromIdentifier(format, options.toConfiguration())
            .createWriterFactory(fileLayout.fileType);
    FileStoreTable pathTable =
        format.equals(options.fileFormatString()) && compression.equals(options.fileCompression())
            ? table
            : table.copy(java.util.Map.of("file.format", format, "file.compression", compression));
    DataFilePathFactory paths =
        pathTable.store().pathFactory().createDataFilePathFactory(partition, bucket);
    long targetRows =
        source == FileSource.COMPACT || changelog
            ? Long.MAX_VALUE
            : PaimonVersion.targetFileRows(options);
    return new Rolling(
        factory,
        fileLayout,
        paths,
        compression,
        level,
        source,
        changelog,
        creationTime,
        options.targetFileSize(true),
        targetRows);
  }

  final class Rolling implements AutoCloseable {
    private final RollingFileWriter<InternalRow, DataFileMeta> writer;
    private final PaimonKeyValueLayout fileLayout;
    private final long targetRows;
    private long fileRows;

    Rolling(
        FormatWriterFactory factory,
        PaimonKeyValueLayout fileLayout,
        DataFilePathFactory paths,
        String compression,
        int level,
        FileSource source,
        boolean changelog,
        Supplier<Timestamp> creationTime,
        long targetSize,
        long targetRows) {
      this.fileLayout = fileLayout;
      this.targetRows = targetRows;
      writer =
          PaimonVersion.rollingWriter(
              () ->
                  new Output(
                      factory,
                      fileLayout,
                      paths,
                      compression,
                      level,
                      source,
                      changelog,
                      creationTime),
              targetSize,
              targetRows,
              () -> fileRows = 0);
    }

    void write(VectorSchemaRoot root) throws IOException {
      for (int offset = 0; offset < root.getRowCount(); ) {
        int count =
            (int)
                Math.min(
                    Math.min(ROWS_PER_CHUNK, root.getRowCount() - offset), targetRows - fileRows);
        fileRows += count;
        writer.writeBundle(new ArrowBatchBundle(root, fileLayout.flinkWriteType, offset, count));
        offset += count;
      }
    }

    List<DataFileMeta> result() throws IOException {
      return writer.result();
    }

    void abort() {
      writer.abort();
    }

    @Override
    public void close() throws IOException {
      writer.close();
    }
  }

  private final class Output extends PaimonSingleFileWriter<InternalRow, DataFileMeta> {
    private final PaimonKeyValueLayout fileLayout;
    private final DataFilePathFactory paths;
    private final int level;
    private final FileSource source;
    private final Supplier<Timestamp> creationTime;
    private BinaryRow minKey;
    private BinaryRow maxKey;
    private long minSequence = Long.MAX_VALUE;
    private long maxSequence = Long.MIN_VALUE;
    private long deleteRows;

    Output(
        FormatWriterFactory factory,
        PaimonKeyValueLayout fileLayout,
        DataFilePathFactory paths,
        String compression,
        int level,
        FileSource source,
        boolean changelog,
        Supplier<Timestamp> creationTime) {
      super(
          table.fileIO(),
          factory,
          changelog ? paths.newChangelogPath() : paths.newPath(),
          row -> row,
          compression,
          table.coreOptions().asyncFileWrite());
      this.fileLayout = fileLayout;
      this.paths = paths;
      this.level = level;
      this.source = source;
      this.creationTime = creationTime;
    }

    @Override
    public void write(InternalRow row) {
      throw new UnsupportedOperationException("The native key-value writer requires Arrow bundles");
    }

    @Override
    public void writeBundle(BundleRecords records) throws IOException {
      ArrowBatchBundle bundle = (ArrowBatchBundle) records;
      VectorSchemaRoot root = bundle.root();
      int start = bundle.rowOffset();
      int count = Math.toIntExact(bundle.rowCount());
      // These views borrow vectors; the caller retains the root until this synchronous write
      // returns.
      VectorSchemaRoot fileRoot =
          fileLayout.thinMode
              ? new VectorSchemaRoot(
                  root.getFieldVectors()
                      .subList(fileLayout.keyFieldCount(), root.getFieldVectors().size()))
              : root;
      super.writeBundle(new ArrowBatchBundle(fileRoot, fileLayout.flinkFileType, start, count));
      BinaryRow[] bounds = keyBounds(root, new int[] {start, start + count - 1});
      if (minKey == null) minKey = bounds[0];
      maxKey = bounds[1];
      BigIntVector sequences = (BigIntVector) root.getVector(fileLayout.sequenceColumn());
      TinyIntVector kinds = (TinyIntVector) root.getVector(fileLayout.kindColumn());
      for (int row = start; row < start + count; row++) {
        minSequence = Math.min(minSequence, sequences.get(row));
        maxSequence = Math.max(maxSequence, sequences.get(row));
        if (RowKind.fromByteValue(kinds.get(row)).isRetract()) deleteRows++;
      }
    }

    @Override
    public DataFileMeta result() throws IOException {
      if (!closed)
        throw new IllegalStateException("The data file must be closed before describing it");
      if (recordCount() == 0) return null;
      long size = bytes();
      SimpleColStats[] rowStats = fileLayout.statsProducer.extract(fileIO, path, size, metadata());
      SimpleColStats[] valueColumns =
          Arrays.copyOfRange(
              rowStats, fileLayout.thinMode ? 2 : fileLayout.valueColumnOffset(), rowStats.length);
      SimpleColStats[] keyColumns = new SimpleColStats[fileLayout.keyFieldCount()];
      for (int i = 0; i < keyColumns.length; i++)
        keyColumns[i] = fileLayout.thinMode ? valueColumns[fileLayout.keyColumns[i]] : rowStats[i];
      SimpleStats keyStats = fileLayout.keyStatsConverter.toBinaryAllMode(keyColumns);
      Pair<List<String>, SimpleStats> valueStats =
          fileLayout.valueStatsConverter.toBinary(valueColumns);
      return PaimonVersion.fileMeta(
          path.getName(),
          size,
          recordCount(),
          minKey,
          maxKey,
          keyStats,
          valueStats.getRight(),
          minSequence,
          maxSequence,
          table.schema().id(),
          level,
          creationTime.get(),
          deleteRows,
          source,
          valueStats.getLeft(),
          PaimonVersion.externalPath(paths, path));
    }
  }

  /** Only small metadata keys cross into Java rows; payloads remain Arrow columns. */
  private BinaryRow[] keyBounds(VectorSchemaRoot root, int[] rows) {
    int[] keyColumns = new int[layout.keyFieldCount()];
    for (int i = 0; i < keyColumns.length; i++) keyColumns[i] = i;
    BufferAllocator allocator =
        root.getFieldVectors().isEmpty()
            ? NativeAllocator.SHARED
            : root.getFieldVectors().get(0).getAllocator();
    byte[][] encoded;
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, NativeAllocator.DICTIONARIES, array, schema);
      encoded =
          Native.flinkBinaryRows(
              array.memoryAddress(),
              schema.memoryAddress(),
              keyColumns,
              layout.keyTimestampPrecisions,
              rows);
    }
    BinaryRow[] bounds = new BinaryRow[encoded.length];
    for (int i = 0; i < encoded.length; i++)
      bounds[i] = PaimonPartitions.fromBytes(encoded[i], keyColumns.length);
    return bounds;
  }
}

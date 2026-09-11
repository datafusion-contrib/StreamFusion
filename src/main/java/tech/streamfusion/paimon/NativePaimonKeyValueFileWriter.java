package tech.streamfusion.paimon;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.Pair;
import tech.streamfusion.Native;
import tech.streamfusion.operator.KeyedUpsertBuffer;
import tech.streamfusion.operator.NativeAllocator;

/**
 * Writes a key-value batch as level-0 data files of a primary-key table. Normal buckets are sorted
 * with one row per key; postpone buckets retain every row in arrival order. Each batch is cut into
 * files at the table's target file size and each file is described the way Paimon's own key-value
 * file writer describes one: key bounds from its first and last row, footer statistics through
 * Paimon's extractor, sequence range and delete count from its rows. Paimon derives that metadata
 * while walking rows one by one; a batch written whole computes it column-wise here. An
 * input-changelog batch uses the same layout and sort but retains every input row; its files roll
 * independently and enter the changelog increment with their own compression and statistics
 * settings.
 */
public final class NativePaimonKeyValueFileWriter {

  private static final int ROWS_PER_CHUNK = 4096;

  /** One data file cut from a flushed batch: the rows {@code start} to {@code end} of the batch. */
  private static final class WrittenFile {
    final Path path;
    final long fileSize;
    final int start;
    final int end;

    WrittenFile(Path path, long fileSize, int start, int end) {
      this.path = path;
      this.fileSize = fileSize;
      this.start = start;
      this.end = end;
    }
  }

  private final FileStoreTable table;
  private final FileIO fileIO;
  private final long schemaId;
  private final long targetFileSize;
  private final String compression;
  private final String changelogCompression;
  private final PaimonKeyValueLayout layout;
  private final PaimonKeyValueLayout changelogLayout;
  private final FormatWriterFactory writerFactory;

  public NativePaimonKeyValueFileWriter(FileStoreTable table, PaimonKeyValueLayout layout) {
    CoreOptions options = table.coreOptions();
    this.table = table;
    this.fileIO = table.fileIO();
    this.schemaId = table.schema().id();
    this.targetFileSize = options.targetFileSize(true);
    this.compression = options.fileCompressionPerLevel().getOrDefault(0, options.fileCompression());
    this.changelogCompression =
        options.changelogFileCompression() == null
            ? compression
            : options.changelogFileCompression();
    this.layout = layout;
    this.changelogLayout = PaimonKeyValueLayout.changelog(table);
    this.writerFactory =
        FileFormat.fromIdentifier(options.fileFormatString(), options.toConfiguration())
            .createWriterFactory(layout.writeType);
  }

  /** Writes and closes both outputs of a bucket's flush, returning data and changelog metadata. */
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
    DataFilePathFactory paths =
        table.store().pathFactory().createDataFilePathFactory(partition, bucket);
    try (flushed) {
      List<DataFileMeta> data = write(flushed.root, paths, false, creationTime);
      List<DataFileMeta> changelog =
          flushed.changelog == null
              ? Collections.emptyList()
              : write(flushed.changelog, paths, true, creationTime);
      return new DataIncrement(data, Collections.emptyList(), changelog);
    }
  }

  private List<DataFileMeta> write(
      VectorSchemaRoot root,
      DataFilePathFactory paths,
      boolean changelog,
      Supplier<Timestamp> creationTime)
      throws IOException {
    List<DataFileMeta> files = new ArrayList<>();
    List<WrittenFile> written = new ArrayList<>();
    int start = 0;
    while (start < root.getRowCount()) {
      WrittenFile file = writeFile(root, start, paths, changelog);
      written.add(file);
      start = file.end;
    }
    BinaryRow[] keyBounds = keyBounds(root, written);
    for (int i = 0; i < written.size(); i++) {
      files.add(
          describe(
              root,
              written.get(i),
              keyBounds[2 * i],
              keyBounds[2 * i + 1],
              paths,
              changelog ? changelogLayout : layout,
              creationTime.get()));
    }
    return files;
  }

  /** Encodes rows from {@code start} into one file until it reaches the target size. */
  private WrittenFile writeFile(
      VectorSchemaRoot root, int start, DataFilePathFactory paths, boolean changelog)
      throws IOException {
    Path path = changelog ? paths.newChangelogPath() : paths.newPath();
    int rows = root.getRowCount();
    int end = start;
    long fileSize;
    try (PositionOutputStream out = fileIO.newOutputStream(path, false)) {
      FormatWriter writer =
          writerFactory.create(out, changelog ? changelogCompression : compression);
      while (end < rows) {
        int count = Math.min(ROWS_PER_CHUNK, rows - end);
        ((NativePaimonParquetWriter) writer)
            .writeBundle(new ArrowBatchBundle(root, layout.flinkWriteType, end, count));
        end += count;
        if (end < rows && writer.reachTargetSize(true, targetFileSize)) {
          break;
        }
      }
      writer.close();
      fileSize = out.getPos();
    }
    return new WrittenFile(path, fileSize, start, end);
  }

  private DataFileMeta describe(
      VectorSchemaRoot root,
      WrittenFile file,
      BinaryRow minKey,
      BinaryRow maxKey,
      DataFilePathFactory paths,
      PaimonKeyValueLayout layout,
      Timestamp creationTime)
      throws IOException {
    BigIntVector sequences = (BigIntVector) root.getVector(layout.sequenceColumn());
    TinyIntVector kinds = (TinyIntVector) root.getVector(layout.kindColumn());
    long minSequence = Long.MAX_VALUE;
    long maxSequence = Long.MIN_VALUE;
    long deleteRows = 0;
    for (int row = file.start; row < file.end; row++) {
      long sequence = sequences.get(row);
      minSequence = Math.min(minSequence, sequence);
      maxSequence = Math.max(maxSequence, sequence);
      if (RowKind.fromByteValue(kinds.get(row)).isRetract()) {
        deleteRows++;
      }
    }
    SimpleColStats[] rowStats = layout.statsProducer.extract(fileIO, file.path, file.fileSize);
    SimpleStats keyStats =
        layout.keyStatsConverter.toBinaryAllMode(
            Arrays.copyOfRange(rowStats, 0, layout.keyFieldCount()));
    Pair<List<String>, SimpleStats> valueStats =
        layout.valueStatsConverter.toBinary(
            Arrays.copyOfRange(rowStats, layout.valueColumnOffset(), rowStats.length));
    return DataFileMeta.create(
        file.path.getName(),
        file.fileSize,
        file.end - file.start,
        minKey,
        maxKey,
        keyStats,
        valueStats.getRight(),
        minSequence,
        maxSequence,
        schemaId,
        0,
        Collections.emptyList(),
        creationTime,
        deleteRows,
        null,
        FileSource.APPEND,
        valueStats.getLeft(),
        paths.isExternalPath() ? file.path.toString() : null,
        null,
        null);
  }

  /** The first and last key of every file as Paimon key rows, encoded in one native pass. */
  private BinaryRow[] keyBounds(VectorSchemaRoot root, List<WrittenFile> files) {
    int[] rows = new int[files.size() * 2];
    for (int i = 0; i < files.size(); i++) {
      rows[2 * i] = files.get(i).start;
      rows[2 * i + 1] = files.get(i).end - 1;
    }
    int[] keyColumns = new int[layout.keyFieldCount()];
    for (int i = 0; i < keyColumns.length; i++) {
      keyColumns[i] = i;
    }
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
    for (int i = 0; i < encoded.length; i++) {
      bounds[i] = PaimonPartitions.fromBytes(encoded[i], keyColumns.length);
    }
    return bounds;
  }
}

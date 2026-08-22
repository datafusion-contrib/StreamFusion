package tech.streamfusion.delta;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.hadoopio.HadoopInputFile;
import io.delta.kernel.defaults.internal.parquet.ParquetStatsReader;
import io.delta.kernel.engine.FileReadResult;
import io.delta.kernel.engine.ParquetHandler;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.*;
import java.io.IOException;
import java.util.*;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.parquet.NativeParquet;

/** Native implementation of Kernel's data-file write method; reads and metadata writes delegate. */
final class NativeDeltaParquetHandler implements ParquetHandler {
  private static final int DRAIN_CHUNK_BYTES = 1 << 20;
  private static final int SIZE_ROLLING_ROWS_PER_CHECK = 1024;

  private final ParquetHandler delegate;
  private final Configuration configuration;
  private final String[] encoderKeys;
  private final String[] encoderValues;
  private final String rollingStrategy;
  private final long rollingLimit;
  private final Set<String> initializedDirectories = new HashSet<>();

  NativeDeltaParquetHandler(
      ParquetHandler delegate,
      Configuration configuration,
      String[] encoderKeys,
      String[] encoderValues,
      String rollingStrategy,
      long rollingLimit) {
    this.delegate = delegate;
    this.configuration = configuration;
    this.encoderKeys = encoderKeys.clone();
    this.encoderValues = encoderValues.clone();
    this.rollingStrategy = rollingStrategy;
    this.rollingLimit = rollingLimit;
  }

  @Override
  public CloseableIterator<FileReadResult> readParquetFiles(
      CloseableIterator<FileStatus> files,
      StructType physicalSchema,
      Optional<io.delta.kernel.expressions.Predicate> predicate)
      throws IOException {
    return delegate.readParquetFiles(files, physicalSchema, predicate);
  }

  @Override
  public CloseableIterator<DataFileStatus> writeParquetFiles(
      String directory,
      CloseableIterator<FilteredColumnarBatch> data,
      List<Column> statisticsColumns)
      throws IOException {
    List<DataFileStatus> files = new ArrayList<>();
    NativeFile file = null;
    try (CloseableIterator<FilteredColumnarBatch> input = data) {
      while (input.hasNext()) {
        FilteredColumnarBatch filtered = input.next();
        ArrowKernelBatch batch = null;
        try {
          if (filtered.getSelectionVector().isPresent()
              || !(filtered.getData() instanceof ArrowKernelBatch)) {
            throw new UnsupportedOperationException(
                "The native Delta writer requires an Arrow-backed Kernel batch with its selection"
                    + " applied; got "
                    + filtered.getData().getClass().getName()
                    + ", filtered="
                    + filtered.getSelectionVector().isPresent());
          }
          batch = (ArrowKernelBatch) filtered.getData();
          int logicalOffset = 0;
          while (logicalOffset < batch.getSize()) {
            if (file == null) {
              file = openFile(directory, batch);
            }
            int rows = rowsForNextWrite(file, batch.getSize() - logicalOffset);
            file.write(batch, logicalOffset, rows);
            logicalOffset += rows;
            if (shouldRoll(file)) {
              files.add(file.finish(statisticsColumns));
              file = null;
            }
          }
        } finally {
          if (batch != null) {
            batch.close();
          }
        }
      }
      if (file != null) {
        files.add(file.finish(statisticsColumns));
        file = null;
      }
    } catch (RuntimeException | IOException failure) {
      if (file != null) {
        file.abort();
      }
      throw failure;
    }
    return io.delta.kernel.internal.util.Utils.toCloseableIterator(
        files.iterator());
  }

  private NativeFile openFile(String directory, ArrowKernelBatch batch) throws IOException {
    Path target = new Path(directory, UUID.randomUUID() + ".parquet");
    FileSystem fs = target.getFileSystem(configuration);
    ensureDirectory(fs, target.getParent());
    return new NativeFile(fs, target, batch);
  }

  private int rowsForNextWrite(NativeFile file, int available) {
    if (rollingLimit < 0) {
      return available;
    }
    if ("count".equals(rollingStrategy)) {
      long remaining = Math.max(1L, rollingLimit - file.rows());
      return (int) Math.min(available, remaining);
    }
    return Math.min(available, SIZE_ROLLING_ROWS_PER_CHECK);
  }

  private boolean shouldRoll(NativeFile file) {
    if (rollingLimit < 0) {
      return false;
    }
    if ("count".equals(rollingStrategy)) {
      return file.rows() >= Math.max(1L, rollingLimit);
    }
    return file.estimatedBytes() >= rollingLimit;
  }

  private final class NativeFile {
    private final FileSystem fs;
    private final Path target;
    private final org.apache.hadoop.fs.FSDataOutputStream output;
    private final StructType dataSchema;
    private long encoder;
    private long rows;
    private boolean closed;

    private NativeFile(FileSystem fs, Path target, ArrowKernelBatch batch) throws IOException {
      this.fs = fs;
      this.target = target;
      this.dataSchema = batch.getSchema();
      this.output = fs.create(target, false);
      byte[] chunk = new byte[DRAIN_CHUNK_BYTES];
      VectorSchemaRoot root = batch.borrowedRoot();
      try (org.apache.arrow.c.ArrowSchema encoderSchema =
          org.apache.arrow.c.ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
        Data.exportSchema(
            NativeAllocator.SHARED,
            root.getSchema(),
            NativeAllocator.DICTIONARIES,
            encoderSchema);
        encoder =
            NativeParquet.createParquetEncoder(
                encoderSchema.memoryAddress(),
                new int[0],
                encoderKeys,
                encoderValues,
                false,
                output,
                chunk);
      } catch (RuntimeException failure) {
        output.close();
        fs.delete(target, false);
        throw failure;
      }
    }

    private void write(ArrowKernelBatch batch, int logicalOffset, int rowCount) {
      int[] selected = batch.selectedRows();
      int[] selection =
          selected.length == 0
              ? selected
              : Arrays.copyOfRange(selected, logicalOffset, logicalOffset + rowCount);
      int physicalOffset = selected.length == 0 ? logicalOffset : 0;
      VectorSchemaRoot root = batch.borrowedRoot();
      try (ArrowArray array = ArrowArray.allocateNew(NativeAllocator.SHARED)) {
        Data.exportVectorSchemaRoot(
            NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array);
        NativeParquet.parquetEncoderWrite(
            encoder, array.memoryAddress(), selection, physicalOffset, rowCount);
        rows += rowCount;
      }
    }

    private long rows() {
      return rows;
    }

    private long estimatedBytes() {
      return NativeParquet.parquetEncoderEstimatedBytes(encoder);
    }

    private DataFileStatus finish(List<Column> statisticsColumns) throws IOException {
      long rowCount = rows();
      NativeParquet.parquetEncoderFinish(encoder);
      output.hflush();
      closeResources();
      org.apache.hadoop.fs.FileStatus status = fs.getFileStatus(target);
      DataFileStatistics statistics;
      if (statisticsColumns.isEmpty()) {
        statistics =
            new DataFileStatistics(
                rowCount,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Optional.empty());
      } else {
        statistics =
            ParquetStatsReader.readDataFileStatistics(
                new HadoopInputFile(fs, target, status.getLen()), dataSchema, statisticsColumns);
      }
      return new DataFileStatus(
          target.toString(),
          status.getLen(),
          status.getModificationTime(),
          Optional.of(statistics));
    }

    private void closeResources() throws IOException {
      if (!closed) {
        closed = true;
        try {
          NativeParquet.closeParquetEncoder(encoder);
          encoder = 0;
        } finally {
          output.close();
        }
      }
    }

    private void abort() {
      try {
        closeResources();
      } catch (IOException ignored) {
        // Preserve the write failure.
      }
      try {
        fs.delete(target, false);
      } catch (IOException ignored) {
        // Preserve the write failure.
      }
    }
  }

  private synchronized void ensureDirectory(FileSystem fs, Path directory) throws IOException {
    String key = directory.makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString();
    if (initializedDirectories.contains(key)) {
      return;
    }
    if (!fs.mkdirs(directory) && !fs.exists(directory)) {
      throw new IOException("Failed to create Delta data directory " + directory);
    }
    initializedDirectories.add(key);
  }

  @Override
  public void writeParquetFileAtomically(
      String filePath, CloseableIterator<FilteredColumnarBatch> data) throws IOException {
    if (delegate == null) {
      throw new UnsupportedOperationException(
          "The native data-file handler does not write Delta metadata files");
    }
    delegate.writeParquetFileAtomically(filePath, data);
  }
}

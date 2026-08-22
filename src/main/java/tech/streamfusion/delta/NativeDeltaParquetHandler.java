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

  private final ParquetHandler delegate;
  private final Configuration configuration;
  private final String[] encoderKeys;
  private final String[] encoderValues;
  private final Set<String> initializedDirectories = new HashSet<>();

  NativeDeltaParquetHandler(
      ParquetHandler delegate,
      Configuration configuration,
      String[] encoderKeys,
      String[] encoderValues) {
    this.delegate = delegate;
    this.configuration = configuration;
    this.encoderKeys = encoderKeys.clone();
    this.encoderValues = encoderValues.clone();
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
    if (!data.hasNext()) {
      data.close();
      return io.delta.kernel.internal.util.Utils.toCloseableIterator(
          Collections.<DataFileStatus>emptyIterator());
    }
    FilteredColumnarBatch first = data.next();
    CloseableIterator<FilteredColumnarBatch> replay = prepend(first, data);
    if (first.getData() instanceof ArrowKernelBatch
        && ((ArrowKernelBatch) first.getData()).hasSparseSelection()) {
      // Kernel's writer is substantially faster for MOR's sparse selections: the native writer
      // must export every full source vector before gathering the selected rows. Kernel does not
      // own StreamFusion's Arrow batches, so close each one immediately after it is consumed.
      try (CloseableIterator<FilteredColumnarBatch> owned = closeConsumedArrowBatches(replay)) {
        if (delegate == null) {
          throw new IllegalStateException("Sparse Delta writes require the stock Parquet handler");
        }
        try (CloseableIterator<DataFileStatus> files =
            delegate.writeParquetFiles(directory, owned, statisticsColumns)) {
          return io.delta.kernel.internal.util.Utils.toCloseableIterator(
              files.toInMemoryList().iterator());
        }
      }
    }
    return writeNativeParquetFiles(directory, replay, statisticsColumns);
  }

  private CloseableIterator<DataFileStatus> writeNativeParquetFiles(
      String directory,
      CloseableIterator<FilteredColumnarBatch> data,
      List<Column> statisticsColumns)
      throws IOException {
    Path target = new Path(directory, UUID.randomUUID() + ".parquet");
    FileSystem fs = target.getFileSystem(configuration);
    ensureDirectory(fs, target.getParent());
    long rows = 0;
    long encoder = 0;
    StructType dataSchema = null;
    byte[] chunk = new byte[DRAIN_CHUNK_BYTES];
    try (org.apache.hadoop.fs.FSDataOutputStream output = fs.create(target, false);
        CloseableIterator<FilteredColumnarBatch> input = data) {
      while (input.hasNext()) {
        FilteredColumnarBatch filtered = input.next();
        if (filtered.getSelectionVector().isPresent()
            || !(filtered.getData() instanceof ArrowKernelBatch)) {
          throw new UnsupportedOperationException(
              "The native Delta writer requires an unfiltered Arrow-backed Kernel batch; got "
                  + filtered.getData().getClass().getName()
                  + ", filtered="
                  + filtered.getSelectionVector().isPresent());
        }
        ArrowKernelBatch batch = (ArrowKernelBatch) filtered.getData();
        VectorSchemaRoot root = batch.borrowedRoot();
        try (ArrowArray array = ArrowArray.allocateNew(NativeAllocator.SHARED)) {
          dataSchema = batch.getSchema();
          if (encoder == 0) {
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
            }
          }
          Data.exportVectorSchemaRoot(
              NativeAllocator.SHARED,
              root,
              NativeAllocator.DICTIONARIES,
              array);
          NativeParquet.parquetEncoderWrite(
              encoder, array.memoryAddress(), batch.selectedRows());
          rows += batch.getSize();
        } finally {
          batch.close();
        }
      }
      if (encoder == 0) {
        fs.delete(target, false);
        return io.delta.kernel.internal.util.Utils.toCloseableIterator(
            Collections.<DataFileStatus>emptyIterator());
      }
      NativeParquet.parquetEncoderFinish(encoder);
      output.hflush();
    } catch (RuntimeException | IOException failure) {
      fs.delete(target, false);
      throw failure;
    } finally {
      if (encoder != 0) {
        NativeParquet.closeParquetEncoder(encoder);
      }
    }
    org.apache.hadoop.fs.FileStatus status = fs.getFileStatus(target);
    DataFileStatistics statistics;
    if (statisticsColumns.isEmpty()) {
      statistics =
          new DataFileStatistics(
              rows,
              Collections.emptyMap(),
              Collections.emptyMap(),
              Collections.emptyMap(),
              Optional.empty());
    } else {
      statistics =
          ParquetStatsReader.readDataFileStatistics(
              new HadoopInputFile(fs, target, status.getLen()), dataSchema, statisticsColumns);
    }
    return io.delta.kernel.internal.util.Utils.toCloseableIterator(
        Collections.singletonList(
                new DataFileStatus(
                    target.toString(),
                    status.getLen(),
                    status.getModificationTime(),
                    Optional.of(statistics)))
            .iterator());
  }

  private static CloseableIterator<FilteredColumnarBatch> prepend(
      FilteredColumnarBatch first, CloseableIterator<FilteredColumnarBatch> rest) {
    return new CloseableIterator<>() {
      private boolean firstPending = true;

      @Override
      public boolean hasNext() {
        return firstPending || rest.hasNext();
      }

      @Override
      public FilteredColumnarBatch next() {
        if (firstPending) {
          firstPending = false;
          return first;
        }
        return rest.next();
      }

      @Override
      public void close() throws IOException {
        rest.close();
      }
    };
  }

  private static CloseableIterator<FilteredColumnarBatch> closeConsumedArrowBatches(
      CloseableIterator<FilteredColumnarBatch> input) {
    return new CloseableIterator<>() {
      private ArrowKernelBatch current;
      private boolean closed;

      private void closeCurrent() {
        if (current != null) {
          current.close();
          current = null;
        }
      }

      @Override
      public boolean hasNext() {
        boolean hasNext = input.hasNext();
        if (!hasNext) {
          closeCurrent();
        }
        return hasNext;
      }

      @Override
      public FilteredColumnarBatch next() {
        closeCurrent();
        FilteredColumnarBatch next = input.next();
        if (next.getData() instanceof ArrowKernelBatch) {
          current = (ArrowKernelBatch) next.getData();
        }
        return next;
      }

      @Override
      public void close() throws IOException {
        if (!closed) {
          closed = true;
          closeCurrent();
          input.close();
        }
      }
    };
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

package tech.streamfusion.paimon;

import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.types.RowType;
import tech.streamfusion.format.ColumnarFileCodec;

/**
 * Encodes borrowed Arrow bundles, or admitted bounded Java rows, over Paimon's output stream. Java
 * rows are copied into bounded Arrow batches after Paimon has selected their file and order.
 * Unmarked row-fed files retain the stock writer. Cursor rows identify an entire borrowed bundle,
 * which is encoded once even when the released Paimon writer iterates it row by row.
 */
public final class NativePaimonFileWriter implements BundleFormatWriter {
  private enum Mode {
    UNDECIDED,
    NATIVE,
    STOCK
  }

  private final RowType writeType;
  private final ColumnarFileCodec codec;
  private final String[] keys, values;
  private final PositionOutputStream out;
  private final String compression;
  private final FormatWriterFactory stockFactory;
  private final PaimonCompactMemory compactMemory;
  @Nullable private PaimonParquetFloatingStats floatingStats;

  private boolean rowEncoding;
  @Nullable private PaimonRowBatch rows;

  private Mode mode = Mode.UNDECIDED;
  private ColumnarFileCodec.Encoder encoder;
  private boolean closed;
  @Nullable private FormatWriter stock;
  @Nullable private ArrowBatchBundle lastBundle;

  NativePaimonFileWriter(
      RowType writeType,
      ColumnarFileCodec codec,
      String[] keys,
      String[] values,
      PositionOutputStream out,
      String compression,
      FormatWriterFactory stockFactory) {
    this.writeType = writeType;
    this.codec = codec;
    this.keys = keys;
    this.values = values;
    this.out = out;
    this.compression = compression;
    this.stockFactory = stockFactory;
    this.compactMemory = PaimonCompactMemory.current();
  }

  NativePaimonFileWriter withParquetFloatingStats() {
    floatingStats = new PaimonParquetFloatingStats(writeType);
    return this;
  }

  NativePaimonFileWriter withRowEncoding(boolean enabled) {
    rowEncoding = enabled;
    return this;
  }

  @Override
  public void writeBundle(BundleRecords bundle) throws IOException {
    if (closed) throw new IllegalStateException("Paimon file writer is closed");
    if (bundle instanceof ArrowBatchBundle) {
      writeNative((ArrowBatchBundle) bundle);
      return;
    }
    for (InternalRow row : bundle) {
      addElement(row);
    }
  }

  @Override
  public void addElement(InternalRow row) throws IOException {
    if (closed) throw new IllegalStateException("Paimon file writer is closed");
    if (row instanceof ArrowBatchBundle.Cursor) {
      writeNative(((ArrowBatchBundle.Cursor) row).bundle());
    } else if (rowEncoding) {
      if (rows == null) rows = new PaimonRowBatch(writeType);
      rows.add(row);
      if (rows.isFull()) flushRows();
    } else {
      stock().addElement(row);
    }
  }

  private void flushRows() throws IOException {
    if (rows == null || rows.isEmpty()) return;
    writeRoot(rows.finish(), 0, rows.size());
    rows.reset();
  }

  private void writeNative(ArrowBatchBundle bundle) throws IOException {
    if (mode == Mode.STOCK) {
      throw new IllegalStateException(
          "an Arrow bundle reached a Paimon data file already written row by row");
    }
    if (bundle == lastBundle) {
      return;
    }
    flushRows();
    writeRoot(bundle.root(), bundle.rowOffset(), (int) bundle.rowCount());
    lastBundle = bundle;
  }

  private void writeRoot(VectorSchemaRoot root, int offset, int count) throws IOException {
    if (mode == Mode.UNDECIDED) {
      openEncoder(root);
    }
    encoder.write(root, new int[0], offset, count);
    if (compactMemory != null) compactMemory.checkWriter(encoder.bufferedBytes());
    if (floatingStats != null) floatingStats.collect(root, offset, count);
  }

  private void openEncoder(VectorSchemaRoot root) throws IOException {
    encoder =
        codec.createEncoder(
            PaimonArrowFields.annotate(root.getSchema(), writeType),
            new int[0],
            keys,
            values,
            false,
            out);
    mode = Mode.NATIVE;
  }

  private FormatWriter stock() throws IOException {
    if (mode == Mode.NATIVE) {
      throw new IllegalStateException(
          "a plain row reached a Paimon data file already written from Arrow bundles");
    }
    if (stock == null) {
      stock = stockFactory.create(out, compression);
      mode = Mode.STOCK;
    }
    return stock;
  }

  @Override
  public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
    if (mode == Mode.STOCK) return stock.reachTargetSize(suggestedCheck, targetSize);
    if (!suggestedCheck) return false;
    long estimated = mode == Mode.NATIVE ? encoder.estimatedBytes() : 0;
    if (rows != null
        && !rows.isEmpty()
        && estimated >= Math.max(0, targetSize - rows.allocatedBytes())) {
      flushRows();
      estimated = encoder.estimatedBytes();
    }
    return mode == Mode.NATIVE && estimated >= targetSize;
  }

  @Nullable
  public Object writerMetadata() {
    return mode == Mode.STOCK ? PaimonVersion.writerMetadata(stock) : floatingStats;
  }

  /** Whether this file used the native encoder. */
  public boolean wroteNatively() {
    return mode == Mode.NATIVE;
  }

  @Override
  public void close() throws IOException {
    if (closed) return;
    closed = true;
    try (PaimonRowBatch bufferedRows = rows;
        java.io.Closeable releaseEncoder = this::closeEncoder) {
      flushRows();
      switch (mode) {
        case NATIVE:
          encoder.finish();
          break;
        case STOCK:
          stock.close();
          break;
        case UNDECIDED:
          stock().close();
          break;
      }
    } finally {
      rows = null;
    }
  }

  private void closeEncoder() {
    if (encoder != null) {
      try {
        encoder.close();
      } finally {
        encoder = null;
      }
    }
  }
}

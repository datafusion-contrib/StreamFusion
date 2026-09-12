package tech.streamfusion.paimon;

import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.types.RowType;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.parquet.NativeParquet;

/**
 * A Paimon data-file writer that encodes Arrow bundles natively. The first record decides the file:
 * an {@link ArrowBatchBundle} opens the native encoder over Paimon's output stream and every later
 * bundle is appended to it column-wise, while a plain row hands the whole file to Paimon's stock
 * Parquet writer. Paimon's released append writer feeds a bundle to its format writer one cursor
 * row at a time, so the cursor path recognises each bundle once and encodes it whole; a Paimon that
 * passes the bundle through takes the direct path. Compaction rewrites arrive as plain rows and
 * take the stock path; the native append spill buffer drains Arrow bundles. The footer stays a
 * standard parquet-rs footer, so Paimon extracts statistics from it exactly as from its own files.
 */
public final class NativePaimonParquetWriter implements BundleFormatWriter {

  private static final int DRAIN_CHUNK_BYTES = 1 << 20;

  private enum Mode {
    UNDECIDED,
    NATIVE,
    STOCK
  }

  private final RowType writeType;
  private final PaimonParquetSettings settings;
  private final PositionOutputStream out;
  private final String compression;
  private final FormatWriterFactory stockFactory;

  private Mode mode = Mode.UNDECIDED;
  private long encoder;
  @Nullable private FormatWriter stock;
  @Nullable private ArrowBatchBundle lastBundle;

  NativePaimonParquetWriter(
      RowType writeType,
      PaimonParquetSettings settings,
      PositionOutputStream out,
      String compression,
      FormatWriterFactory stockFactory) {
    this.writeType = writeType;
    this.settings = settings;
    this.out = out;
    this.compression = compression;
    this.stockFactory = stockFactory;
  }

  @Override
  public void writeBundle(BundleRecords bundle) throws IOException {
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
    if (row instanceof ArrowBatchBundle.Cursor) {
      writeNative(((ArrowBatchBundle.Cursor) row).bundle());
    } else {
      stock().addElement(row);
    }
  }

  private void writeNative(ArrowBatchBundle bundle) throws IOException {
    if (mode == Mode.STOCK) {
      throw new IllegalStateException(
          "an Arrow bundle reached a Paimon data file already written row by row");
    }
    if (bundle == lastBundle) {
      return;
    }
    VectorSchemaRoot root = bundle.root();
    if (mode == Mode.UNDECIDED) {
      openEncoder(root);
    }
    BufferAllocator allocator = allocatorOf(root);
    try (ArrowArray array = ArrowArray.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, NativeAllocator.DICTIONARIES, array);
      NativeParquet.parquetEncoderWrite(
          encoder, array.memoryAddress(), new int[0], bundle.rowOffset(), (int) bundle.rowCount());
    }
    lastBundle = bundle;
  }

  private void openEncoder(VectorSchemaRoot root) {
    BufferAllocator allocator = allocatorOf(root);
    try (ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportSchema(
          allocator,
          PaimonArrowFields.annotate(root.getSchema(), writeType),
          NativeAllocator.DICTIONARIES,
          schema);
      encoder =
          NativeParquet.createParquetEncoder(
              schema.memoryAddress(),
              new int[0],
              settings.keys(),
              settings.values(),
              false,
              out,
              new byte[DRAIN_CHUNK_BYTES]);
    }
    mode = Mode.NATIVE;
  }

  private static BufferAllocator allocatorOf(VectorSchemaRoot root) {
    return root.getFieldVectors().isEmpty()
        ? NativeAllocator.SHARED
        : root.getFieldVectors().get(0).getAllocator();
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
    return switch (mode) {
      case NATIVE -> suggestedCheck && NativeParquet.parquetEncoderEstimatedBytes(encoder) >= targetSize;
      case STOCK -> stock.reachTargetSize(suggestedCheck, targetSize);
      case UNDECIDED -> false;
    };
  }

  @Nullable
  @Override
  public Object writerMetadata() {
    return mode == Mode.STOCK ? stock.writerMetadata() : null;
  }

  /** Whether this file was written from Arrow bundles rather than by Paimon's stock writer. */
  public boolean wroteNatively() {
    return mode == Mode.NATIVE;
  }

  @Override
  public void close() throws IOException {
    switch (mode) {
      case NATIVE -> {
        try {
          NativeParquet.parquetEncoderFinish(encoder);
        } finally {
          NativeParquet.closeParquetEncoder(encoder);
          encoder = 0;
        }
      }
      case STOCK -> stock.close();
      case UNDECIDED -> stock().close();
    }
  }
}

package tech.streamfusion.operator;

import tech.streamfusion.parquet.NativeParquet;
import tech.streamfusion.arrow.ArrowConversion;
import java.io.IOException;
import java.lang.ref.Cleaner;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.table.types.logical.RowType;

/**
 * Creates the native Parquet writers behind the sink's part files. Each part file pairs a native
 * encoder (Arrow batches in, encoded Parquet bytes out through an in-memory buffer) with the Flink
 * {@link FSDataOutputStream} the bucket opened, so the bytes travel Flink's own recoverable-stream
 * path — any Flink filesystem, the host's exactly-once commit — while the encoding never touches
 * rows. The drain is a single memcpy per chunk: the native side pins the reusable array and copies
 * flushed compressed bytes straight into it.
 */
public final class NativeParquetBulkWriterFactory
    implements BulkWriter.Factory<PartitionedArrowBatch> {

  private static final int DRAIN_CHUNK_BYTES = 1 << 20;

  private final RowType rowType;
  private final int[] partitionColumns;
  private final String[] configKeys;
  private final String[] configValues;

  public NativeParquetBulkWriterFactory(
      RowType rowType, int[] partitionColumns, String[] configKeys, String[] configValues) {
    this.rowType = rowType;
    this.partitionColumns = partitionColumns;
    this.configKeys = configKeys;
    this.configValues = configValues;
  }

  @Override
  public BulkWriter<PartitionedArrowBatch> create(FSDataOutputStream out) {
    BufferAllocator allocator = NativeAllocator.SHARED;
    long encoder;
    try (ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportSchema(
          allocator, ArrowConversion.toArrowSchema(rowType), NativeAllocator.DICTIONARIES, schema);
      encoder =
          NativeParquet.createParquetEncoder(
              schema.memoryAddress(), partitionColumns, configKeys, configValues);
    }
    return new NativeParquetBulkWriter(encoder, out);
  }

  private static final class NativeParquetBulkWriter implements BulkWriter<PartitionedArrowBatch> {

    private static final Cleaner ABANDONED = Cleaner.create();

    private final long encoder;
    private final FSDataOutputStream out;
    private final byte[] chunk = new byte[DRAIN_CHUNK_BYTES];
    private final Backstop backstop;

    private NativeParquetBulkWriter(long encoder, FSDataOutputStream out) {
      this.encoder = encoder;
      this.out = out;
      // Flink disposes an in-progress part file by closing only its stream — the bulk writer is
      // dropped without finish() — so a backstop frees the native encoder when that happens.
      this.backstop = new Backstop(encoder);
      ABANDONED.register(this, backstop);
    }

    @Override
    public void addElement(PartitionedArrowBatch element) throws IOException {
      VectorSchemaRoot batch = element.root();
      BufferAllocator batchAllocator =
          batch.getFieldVectors().isEmpty()
              ? NativeAllocator.SHARED
              : batch.getFieldVectors().get(0).getAllocator();
      try (ArrowArray array = ArrowArray.allocateNew(batchAllocator);
          ArrowSchema schema = ArrowSchema.allocateNew(batchAllocator)) {
        Data.exportVectorSchemaRoot(
            batchAllocator, batch, NativeAllocator.DICTIONARIES, array, schema);
        NativeParquet.parquetEncoderWrite(encoder, array.memoryAddress(), schema.memoryAddress());
      } finally {
        batch.close();
      }
      drain();
    }

    @Override
    public void flush() throws IOException {
      drain();
    }

    @Override
    public void finish() throws IOException {
      NativeParquet.parquetEncoderFinish(encoder);
      drain();
      backstop.released = true;
      NativeParquet.closeParquetEncoder(encoder);
    }

    private void drain() throws IOException {
      int filled;
      while ((filled = NativeParquet.parquetEncoderDrain(encoder, chunk)) > 0) {
        out.write(chunk, 0, filled);
      }
    }

    /** Frees the encoder of a part file disposed without finish; must not reference its writer. */
    private static final class Backstop implements Runnable {

      private final long encoder;
      private volatile boolean released;

      private Backstop(long encoder) {
        this.encoder = encoder;
      }

      @Override
      public void run() {
        if (!released) {
          NativeParquet.closeParquetEncoder(encoder);
        }
      }
    }
  }
}

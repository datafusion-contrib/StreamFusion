package tech.streamfusion.operator;

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
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.parquet.NativeParquet;

/**
 * Creates the native Parquet writers behind the sink's part files. Each part file pairs a native
 * encoder (Arrow batches in, encoded Parquet column chunks out through a bounded bridge) with the
 * Flink {@link FSDataOutputStream} the bucket opened, so the bytes travel Flink's own
 * recoverable-stream path — any Flink filesystem, the host's exactly-once commit — while the
 * encoding never touches rows. The lower-level native writer appends one column chunk at a time and
 * forwards bytes through a reusable one-MiB array, so the bridge never stages a second complete
 * compressed row group before Flink can form filesystem upload parts.
 */
public final class NativeParquetBulkWriterFactory
    implements BulkWriter.Factory<PartitionedArrowBatch> {

  private static final int DRAIN_CHUNK_BYTES = 1 << 20;

  private final RowType rowType;
  private final int[] partitionColumns;
  private final String[] configKeys;
  private final String[] configValues;
  private final boolean changelog;

  public NativeParquetBulkWriterFactory(
      RowType rowType, int[] partitionColumns, String[] configKeys, String[] configValues) {
    this(rowType, partitionColumns, configKeys, configValues, false);
  }

  public NativeParquetBulkWriterFactory(
      RowType rowType,
      int[] partitionColumns,
      String[] configKeys,
      String[] configValues,
      boolean changelog) {
    this.rowType = rowType;
    this.partitionColumns = partitionColumns;
    this.configKeys = configKeys;
    this.configValues = configValues;
    this.changelog = changelog;
  }

  @Override
  public BulkWriter<PartitionedArrowBatch> create(FSDataOutputStream out) {
    BufferAllocator allocator = NativeAllocator.SHARED;
    byte[] chunk = new byte[DRAIN_CHUNK_BYTES];
    long encoder;
    try (ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportSchema(
          allocator, ArrowConversion.toArrowSchema(rowType), NativeAllocator.DICTIONARIES, schema);
      encoder =
          NativeParquet.createParquetEncoder(
              schema.memoryAddress(),
              partitionColumns,
              configKeys,
              configValues,
              changelog,
              out,
              chunk);
    }
    return new NativeParquetBulkWriter(encoder);
  }

  private static final class NativeParquetBulkWriter implements BulkWriter<PartitionedArrowBatch> {

    private static final Cleaner ABANDONED = Cleaner.create();

    private final long encoder;
    private final Backstop backstop;

    private NativeParquetBulkWriter(long encoder) {
      this.encoder = encoder;
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
      try (ArrowArray array = ArrowArray.allocateNew(batchAllocator)) {
        Data.exportVectorSchemaRoot(batchAllocator, batch, NativeAllocator.DICTIONARIES, array);
        NativeParquet.parquetEncoderWrite(encoder, array.memoryAddress(), new int[0]);
      } finally {
        batch.close();
      }
    }

    @Override
    public void flush() throws IOException {
      // Completed row groups are forwarded while addElement runs; an incomplete row group has no
      // valid bytes to flush without changing the file's row-group layout.
    }

    @Override
    public void finish() throws IOException {
      NativeParquet.parquetEncoderFinish(encoder);
      backstop.released = true;
      NativeParquet.closeParquetEncoder(encoder);
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

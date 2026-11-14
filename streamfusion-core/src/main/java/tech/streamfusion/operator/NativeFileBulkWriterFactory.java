package tech.streamfusion.operator;

import java.io.IOException;
import java.lang.ref.Cleaner;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.compat.FileSinkCompat;
import tech.streamfusion.format.ColumnarFileCodec;

/**
 * Creates the native columnar writers behind the sink's part files. Each part file pairs a native
 * Arrow file writer with the Flink {@link FSDataOutputStream} the bucket opened, so the bytes
 * travel Flink's own recoverable-stream path — any Flink filesystem, the host's exactly-once commit
 * — while the encoding never touches Java rows. Encoded bytes cross JNI through a reusable one-MiB
 * array.
 */
public class NativeFileBulkWriterFactory implements BulkWriter.Factory<PartitionedArrowBatch> {

  private final ColumnarFileCodec codec;
  private final RowType rowType;
  private final int[] partitionColumns;
  private final String[] configKeys;
  private final String[] configValues;
  private final boolean changelog;

  public NativeFileBulkWriterFactory(
      ColumnarFileCodec codec,
      RowType rowType,
      int[] partitionColumns,
      String[] configKeys,
      String[] configValues) {
    this(codec, rowType, partitionColumns, configKeys, configValues, false);
  }

  public NativeFileBulkWriterFactory(
      ColumnarFileCodec codec,
      RowType rowType,
      int[] partitionColumns,
      String[] configKeys,
      String[] configValues,
      boolean changelog) {
    this.codec = codec;
    this.rowType = rowType;
    this.partitionColumns = partitionColumns;
    this.configKeys = configKeys;
    this.configValues = configValues;
    this.changelog = changelog;
  }

  @Override
  public BulkWriter<PartitionedArrowBatch> create(FSDataOutputStream out) throws IOException {
    ColumnarFileCodec.Encoder encoder =
        codec.createEncoder(
            FileSinkCompat.encoderSchema(rowType),
            partitionColumns,
            configKeys,
            configValues,
            changelog,
            out);
    return new NativeFileBulkWriter(encoder);
  }

  private static final class NativeFileBulkWriter implements BulkWriter<PartitionedArrowBatch> {

    private static final Cleaner ABANDONED = Cleaner.create();

    private final ColumnarFileCodec.Encoder encoder;
    private final Backstop backstop;
    private final Cleaner.Cleanable cleanable;

    private NativeFileBulkWriter(ColumnarFileCodec.Encoder encoder) {
      this.encoder = encoder;
      // Flink disposes an in-progress part file by closing only its stream — the bulk writer is
      // dropped without finish() — so a backstop releases the encoder when that happens.
      this.backstop = new Backstop(encoder);
      cleanable = ABANDONED.register(this, backstop);
    }

    @Override
    public void addElement(PartitionedArrowBatch element) throws IOException {
      VectorSchemaRoot batch = element.root();
      try {
        encoder.write(batch, new int[0], 0, -1);
      } finally {
        batch.close();
      }
    }

    @Override
    public void flush() throws IOException {
      // The codec owns stripe or row-group finalization; Flink calls finish before publishing a
      // part file.
    }

    @Override
    public void finish() throws IOException {
      if (backstop.released) return;
      try {
        encoder.finish();
      } finally {
        cleanable.clean();
      }
    }

    /** Frees the encoder of a part file disposed without finish; must not reference its writer. */
    private static final class Backstop implements Runnable {

      private final ColumnarFileCodec.Encoder encoder;
      private volatile boolean released;

      private Backstop(ColumnarFileCodec.Encoder encoder) {
        this.encoder = encoder;
      }

      @Override
      public void run() {
        if (!released) {
          released = true;
          encoder.close();
        }
      }
    }
  }
}

package tech.streamfusion.orc;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.flink.orc.writer.PhysicalWriterImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.WriterImpl;

/** Uses Flink's normal physical writer over its checkpointed part-file stream. */
final class FlinkOrcWriter implements OrcVectorWriter.Factory {
  @Override
  public OrcVectorWriter create(
      String description, List<Field> fields, Properties properties, OrcOutput output)
      throws IOException {
    var schema = TypeDescription.fromString(description);
    var options =
        OrcFile.writerOptions(properties, new Configuration(false))
            .setSchema(schema)
            .useUTCTimestamp(true)
            .enforceBufferSize();
    var physical =
        new PhysicalWriterImpl(output, options) {
          private boolean closed;

          @Override
          public void close() {
            if (!closed) {
              closed = true;
              super.close();
            }
          }
        };
    options.physicalWriter(physical);
    var path = new Path(UUID.randomUUID().toString());
    final WriterImpl writer;
    try {
      writer = new WriterImpl(null, path, options);
    } catch (IOException | RuntimeException failure) {
      output.discard();
      physical.close();
      throw failure;
    }
    var batch = schema.createRowBatch(TypeDescription.RowBatchVersion.USE_DECIMAL64, 4096);
    return new OrcVectorWriter() {
      private boolean closed, marked;

      public Object batch() {
        return batch;
      }

      public void addBatch() throws IOException {
        writer.addRowBatch(batch);
        if (!marked) {
          writer.addUserMetadata(
              OrcVectorWriter.ARROW_WRITER_METADATA, java.nio.ByteBuffer.wrap(new byte[] {1}));
          marked = true;
        }
      }

      public long estimatedBytes() {
        return 0; /* Flink rolls part files using its stream position. */
      }

      public void close() throws IOException {
        if (closed) return;
        closed = true;
        try {
          writer.close();
        } finally {
          try {
            physical.close();
          } finally {
            options.getMemoryManager().removeWriter(path);
          }
        }
      }
    };
  }
}

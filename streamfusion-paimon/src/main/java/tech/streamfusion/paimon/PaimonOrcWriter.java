package tech.streamfusion.paimon;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.paimon.shade.org.apache.orc.OrcFile;
import org.apache.paimon.shade.org.apache.orc.TypeDescription;
import org.apache.paimon.shade.org.apache.orc.impl.PhysicalFsWriter;
import org.apache.paimon.shade.org.apache.orc.impl.WriterImpl;
import org.apache.paimon.shade.org.apache.orc.impl.writer.WriterEncryptionVariant;
import tech.streamfusion.orc.OrcOutput;
import tech.streamfusion.orc.OrcVectorWriter;

/** Uses Paimon's shaded ORC release and keeps its recursive schema evolution field IDs. */
final class PaimonOrcWriter implements OrcVectorWriter.Factory {
  private static void attributes(Field field, TypeDescription type) {
    String id = field.getMetadata() == null ? null : field.getMetadata().get("PARQUET:field_id");
    if (id != null) type.setAttribute("paimon.id", id);
    if (tech.streamfusion.arrow.TimestampAccessor.isComponentTimestamp(field)) return;
    var children = field.getChildren();
    if (field.getType() instanceof ArrowType.Map) children = children.get(0).getChildren();
    for (int c = 0; c < children.size(); c++)
      attributes(children.get(c), type.getChildren().get(c));
  }

  @Override
  public OrcVectorWriter create(
      String description, List<Field> fields, Properties properties, OrcOutput output)
      throws IOException {
    var schema = TypeDescription.fromString(description);
    for (int c = 0; c < fields.size(); c++) attributes(fields.get(c), schema.getChildren().get(c));
    var options =
        OrcFile.writerOptions(properties, new Configuration(false))
            .setSchema(schema)
            .useUTCTimestamp(true)
            .enforceBufferSize();
    var physical =
        new PhysicalFsWriter(
            new FSDataOutputStream(output, null), options, new WriterEncryptionVariant[0]) {
          private boolean closed;

          @Override
          public void close() throws IOException {
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
      try {
        physical.close();
      } catch (IOException cleanup) {
        failure.addSuppressed(cleanup);
      }
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
        return writer.estimateMemory();
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

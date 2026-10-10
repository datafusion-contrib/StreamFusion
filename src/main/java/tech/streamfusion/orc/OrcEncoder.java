package tech.streamfusion.orc;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.IntStream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import tech.streamfusion.compat.ListCollectors;
import tech.streamfusion.format.ColumnarFileCodec;
import tech.streamfusion.operator.NativeAllocator;

/** A file-scoped Java ORC writer that consumes Arrow buffers synchronously. */
final class OrcEncoder implements ColumnarFileCodec.Encoder {
  private static final int BATCH_ROWS = 4096;
  private final VectorSchemaRoot imported;
  private final OrcVectorWriter writer;
  private final ArrowOrcVectors vectors;
  private final OrcOutput output;
  private boolean finished, closed;

  OrcEncoder(
      org.apache.arrow.vector.types.pojo.Schema schema,
      String description,
      int[] partitions,
      String[] keys,
      String[] values,
      OutputStream output,
      OrcVectorWriter.Factory factory)
      throws IOException {
    if (keys.length != values.length) throw new IllegalArgumentException("ORC option count");
    var allocator = NativeAllocator.SHARED;
    int[] projection =
        IntStream.range(0, schema.getFields().size())
            .filter(c -> Arrays.stream(partitions).noneMatch(p -> p == c))
            .toArray();
    var fields =
        Arrays.stream(projection)
            .mapToObj(schema.getFields()::get)
            .collect(ListCollectors.toList());
    var properties = new Properties();
    boolean legacyTimestampLtz = true;
    for (int i = 0; i < keys.length; i++) {
      if (keys[i].equals("legacy.timestamp-ltz")) {
        legacyTimestampLtz = Boolean.parseBoolean(values[i]);
        continue;
      }
      if (keys[i].equals("timezone")) continue;
      properties.setProperty(
          "orc." + (keys[i].equals("compression") ? "compress" : keys[i]), values[i]);
    }
    this.output = new OrcOutput(output);
    imported = VectorSchemaRoot.create(schema, allocator);
    OrcVectorWriter opened = null;
    try {
      opened = factory.create(description, fields, properties, this.output);
      vectors = new ArrowOrcVectors(schema.getFields(), opened.batch(), projection, legacyTimestampLtz);
      writer = opened;
    } catch (IOException | RuntimeException | Error failure) {
      this.output.discard();
      if (opened != null) {
        try {
          opened.close();
        } catch (IOException cleanup) {
          failure.addSuppressed(cleanup);
        }
      }
      imported.close();
      throw failure;
    }
  }

  @Override
  public void write(long array, int[] selected, int offset, int count) throws IOException {
    try {
      Data.importIntoVectorSchemaRoot(
          NativeAllocator.SHARED, ArrowArray.wrap(array), imported, NativeAllocator.DICTIONARIES);
      write(imported, selected, offset, count);
    } finally {
      imported.clear();
    }
  }

  @Override
  public void write(VectorSchemaRoot batch, int[] selected, int offset, int count)
      throws IOException {
    if (finished || closed) throw new IllegalStateException("ORC writer is closed");
    if (batch.getFieldVectors().size() != imported.getFieldVectors().size())
      throw new IllegalArgumentException("ORC column count");
    if (selected.length != 0) {
      if (offset != 0 || (count >= 0 && count != selected.length))
        throw new IllegalArgumentException("ORC selected row range");
      for (int row : selected) Objects.checkIndex(row, batch.getRowCount());
      // Native partition routing already sends contiguous batches. Preserve the selected-row
      // contract for other callers without allocating a second set of Arrow buffers.
      for (int row : selected) {
        vectors.copy(batch, row, 1);
        writer.addBatch();
      }
    } else {
      if (count < 0) count = batch.getRowCount() - offset;
      Objects.checkFromIndexSize(offset, count, batch.getRowCount());
      for (int at = 0; at < count; at += BATCH_ROWS) {
        vectors.copy(batch, offset + at, Math.min(BATCH_ROWS, count - at));
        writer.addBatch();
      }
    }
  }

  @Override
  public long estimatedBytes() {
    return output.getPos()
        + (finished || closed ? 0 : writer.estimatedBytes() + vectors.estimatedBytes());
  }

  @Override
  public void finish() throws IOException {
    if (finished) return;
    if (closed) throw new IllegalStateException("ORC writer is closed");
    writer.close();
    finished = true;
  }

  @Override
  public void close() {
    if (closed) return;
    closed = true;
    output.discard();
    try {
      writer.close();
    } catch (IOException cleanup) {
      org.slf4j.LoggerFactory.getLogger(OrcEncoder.class)
          .warn("Closing abandoned ORC writer", cleanup);
    } finally {
      imported.close();
    }
  }
}

package tech.streamfusion.iceberg;

import java.io.IOException;
import org.apache.arrow.vector.VectorSchemaRoot;

interface IcebergTaskBatchReader extends AutoCloseable {
  VectorSchemaRoot next();

  @Override
  void close() throws IOException;
}

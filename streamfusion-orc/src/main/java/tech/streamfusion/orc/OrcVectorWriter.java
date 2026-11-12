package tech.streamfusion.orc;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import org.apache.arrow.vector.types.pojo.Field;

/** The host supplies its released ORC writer and Hive vector classes. */
public interface OrcVectorWriter extends AutoCloseable {
  String ARROW_WRITER_METADATA = "streamfusion.arrow.writer";

  Object batch();

  void addBatch() throws IOException;

  long estimatedBytes();

  @Override
  void close() throws IOException;

  interface Factory extends Serializable {
    OrcVectorWriter create(
        String description, List<Field> fields, Properties properties, OrcOutput output)
        throws IOException;
  }
}

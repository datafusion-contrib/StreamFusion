package tech.streamfusion.paimon;

import java.io.IOException;
import java.util.function.Function;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.SingleFileWriter;

/** Accesses file metadata through the matching released writer API. */
abstract class PaimonSingleFileWriter<T, R> extends SingleFileWriter<T, R> {
  PaimonSingleFileWriter(
      FileIO io,
      FormatWriterFactory factory,
      Path path,
      Function<T, InternalRow> converter,
      String compression,
      boolean async) {
    super(io, factory, path, converter, compression, async);
  }

  final long bytes() throws IOException {
    return fileIO.getFileSize(path);
  }

  final Object metadata() {
    return null;
  }
}

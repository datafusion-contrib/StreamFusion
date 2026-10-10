package tech.streamfusion.orc;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.flink.core.fs.FSDataOutputStream;

/** The connector owns the stream. Aborting discards finalization writes while releasing ORC. */
public final class OrcOutput extends FSDataOutputStream {
  private final OutputStream output;
  private long position;
  private boolean discarded;

  public OrcOutput(OutputStream output) {
    this.output = output;
  }

  public void discard() {
    discarded = true;
  }

  @Override
  public long getPos() {
    return position;
  }

  @Override
  public void write(int value) throws IOException {
    if (!discarded) {
      output.write(value);
      position++;
    }
  }

  @Override
  public void write(byte[] bytes, int offset, int length) throws IOException {
    if (!discarded) {
      output.write(bytes, offset, length);
      position += length;
    }
  }

  @Override
  public void flush() throws IOException {
    if (!discarded) output.flush();
  }

  @Override
  public void sync() throws IOException {
    if (!discarded && output instanceof FSDataOutputStream) {
      FSDataOutputStream stream = ((FSDataOutputStream) output);
      stream.sync();
    }
  }

  @Override
  public void close() {}
}

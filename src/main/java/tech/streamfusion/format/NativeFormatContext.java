package tech.streamfusion.format;

import java.io.Serializable;
import java.util.Map;
import org.apache.flink.table.types.logical.RowType;

/** Planner-owned inputs a format extension needs to build a message decoder. */
public final class NativeFormatContext implements Serializable {

  private static final long serialVersionUID = 1L;

  private final RowType outputType;
  private final RowType writerType;
  private final Map<String, String> options;
  private final boolean ignoreParseErrors;

  public NativeFormatContext(
      RowType outputType,
      RowType writerType,
      Map<String, String> options,
      boolean ignoreParseErrors) {
    this.outputType = outputType;
    this.writerType = writerType;
    this.options = Map.copyOf(options);
    this.ignoreParseErrors = ignoreParseErrors;
  }

  public RowType outputType() {
    return outputType;
  }

  public RowType writerType() {
    return writerType;
  }

  public Map<String, String> options() {
    return options;
  }

  public boolean ignoreParseErrors() {
    return ignoreParseErrors;
  }
}

package tech.streamfusion.compat;

public abstract class RichMapFunction<I, O>
    extends org.apache.flink.api.common.functions.RichMapFunction<I, O> {
  @Override
  public final void open(org.apache.flink.api.common.functions.OpenContext context)
      throws Exception {
    initialize();
  }

  protected abstract void initialize() throws Exception;
}

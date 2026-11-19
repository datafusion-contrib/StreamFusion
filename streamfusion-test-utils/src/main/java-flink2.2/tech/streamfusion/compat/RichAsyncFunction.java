package tech.streamfusion.compat;

public abstract class RichAsyncFunction<I, O>
    extends org.apache.flink.streaming.api.functions.async.RichAsyncFunction<I, O> {
  @Override
  public final void open(org.apache.flink.api.common.functions.OpenContext context)
      throws Exception {
    initialize();
  }

  protected abstract void initialize() throws Exception;
}

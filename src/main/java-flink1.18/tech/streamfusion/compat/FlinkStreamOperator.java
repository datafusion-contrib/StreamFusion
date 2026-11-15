package tech.streamfusion.compat;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;

/** Keep columnar operators in the upstream task chain when Flink's topology permits it. */
public abstract class FlinkStreamOperator<OUT> extends AbstractStreamOperator<OUT> {
  private static final long serialVersionUID = 1L;

  protected FlinkStreamOperator() {
    setChainingStrategy(ChainingStrategy.ALWAYS);
  }

  @Override
  public void setup(
      org.apache.flink.streaming.runtime.tasks.StreamTask<?, ?> task,
      org.apache.flink.streaming.api.graph.StreamConfig config,
      org.apache.flink.streaming.api.operators.Output<
              org.apache.flink.streaming.runtime.streamrecord.StreamRecord<OUT>>
          output) {
    super.setup(task, config, output);
    tech.streamfusion.shuffle.NativeShuffleDecompression.install(task, getMetricGroup());
  }
}

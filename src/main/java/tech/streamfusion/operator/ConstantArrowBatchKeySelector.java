package tech.streamfusion.operator;

import org.apache.flink.api.java.functions.KeySelector;

/** Runtime-only state key selector for operators that deliberately use one Flink key. */
public final class ConstantArrowBatchKeySelector implements KeySelector<ArrowBatch, Integer> {
  private static final long serialVersionUID = 1L;

  private final int key;

  public ConstantArrowBatchKeySelector(int key) {
    this.key = key;
  }

  @Override
  public Integer getKey(ArrowBatch batch) {
    return key;
  }
}

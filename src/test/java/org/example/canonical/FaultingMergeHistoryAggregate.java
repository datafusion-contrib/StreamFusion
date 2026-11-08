package org.example.canonical;

import org.example.canonical.CanonicalFixtureFactory.MergeHistoryAccumulator;
import org.example.canonical.CanonicalFixtureFactory.MergeHistoryAggregate;

public final class FaultingMergeHistoryAggregate
    extends org.apache.flink.table.functions.AggregateFunction<Long, MergeHistoryAccumulator> {
  private final MergeHistoryAggregate delegate = new MergeHistoryAggregate();

  @Override
  public MergeHistoryAccumulator createAccumulator() {
    return delegate.createAccumulator();
  }

  @Override
  public Long getValue(MergeHistoryAccumulator accumulator) {
    return delegate.getValue(accumulator);
  }

  public void accumulate(MergeHistoryAccumulator accumulator, Long value) throws Exception {
    if (value != null && value == Long.MIN_VALUE)
      throw new IllegalStateException("Injected local UDF failure");
    delegate.accumulate(accumulator, value);
  }

  public void retract(MergeHistoryAccumulator accumulator, Long value) throws Exception {
    delegate.retract(accumulator, value);
  }

  public void merge(MergeHistoryAccumulator accumulator, Iterable<MergeHistoryAccumulator> partials)
      throws Exception {
    var retained = new java.util.ArrayList<MergeHistoryAccumulator>();
    for (var partial : partials) {
      if (partial.counts.get(Long.MAX_VALUE) != null)
        throw new IllegalStateException("Injected global UDF failure");
      retained.add(partial);
    }
    delegate.merge(accumulator, retained);
  }
}

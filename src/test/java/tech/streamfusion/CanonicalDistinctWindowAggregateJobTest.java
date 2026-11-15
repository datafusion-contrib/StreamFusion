package tech.streamfusion;
class CanonicalDistinctWindowAggregateJobTest extends CanonicalFixedWindowAggregateJobTest {
  @Override boolean distinct() { return true; }
}

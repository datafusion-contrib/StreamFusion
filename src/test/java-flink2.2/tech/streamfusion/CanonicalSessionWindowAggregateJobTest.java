package tech.streamfusion;
class CanonicalSessionWindowAggregateJobTest extends CanonicalFixedWindowAggregateJobTest {
  @Override boolean session() { return true; }
}

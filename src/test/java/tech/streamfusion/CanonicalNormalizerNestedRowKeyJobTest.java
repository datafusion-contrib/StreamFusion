package tech.streamfusion;

class CanonicalNormalizerNestedRowKeyJobTest extends CanonicalNormalizerRowKeyJobTest {
  @Override
  boolean nested() {
    return true;
  }
}

package tech.streamfusion;

class CanonicalCachedMiniBatchManagedUdfGroupJobTest
    extends CanonicalMiniBatchManagedUdfGroupJobTest {
  @Override
  int cacheEntries() {
    return 16;
  }
}

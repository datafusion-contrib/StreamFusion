package tech.streamfusion;

class CanonicalCachedManagedUdfGroupJobTest extends CanonicalManagedUdfGroupJobTest {
  @Override
  int cacheEntries() {
    return 16;
  }
}

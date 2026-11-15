package tech.streamfusion;

class CanonicalCachedGlobalManagedUdfGroupJobTest extends CanonicalGlobalManagedUdfGroupJobTest {
  @Override
  int cacheEntries() {
    return 16;
  }
}

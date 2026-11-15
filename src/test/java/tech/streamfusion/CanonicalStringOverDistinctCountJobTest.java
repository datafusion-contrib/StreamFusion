package tech.streamfusion;

class CanonicalStringOverDistinctCountJobTest extends CanonicalScalarOverDistinctCountJobTest {
  @Override
  String argumentType() {
    return "VARCHAR(100)";
  }
}

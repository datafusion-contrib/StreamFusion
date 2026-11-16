package tech.streamfusion;

class CanonicalVariableExtremaKeyJobTest extends CanonicalCompositeExtremaKeyJobTest {
  @Override
  String keyColumns() {
    return "ka VARCHAR(100),kb DECIMAL(38,7),v BIGINT";
  }

  @Override
  String firstKey(String value) {
    return value.equals("null")
        ? value
        : String.format(java.util.Locale.ROOT, "%020d", Long.parseLong(value));
  }
}

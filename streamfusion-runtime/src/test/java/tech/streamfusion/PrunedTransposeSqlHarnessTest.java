package tech.streamfusion;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.runtime.common.NativeParity;

class PrunedTransposeSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void reusedWideRowsMatchReleasedFlink(boolean binary) throws Exception {
    NativeParity.assertParity(
        () -> PrunedTransposeBenchmark.environment(binary, 65536, 2051),
        PrunedTransposeBenchmark.QUERY);
  }
}

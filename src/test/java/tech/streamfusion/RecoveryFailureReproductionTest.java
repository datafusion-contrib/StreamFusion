package tech.streamfusion;

import java.util.stream.IntStream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.operator.CoalescingOff;

@ExtendWith(CoalescingOff.class)
class RecoveryFailureReproductionTest {
  @ParameterizedTest(name = "overflow-repetition={0}")
  @MethodSource("repetitions")
  void repeatedOverflow(int repetition) {
    new StatefulRecoveryMatrixTest().filteredConversionsAcrossTwoRestores(
        repetition % 2 != 0, 1, 0, false, true);
  }

  static IntStream repetitions() {
    return IntStream.range(0, Integer.getInteger("sf.recovery.repetitions", 20));
  }
}

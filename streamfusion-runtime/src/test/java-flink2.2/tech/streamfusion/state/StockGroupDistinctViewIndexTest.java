package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class StockGroupDistinctViewIndexTest {
  private static StockGroupDistinctView view(int... aggregates) {
    return new StockGroupDistinctView(
        "distinct$0",
        new IntType(),
        IntSerializer.INSTANCE,
        LongSerializer.INSTANCE,
        aggregates,
        false);
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 255, 256, Integer.MAX_VALUE})
  void unencodableCompanionIdsAreRejectedBeforeNativeInitialization(int slot) {
    assertThrows(IllegalArgumentException.class, () -> view(slot));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 254})
  void allNonzeroByteTableIdsRemainAvailable(int slot) {
    assertDoesNotThrow(() -> view(slot));
  }

  @Test
  void sharedMembershipBitsCannotAliasOneNativeAggregate() {
    assertThrows(IllegalArgumentException.class, () -> view(1, 1));
  }

  @Test
  void anEmptyViewCannotClaimAnAccumulatorPlaceholder() {
    assertThrows(IllegalArgumentException.class, () -> view());
  }
}

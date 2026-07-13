package io.github.jordepic.streamfusion.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.junit.jupiter.api.Test;

class MiniBatchBoundaryTest {

  @Test
  void splitsPhysicalBatchesAtExactLogicalBoundaries() {
    MiniBatchBoundary boundary = new MiniBatchBoundary(4);

    assertThat(boundary.nextSliceLength(3)).isEqualTo(3);
    assertThat(boundary.onSlice(3)).isFalse();
    assertThat(boundary.nextSliceLength(5)).isEqualTo(1);
    assertThat(boundary.onSlice(1)).isTrue();
    assertThat(boundary.nextSliceLength(4)).isEqualTo(4);
    assertThat(boundary.onSlice(4)).isTrue();
    assertThat(boundary.bufferedRows()).isZero();
  }

  @Test
  void resetStartsANewLogicalBundle() {
    MiniBatchBoundary boundary = new MiniBatchBoundary(4);

    assertThat(boundary.onSlice(3)).isFalse();
    boundary.reset();

    assertThat(boundary.bufferedRows()).isZero();
    assertThat(boundary.nextSliceLength(4)).isEqualTo(4);
  }

  @Test
  void disabledCountBoundaryAcceptsTheWholePhysicalBatch() {
    MiniBatchBoundary boundary = new MiniBatchBoundary(-1);

    assertThat(boundary.nextSliceLength(50_000)).isEqualTo(50_000);
    assertThat(boundary.onSlice(50_000)).isFalse();
    assertThat(boundary.bufferedRows()).isEqualTo(50_000);
  }

  @Test
  void rejectsASliceThatWouldOvershoot() {
    MiniBatchBoundary boundary = new MiniBatchBoundary(4);
    boundary.onSlice(3);

    assertThatIllegalArgumentException().isThrownBy(() -> boundary.onSlice(2));
  }
}

package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The FLOAT/DOUBLE spelling probe and its data plane. The suite runs on the parity JDK (17), so
 * the probe must pass, and a wide seeded differential pins the native legacy-algorithm port
 * against {@code Double.toString}/{@code Float.toString} value by value.
 */
@Tag("streamfusion-kafka")
class JdkFloatSpellingTest {

  @Test
  void probePassesOnTheParityJdk() {
    assertTrue(JdkFloatSpelling.nativeMatchesJvm());
  }

  @Test
  void nativeSpellingMatchesJvmOnWideRandomSweep() {
    double[] doubles = FloatingPointCorpus.randomDoubles(200_000, 0xD1FFE7E57DL);
    float[] floats = FloatingPointCorpus.randomFloats(200_000, 0xF1FFE7E57DL);
    byte[] spelled = NativeKafka.spellFloatingPoint(doubles, floats);

    String[] lines = new String(spelled, StandardCharsets.UTF_8).split("\n", -1);
    assertEquals(doubles.length + floats.length + 1, lines.length);
    for (int i = 0; i < doubles.length; i++) {
      assertEquals(Double.toString(doubles[i]), lines[i]);
    }
    for (int i = 0; i < floats.length; i++) {
      assertEquals(Float.toString(floats[i]), lines[doubles.length + i]);
    }
  }
}

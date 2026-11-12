package tech.streamfusion.kafka;

import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * The plan-time probe deciding whether the native library spells FLOAT/DOUBLE exactly like this
 * JVM's {@code Float.toString}/{@code Double.toString}. The native side ports the legacy (JDK ≤
 * 18) algorithm; JDK 19 switched to shortest-representation digits, which differ on ~0.3% of
 * random doubles and ~11% of random floats. The corpus deliberately contains values where the two
 * algorithms disagree, so on a JDK 19+ host the probe fails closed and FLOAT/DOUBLE columns keep
 * host serialization instead of silently diverging.
 */
public final class JdkFloatSpelling {

  private JdkFloatSpelling() {}

  private static volatile Boolean nativeMatchesJvm;

  /** Test hook forcing the probe's answer; null probes for real. */
  static Boolean probeOverride;

  public static boolean nativeMatchesJvm() {
    Boolean override = probeOverride;
    if (override != null) {
      return override;
    }
    Boolean matches = nativeMatchesJvm;
    if (matches == null) {
      matches = probe();
      nativeMatchesJvm = matches;
    }
    return matches;
  }

  private static boolean probe() {
    double[] doubles = probeDoubles();
    float[] floats = probeFloats();
    byte[] spelled;
    try {
      spelled = NativeKafka.spellFloatingPoint(doubles, floats);
    } catch (LinkageError missingLibrary) {
      return false;
    }
    if (spelled == null) {
      return false;
    }
    StringBuilder expected = new StringBuilder();
    for (double value : doubles) {
      expected.append(value).append('\n');
    }
    for (float value : floats) {
      expected.append(value).append('\n');
    }
    return expected.toString().equals(new String(spelled, StandardCharsets.UTF_8));
  }

  /**
   * Doubles whose legacy spelling is not the shortest representation (mined against JDK 17):
   * random doubles diverge too rarely (~0.3%) for a 64-value sample to guarantee one, so these
   * make the double half of the probe unable to pass by luck on a shortest-digits JVM.
   */
  private static final long[] DIVERGENT_DOUBLE_BITS = {
    0x43B010EECEFE7DC2L, 0xC3D4DDBEC2FB191DL, 0xC3ADB50F5FBBE557L
  };

  static double[] probeDoubles() {
    double[] edges = {
      Double.NaN,
      Double.POSITIVE_INFINITY,
      Double.NEGATIVE_INFINITY,
      0.0d,
      -0.0d,
      Double.MIN_VALUE,
      Double.MIN_NORMAL,
      Double.MAX_VALUE,
      0.1d,
      1.0d / 3.0d,
      1e-4d,
      1e-3d,
      1e7d,
      1e8d
    };
    double[] corpus = new double[edges.length + DIVERGENT_DOUBLE_BITS.length + 64];
    System.arraycopy(edges, 0, corpus, 0, edges.length);
    int next = edges.length;
    for (long bits : DIVERGENT_DOUBLE_BITS) {
      corpus[next++] = Double.longBitsToDouble(bits);
    }
    Random random = new Random(0x5DF0A7D0);
    while (next < corpus.length) {
      double value = Double.longBitsToDouble(random.nextLong());
      if (!Double.isNaN(value)) {
        corpus[next++] = value;
      }
    }
    return corpus;
  }

  /**
   * With floats diverging at ~11%, 128 fixed random values are guaranteed to carry divergent ones
   * (this seed carries 11, verified against JDK 17).
   */
  static float[] probeFloats() {
    float[] edges = {
      Float.NaN,
      Float.POSITIVE_INFINITY,
      Float.NEGATIVE_INFINITY,
      0.0f,
      -0.0f,
      Float.MIN_VALUE,
      Float.MIN_NORMAL,
      Float.MAX_VALUE,
      0.1f,
      1.0f / 3.0f,
      1e-4f,
      1e-3f,
      1e7f,
      1e8f
    };
    float[] corpus = new float[edges.length + 128];
    System.arraycopy(edges, 0, corpus, 0, edges.length);
    Random random = new Random(0x5DF0A7F0);
    int next = edges.length;
    while (next < corpus.length) {
      float value = Float.intBitsToFloat(random.nextInt());
      if (!Float.isNaN(value)) {
        corpus[next++] = value;
      }
    }
    return corpus;
  }
}

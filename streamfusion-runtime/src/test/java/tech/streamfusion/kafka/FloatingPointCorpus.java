package tech.streamfusion.kafka;

import java.util.Random;

/**
 * The FLOAT/DOUBLE corpora refereed against Flink's serializers: spelling edge cases (non-finite
 * values, zero signs, subnormal and extreme magnitudes, the plain/scientific boundary, values
 * where the legacy JDK spelling is not the shortest representation) plus seeded random bit
 * patterns.
 */
final class FloatingPointCorpus {

  private FloatingPointCorpus() {}

  /** Doubles whose legacy (JDK ≤ 18) spelling differs from shortest-representation digits. */
  private static final long[] DIVERGENT_DOUBLE_BITS = {
    0x43B010EECEFE7DC2L, 0xC3D4DDBEC2FB191DL, 0xC3ADB50F5FBBE557L
  };

  static double[] edgeDoubles() {
    double[] edges = {
      Double.NaN,
      Double.POSITIVE_INFINITY,
      Double.NEGATIVE_INFINITY,
      0.0d,
      -0.0d,
      Double.MIN_VALUE,
      -Double.MIN_VALUE,
      Double.MIN_NORMAL,
      Math.nextDown(Double.MIN_NORMAL),
      Double.MAX_VALUE,
      -Double.MAX_VALUE,
      0.1d,
      1.0d / 3.0d,
      9007199254740991.0d, // 2^53 - 1
      9007199254740993.0d, // rounds to 2^53
      1e-5d,
      1e-4d, // scientific below here
      1e-3d,
      1.0d,
      1e7d, // plain up to here
      1e8d,
      Double.longBitsToDouble(DIVERGENT_DOUBLE_BITS[0]),
      Double.longBitsToDouble(DIVERGENT_DOUBLE_BITS[1]),
      Double.longBitsToDouble(DIVERGENT_DOUBLE_BITS[2])
    };
    return edges;
  }

  static float[] edgeFloats() {
    float[] edges = {
      Float.NaN,
      Float.POSITIVE_INFINITY,
      Float.NEGATIVE_INFINITY,
      0.0f,
      -0.0f,
      Float.MIN_VALUE,
      -Float.MIN_VALUE,
      Float.MIN_NORMAL,
      Math.nextDown(Float.MIN_NORMAL),
      Float.MAX_VALUE,
      -Float.MAX_VALUE,
      0.1f,
      1.0f / 3.0f,
      16777215.0f, // 2^24 - 1
      16777217.0f, // rounds to 2^24
      1e-5f,
      1e-4f,
      1e-3f,
      1.0f,
      1e7f,
      1e8f,
      // Legacy Float.toString is not shortest on ~11% of random floats; two such values.
      Float.intBitsToFloat(0x52BDCBEB), // 4.07584997E11, shortest is 4.07585E11
      Float.intBitsToFloat(0xCEE7FBA1) // -1.94601382E9, shortest is -1.9460138E9
    };
    return edges;
  }

  /** Random bit patterns; NaN payloads stay in — every NaN spells "NaN" on both sides. */
  static double[] randomDoubles(int count, long seed) {
    Random random = new Random(seed);
    double[] values = new double[count];
    for (int i = 0; i < count; i++) {
      values[i] = Double.longBitsToDouble(random.nextLong());
    }
    return values;
  }

  static float[] randomFloats(int count, long seed) {
    Random random = new Random(seed);
    float[] values = new float[count];
    for (int i = 0; i < count; i++) {
      values[i] = Float.intBitsToFloat(random.nextInt());
    }
    return values;
  }
}

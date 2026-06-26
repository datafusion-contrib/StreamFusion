package io.github.jordepic.streamfusion.operator;

import java.io.Serializable;

/**
 * A residual non-equi join predicate encoded in the pre-order form the native filter engine decodes
 * (the {@code RexExpression} arrays), carried to a native join operator. {@link #NONE} is the
 * empty predicate (no residual condition); the native side treats empty {@code kinds} as "no
 * predicate".
 */
public final class EncodedPredicate implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final EncodedPredicate NONE =
      new EncodedPredicate(new int[0], new int[0], new int[0], new long[0], new double[0], new String[0]);

  public final int[] kinds;
  public final int[] payload;
  public final int[] childCounts;
  public final long[] longs;
  public final double[] doubles;
  public final String[] strings;

  public EncodedPredicate(
      int[] kinds,
      int[] payload,
      int[] childCounts,
      long[] longs,
      double[] doubles,
      String[] strings) {
    this.kinds = kinds;
    this.payload = payload;
    this.childCounts = childCounts;
    this.longs = longs;
    this.doubles = doubles;
    this.strings = strings;
  }
}

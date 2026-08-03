package tech.streamfusion.operator;

import java.io.Serializable;

/**
 * A residual non-equi join predicate encoded in the pre-order form the native filter engine decodes
 * (the {@code RexExpression} arrays), carried to a native join operator. {@link #NONE} is the
 * empty predicate (no residual condition); the native side treats empty {@code kinds} as "no
 * predicate".
 *
 * <p>Carries a {@link NativeUdf.Binding} so a predicate that references a UDF (e.g. {@code UPPER} or a
 * user {@code ScalarFunction}) is distributed-safe: the operator calls {@link #bind} at {@code open()}
 * to register the UDFs on its own JVM and get the {@code longs} with runtime ids patched in, and
 * {@link #unbind} at {@code close()} — so a distributed task manager resolves them, not just the planner.
 */
public final class EncodedPredicate implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final EncodedPredicate NONE =
      new EncodedPredicate(
          new int[0], new int[0], new int[0], new long[0], new double[0], new String[0],
          NativeUdf.Binding.EMPTY);

  public final int[] kinds;
  public final int[] payload;
  public final int[] childCounts;
  public final long[] longs;
  public final double[] doubles;
  public final String[] strings;
  public final NativeUdf.Binding binding;

  private transient long[] boundLongs;

  public EncodedPredicate(
      int[] kinds,
      int[] payload,
      int[] childCounts,
      long[] longs,
      double[] doubles,
      String[] strings,
      NativeUdf.Binding binding) {
    this.kinds = kinds;
    this.payload = payload;
    this.childCounts = childCounts;
    this.longs = longs;
    this.doubles = doubles;
    this.strings = strings;
    this.binding = binding;
  }

  /**
   * Registers this predicate's UDFs on the current JVM and caches the {@code longs} with their ids
   * patched to the task-local runtime ids. Call at operator {@code open()} before compiling the
   * predicate; use {@link #boundLongs()} in place of {@link #longs} thereafter.
   */
  public void bind() {
    boundLongs = binding.bind(longs);
  }

  /** The {@code longs} to compile with — the patched copy after {@link #bind}, else the raw array. */
  public long[] boundLongs() {
    return boundLongs != null ? boundLongs : longs;
  }

  /** Frees the registrations obtained by {@link #bind}; call at operator {@code close()}. */
  public void unbind() {
    binding.unbind();
  }
}

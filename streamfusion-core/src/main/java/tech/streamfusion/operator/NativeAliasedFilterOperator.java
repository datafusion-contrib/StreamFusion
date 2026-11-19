package tech.streamfusion.operator;

/** Gives repeated filter projections distinct Arrow field names in the deployed plan. */
public final class NativeAliasedFilterOperator extends NativeFilterOperator {
  private static final long serialVersionUID = 1L;

  private final String[] outputNames;

  public NativeAliasedFilterOperator(
      int[] projection,
      int[] kinds,
      int[] payload,
      int[] childCounts,
      long[] longs,
      double[] doubles,
      String[] strings,
      NativeUdf.Binding udfBinding,
      String[] outputNames) {
    super(projection, kinds, payload, childCounts, longs, doubles, strings, udfBinding);
    this.outputNames = outputNames.clone();
  }

  @Override
  protected String[] outputNames() {
    return outputNames;
  }
}

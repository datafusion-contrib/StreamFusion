package tech.streamfusion.operator;

/** Test-only view into a native stateful operator's resolved state route. */
public final class NativeStateRouteProbe {

  private NativeStateRouteProbe() {}

  public static boolean directRocksDBState(AbstractNativeStatefulOperator<?> operator) {
    return operator.directRocksDBState();
  }
}

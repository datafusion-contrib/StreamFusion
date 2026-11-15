package tech.streamfusion.compat;

/** Preserves released SINGLE_VALUE and JSON aggregate failure contracts across Flink lines. */
public final class TableErrors {
  private TableErrors() {}

  public static Class<? extends RuntimeException> exceptionType() {
    return RuntimeException.class;
  }

  public static void fail(String message) {
    throw new RuntimeException(message);
  }

  public static void jsonAggregate(String message) {
    throw new org.apache.flink.table.api.TableException(message);
  }

  public static void jsonNullKey(String message) {
    throw new org.apache.flink.table.api.TableException(message);
  }

  public static void jsonSerialization(String message) {
    // Delegate this cold failure to the released host to preserve its wrapped cause.
    var function =
        new org.apache.flink.table.runtime.functions.aggregate.JsonObjectAggFunction(
            new org.apache.flink.table.types.logical.LogicalType[0], false);
    var accumulator = function.createAccumulator();
    accumulator.map.setMap(
        java.util.Collections.singletonMap(
            null, org.apache.flink.table.data.StringData.fromString("0")));
    function.getValue(accumulator);
    throw new IllegalStateException("Expected the released NULL-key serialization failure");
  }

  public static void jsonDuplicateNullKey(String message) throws Exception {
    var function =
        new org.apache.flink.table.runtime.functions.aggregate.JsonObjectAggFunction(
            new org.apache.flink.table.types.logical.LogicalType[0], false);
    var accumulator = function.createAccumulator();
    accumulator.map.setMap(
        java.util.Collections.singletonMap(
            null, org.apache.flink.table.data.StringData.fromString("0")));
    function.accumulate(accumulator, null, org.apache.flink.table.data.StringData.fromString("0"));
    throw new IllegalStateException("Expected the released duplicate NULL-key failure");
  }

  public static void jsonLocalDuplicateNullKey(String message) throws Exception {
    var function =
        new org.apache.flink.table.runtime.functions.aggregate.JsonObjectAggFunction(
            new org.apache.flink.table.types.logical.LogicalType[0], false);
    var accumulator = function.createAccumulator();
    accumulator.map.setMap(
        java.util.Collections.singletonMap(
            null, org.apache.flink.table.data.StringData.fromString("0")));
    function.accumulate(accumulator, null, org.apache.flink.table.data.StringData.fromString("0"));
    throw new IllegalStateException("Expected the released local duplicate NULL-key failure");
  }
}

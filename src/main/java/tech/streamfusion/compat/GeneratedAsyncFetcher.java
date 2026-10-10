package tech.streamfusion.compat;

import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.types.DataType;

public final class GeneratedAsyncFetcher {
  private final GeneratedFunction<AsyncFunction<RowData, Object>> tableFunc;
  private final DataType dataType;

  public GeneratedAsyncFetcher(
      GeneratedFunction<AsyncFunction<RowData, Object>> tableFunc, DataType dataType) {
    this.tableFunc = tableFunc;
    this.dataType = dataType;
  }

  public GeneratedFunction<AsyncFunction<RowData, Object>> tableFunc() {
    return tableFunc;
  }

  public DataType dataType() {
    return dataType;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;
    GeneratedAsyncFetcher that = (GeneratedAsyncFetcher) other;
    return java.util.Objects.equals(tableFunc, that.tableFunc)
        && java.util.Objects.equals(dataType, that.dataType);
  }

  @Override
  public int hashCode() {
    int result = 0;
    result = 31 * result + java.util.Objects.hashCode(tableFunc);
    result = 31 * result + java.util.Objects.hashCode(dataType);
    return result;
  }

  @Override
  public String toString() {
    return "GeneratedAsyncFetcher[tableFunc=" + tableFunc + ", dataType=" + dataType + "]";
  }
}

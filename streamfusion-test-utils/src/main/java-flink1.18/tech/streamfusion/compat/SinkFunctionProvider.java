package tech.streamfusion.compat;

public final class SinkFunctionProvider {
  private SinkFunctionProvider() {}

  public static org.apache.flink.table.connector.sink.DynamicTableSink.SinkRuntimeProvider of(
      org.apache.flink.streaming.api.functions.sink.SinkFunction<
              org.apache.flink.table.data.RowData>
          function) {
    return org.apache.flink.table.connector.sink.SinkFunctionProvider.of(function);
  }
}

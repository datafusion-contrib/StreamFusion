package tech.streamfusion.hudi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import tech.streamfusion.operator.ArrowBatch;

/** Validated writer boundary shared by the native Hudi sink lowering. */
public interface HudiSinkPlan {
  DataStreamSink<?> append(DataStream<ArrowBatch> input, DynamicTableSink.Context context);

  default String writeMode() {
    return "COW append";
  }
}

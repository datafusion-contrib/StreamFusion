package tech.streamfusion.delta;

import io.delta.flink.sink.DeltaSink;
import io.delta.flink.sink.DeltaSinkConf;
import io.delta.flink.table.DeltaTable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;

/** Delta sink variant whose Arrow input has already been routed by table partition. */
public final class NativeDeltaSink extends DeltaSink {
  public NativeDeltaSink(DeltaTable table, DeltaSinkConf conf) {
    super(table, conf);
  }

  @Override
  public DataStream<RowData> addPreWriteTopology(DataStream<RowData> input) {
    return input;
  }
}

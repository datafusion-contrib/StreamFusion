package tech.streamfusion.compat;

import java.nio.file.Path;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

/** Uses the line's filesystem SQL writer generation for exchange checkpoint verification. */
public final class CheckpointFileSink {
  private CheckpointFileSink() {}

  public static void attach(DataStream<String> rows, Path output, String uid) {
    var sink =
        org.apache.flink.connector.file.sink.FileSink.forRowFormat(
                new org.apache.flink.core.fs.Path(output.toUri()),
                new SimpleStringEncoder<String>("UTF-8"))
            .withRollingPolicy(OnCheckpointRollingPolicy.build())
            .build();
    rows.sinkTo(sink).uid(uid);
  }
}
